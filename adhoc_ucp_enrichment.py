#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================================================================
# VVD v2 — UCP Enrichment (Standalone)
#
# Reads persisted result_df from HDFS, enriches with UCP demographics,
# saves enriched version back to HDFS. Not integrated into main pipeline.
#
# UCP = Unified Client Profile. Monthly snapshots with demographics,
# product holdings, digital engagement, channel preferences.
#
# Join logic: each result_df row maps to the UCP snapshot from the
# month-end of its treatment start date, capped at the latest available
# partition.
# ====================================================================


# ============================================================
# CELL 1: CONFIG
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import StorageLevel

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# UPDATE THIS DATE — set to latest available UCP partition
UCP_LATEST_PARTITION = "2026-01-31"
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

UCP_BASE_PATH = "/prod/sz/tsz/00172/data/ucp4"
HDFS_INPUT = "/user/427966379/vvd_v2_result"
HDFS_OUTPUT = "/user/427966379/vvd_v2_result_with_ucp"
BROADCAST_THRESHOLD = 10000

UCP_COLUMNS = [
    "CLNT_NO",
    # Demographics
    "AGE", "AGE_RNG", "GENERATION", "CLNT_SPCFC_CD_VAL",
    "NXT_GEN_STUD_SEG_CD", "NEW_IMGRNT_SEG_CD", "LOG_COMP_SEG_CD",
    "ACTV_PROD_CNT", "ACTV_PROD_SRVC_CNT",
    # Tenure
    "TENURE_RBC_YEARS", "TENURE_RBC_RNG",
    # Credit
    "CREDIT_SCORE_RNG", "DLQY_IND",
    # Income/Value
    "INCOME_AFTER_TAX_RNG", "PROF_TOT_ANNUAL", "PROF_TOT_MONTHLY", "PROF_SEG_CD",
    # Relationship
    "ACCT_MGD_RELTN_TP_CD", "REL_TP_SEG_CD", "RELN_MG_UNIT_NO",
    # Credit Cards
    "CC_VISA_ALL_TOT_IND", "CC_VISA_CLAS_II_STUD_TOT_IND", "CC_VISA_CAS_STUD_TOT_IND",
    "CC_VISA_DR_IND", "CC_VISA_CLSIC_TOT_IND", "CC_VISA_CLSIC_RWD_TOT_IND",
    "CC_VISA_GOLD_PRFR_TOT_IND", "CC_VISA_INF_TOT_IND", "CC_VISA_IAV_TOT_IND",
    "CC_MASTERCARD_ALL_TOT_IND",
    # Lending
    "LOAN_TOT_IND", "MORTGAGE_RESID_TOT_IND", "LOAN_RCL_UNSEC_TOT_IND",
    "LOAN_RCL_STUDENT_TOT_IND", "LOAN_STUDENT_TOT_IND",
    "PDA_STUDENT_CURR_TOT_IND", "PDA_STUDENT_TOT_IND",
    # Multi-Product
    "MULTI_PROD_RBT_TOT_IND", "SRVC_CNT",
    # Digital
    "MOBILE_AUTH_MOB_CNT", "OLB_ENROLLED_IND", "CPC_OLB_ELIGIBLE", "MOBILE_AUTH_CNT",
    # Channel Preferences
    "D2D_BILL_PYMT_CHNL_PREF_SEG_CD", "D2D_DEP_CHNL_PREF_SEG_CD",
    "D2D_INFO_SEEK_CHNL_PREF_SEG_CD", "D2D_TRF_CHNL_PREF_SEG_CD",
    "D2D_MQ_CHNL_PREF_SEG_CD",
    # Transactions
    "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT",
    # OFI
    "OFI_M_PROD_CNT", "OFI_L_PROD_CNT", "OFI_C_PROD_CNT",
    "OFI_I_PROD_CNT", "OFI_T_PROD_CNT",
]

print(f"Config loaded. UCP_LATEST_PARTITION = {UCP_LATEST_PARTITION}")
print(f"UCP columns to extract: {len(UCP_COLUMNS)} (including CLNT_NO)")


# ============================================================
# CELL 2: LOAD RESULT_DF FROM HDFS
# ============================================================

result_df = spark.read.parquet(HDFS_INPUT)
result_count = result_df.count()
result_clients = result_df.select("CLNT_NO").distinct().count()
print(f"Loaded result_df: {result_count:,} rows, {result_clients:,} unique clients, {len(result_df.columns)} columns")
result_df.printSchema()
result_df.persist(StorageLevel.MEMORY_AND_DISK)


# ============================================================
# CELL 3: DERIVE UCP_MONTH_END_DATE
# ============================================================

cap_date = F.lit(UCP_LATEST_PARTITION).cast("date")

result_df = result_df.withColumn(
    "UCP_MONTH_END_DATE",
    F.when(
        F.last_day(F.col("TREATMT_STRT_DT")) > cap_date,
        cap_date
    ).otherwise(
        F.last_day(F.col("TREATMT_STRT_DT"))
    )
)

result_df.select("CLNT_NO", "MNE", "TREATMT_STRT_DT", "UCP_MONTH_END_DATE").show(20, truncate=False)

partition_summary = (
    result_df
    .groupBy("UCP_MONTH_END_DATE")
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct("CLNT_NO").alias("clients")
    )
    .orderBy("UCP_MONTH_END_DATE")
)
partition_summary.show(50, truncate=False)
partition_dates = [row["UCP_MONTH_END_DATE"] for row in partition_summary.collect()]
print(f"Distinct UCP partitions needed: {len(partition_dates)}")


# ============================================================
# CELL 4: EXTRACT UCP DATA PER PARTITION
# ============================================================

client_partition_pairs = (
    result_df
    .select("CLNT_NO", "UCP_MONTH_END_DATE")
    .distinct()
)
client_partition_pairs.persist(StorageLevel.MEMORY_AND_DISK)
total_pairs = client_partition_pairs.count()
print(f"Distinct (CLNT_NO, UCP_MONTH_END_DATE) pairs: {total_pairs:,}")

ucp_extracts = []

for part_date in partition_dates:
    part_str = str(part_date)
    part_path = f"{UCP_BASE_PATH}/MONTH_END_DATE={part_str}"

    clients_this_partition = (
        client_partition_pairs
        .filter(F.col("UCP_MONTH_END_DATE") == F.lit(part_str).cast("date"))
        .select("CLNT_NO")
        .distinct()
    )
    client_count = clients_this_partition.count()
    print(f"\nPartition {part_str}: {client_count:,} clients to look up")

    ucp_raw = spark.read.parquet(part_path).select(UCP_COLUMNS)

    if client_count < BROADCAST_THRESHOLD:
        client_list = [row["CLNT_NO"] for row in clients_this_partition.collect()]
        ucp_filtered = ucp_raw.filter(F.col("CLNT_NO").isin(client_list))
    else:
        ucp_filtered = ucp_raw.join(
            F.broadcast(clients_this_partition),
            on="CLNT_NO",
            how="inner"
        )

    ucp_filtered = ucp_filtered.withColumn(
        "UCP_MONTH_END_DATE", F.lit(part_str).cast("date")
    )

    found = ucp_filtered.count()
    missing = client_count - found
    print(f"  Found: {found:,} | Missing: {missing:,} ({missing/client_count*100:.1f}%)")

    ucp_extracts.append(ucp_filtered)

client_partition_pairs.unpersist()

ucp_all = ucp_extracts[0]
for df in ucp_extracts[1:]:
    ucp_all = ucp_all.unionByName(df)

ucp_total = ucp_all.count()
print(f"\nTotal UCP rows extracted across all partitions: {ucp_total:,}")


# ============================================================
# CELL 5: DEDUP AND VALIDATE UCP
# ============================================================

dedup_window = Window.partitionBy("CLNT_NO", "UCP_MONTH_END_DATE").orderBy(F.lit(1))
ucp_all = (
    ucp_all
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
ucp_deduped_count = ucp_all.count()
print(f"UCP after dedup: {ucp_deduped_count:,} rows (was {ucp_total:,})")

ucp_clients = ucp_all.select("CLNT_NO").distinct().count()
print(f"Unique clients with UCP data: {ucp_clients:,}")

null_counts = ucp_all.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in UCP_COLUMNS if c != "CLNT_NO"
])
null_row = null_counts.collect()[0]
print(f"\nNULL rates (of {ucp_deduped_count:,} rows):")
for c in UCP_COLUMNS:
    if c == "CLNT_NO":
        continue
    nulls = null_row[c]
    pct = nulls / ucp_deduped_count * 100 if ucp_deduped_count > 0 else 0
    if pct > 5:
        print(f"  {c}: {nulls:,} ({pct:.1f}%) ***")
    elif pct > 0:
        print(f"  {c}: {nulls:,} ({pct:.1f}%)")

print(f"\nUCP extraction complete: {ucp_clients:,} clients matched out of {result_clients:,} total")


# ============================================================
# CELL 6: JOIN TO RESULT_DF
# ============================================================

before_count = result_df.count()
print(f"result_df before join: {before_count:,} rows")

ucp_join_cols = [c for c in UCP_COLUMNS if c != "CLNT_NO"]
ucp_renamed = ucp_all
for c in ucp_join_cols:
    ucp_renamed = ucp_renamed.withColumnRenamed(c, f"UCP_{c}")
ucp_renamed = ucp_renamed.withColumnRenamed("UCP_MONTH_END_DATE", "UCP_MONTH_END_DATE_R")

enriched_df = result_df.join(
    ucp_renamed,
    on=[
        result_df["CLNT_NO"] == ucp_renamed["CLNT_NO"],
        result_df["UCP_MONTH_END_DATE"] == ucp_renamed["UCP_MONTH_END_DATE_R"]
    ],
    how="left"
).drop(ucp_renamed["CLNT_NO"]).drop("UCP_MONTH_END_DATE_R")

after_count = enriched_df.count()
print(f"result_df after join: {after_count:,} rows")

if before_count != after_count:
    print(f"FATAL: Fan-out detected! Before={before_count:,}, After={after_count:,}. "
          f"Difference={after_count - before_count:,}. STOPPING.")
    raise AssertionError(f"Join fan-out: {before_count} -> {after_count}")

print(f"Join verified: {before_count:,} == {after_count:,}, no fan-out")

enriched_df.select(
    "CLNT_NO", "MNE", "TST_GRP_CD", "UCP_MONTH_END_DATE",
    "UCP_AGE", "UCP_GENERATION", "UCP_INCOME_AFTER_TAX_RNG", "UCP_CREDIT_SCORE_RNG"
).show(10, truncate=False)

print(f"Enriched schema: {len(enriched_df.columns)} columns (was {len(result_df.columns)})")
for c in sorted(enriched_df.columns):
    if c.startswith("UCP_"):
        print(f"  + {c}")


# ============================================================
# CELL 7: SAVE TO HDFS
# ============================================================

enriched_df.write.mode("overwrite").parquet(HDFS_OUTPUT)
print(f"Saved enriched result to {HDFS_OUTPUT}")

verify = spark.read.parquet(HDFS_OUTPUT)
verify_count = verify.count()
verify_cols = len(verify.columns)
print(f"Enriched result saved: {verify_count:,} rows, {verify_cols} columns")

if verify_count != before_count:
    print(f"WARNING: Written rows ({verify_count:,}) != expected ({before_count:,})")
else:
    print(f"Verification passed: {verify_count:,} rows match expected count")


# ============================================================
# CELL 8: QUICK PROFILE SUMMARY
# ============================================================

print("=== UCP PROFILE: Key Column Distributions ===\n")

for col_name in ["UCP_GENERATION", "UCP_INCOME_AFTER_TAX_RNG", "UCP_CREDIT_SCORE_RNG", "UCP_TENURE_RBC_RNG"]:
    print(f"--- {col_name} ---")
    (
        enriched_df
        .groupBy(col_name)
        .agg(F.count("*").alias("count"))
        .withColumn("pct", F.round(F.col("count") / F.lit(after_count) * 100, 1))
        .orderBy(F.desc("count"))
        .show(20, truncate=False)
    )

print("\n=== DEMOGRAPHICS BY CAMPAIGN ===\n")
(
    enriched_df
    .groupBy("MNE")
    .agg(
        F.count("*").alias("rows"),
        F.round(F.avg("UCP_AGE"), 1).alias("avg_age"),
        F.round(F.avg("UCP_TENURE_RBC_YEARS"), 1).alias("avg_tenure_yrs"),
        F.round(F.avg("UCP_ACTV_PROD_CNT"), 1).alias("avg_active_products"),
        F.round(F.avg("UCP_SRVC_CNT"), 1).alias("avg_services"),
    )
    .orderBy("MNE")
    .show(truncate=False)
)

generation_mode = (
    enriched_df
    .groupBy("MNE", "UCP_GENERATION")
    .agg(F.count("*").alias("cnt"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("MNE").orderBy(F.desc("cnt"))
    ))
    .filter(F.col("rn") == 1)
    .select("MNE", F.col("UCP_GENERATION").alias("top_generation"), "cnt")
    .orderBy("MNE")
)
print("Most common generation per campaign:")
generation_mode.show(truncate=False)

ucp_match_rate = (
    enriched_df
    .groupBy("MNE")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("UCP_AGE").isNotNull(), 1).otherwise(0)).alias("matched"),
    )
    .withColumn("match_pct", F.round(F.col("matched") / F.col("total") * 100, 1))
    .orderBy("MNE")
)
print("UCP match rate per campaign:")
ucp_match_rate.show(truncate=False)

print("UCP enrichment complete.")
