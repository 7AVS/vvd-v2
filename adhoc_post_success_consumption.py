# Post-Success Consumption Analysis
# Measures spending behavior AFTER campaign success events.
# Uses persisted result_df (from Cell 4) + raw POS transactions.
#
# Three analyses:
# 1. Post-acquisition spending (VCN, VDA) — Action vs Control
# 2. Post-activation spending (VDT) — Action vs Control
# 3. Post-provisioning spending (VUT, VAW) — Before vs After + Action vs Control
#
# Reads result_df from HDFS if not in memory:
#   result_df = spark.read.parquet("/user/427966379/vvd_v2_result")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Config ---
POS_TXN_PATH = "/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT={year}*"
YEARS = [2024, 2025, 2026]
POST_WINDOW_DAYS = 90   # measure spending up to 90 days after success
PRE_WINDOW_DAYS = 90    # for provisioning: compare 90 days before vs after

# --- Load raw POS with amounts ---
txn_paths = [POS_TXN_PATH.format(year=y) for y in YEARS]
raw_txn = spark.read.parquet(*txn_paths)

pos_df = (
    raw_txn
    .filter(F.col("SRVC_CD") == 36)
    .filter(
        ((F.col("TXN_TP") == 10) & (F.col("MSG_TP") == "0210")) |
        ((F.col("TXN_TP") == 13) & (F.col("MSG_TP") == "0210")) |
        ((F.col("TXN_TP") == 12) & (F.col("MSG_TP") == "0220"))
    )
    .filter(F.col("AMT1") > 0)
    .withColumn("CLNT_NO", F.regexp_replace(F.substring(F.col("CLNT_CRD_NO"), 7, 9), "^0+", ""))
    .select("CLNT_NO", F.col("TXN_DT").cast("date").alias("TXN_DT"), F.col("AMT1").cast("double").alias("TXN_AMT"))
    .filter(F.col("TXN_DT").isNotNull())
)

# Pre-filter POS to experiment clients only
experiment_clients = result_df.select("CLNT_NO").distinct()
pos_df = pos_df.join(experiment_clients, "CLNT_NO", "left_semi")
print(f"POS transactions (experiment clients): {pos_df.count():,}")


# ============================================================
# 1. POST-ACQUISITION SPENDING (VCN, VDA)
# For clients who acquired, compare spending in 90 days after
# ============================================================
print("\n" + "=" * 60)
print("POST-ACQUISITION SPENDING (VCN, VDA)")
print("=" * 60)

acq_clients = (
    result_df
    .filter(F.col("MNE").isin(["VCN", "VDA"]))
    .filter(F.col("ACQUISITION_SUCCESS") == 1)
    .select("CLNT_NO", "MNE", "TST_GRP_CD", "FIRST_ACQUISITION_SUCCESS_DT")
    .dropDuplicates(["CLNT_NO", "MNE"])
)

acq_spending = (
    acq_clients
    .join(pos_df, "CLNT_NO", "inner")
    .filter(F.col("TXN_DT") >= F.col("FIRST_ACQUISITION_SUCCESS_DT"))
    .filter(F.col("TXN_DT") <= F.date_add(F.col("FIRST_ACQUISITION_SUCCESS_DT"), POST_WINDOW_DAYS))
)

acq_summary = (
    acq_spending
    .groupBy("MNE", "TST_GRP_CD")
    .agg(
        F.countDistinct("CLNT_NO").alias("clients_with_spend"),
        F.count("*").alias("txn_count"),
        F.sum("TXN_AMT").alias("total_spend"),
        F.avg("TXN_AMT").alias("avg_txn_amt"),
        (F.count("*") / F.countDistinct("CLNT_NO")).alias("avg_txn_per_client"),
        (F.sum("TXN_AMT") / F.countDistinct("CLNT_NO")).alias("avg_spend_per_client"),
    )
    .orderBy("MNE", "TST_GRP_CD")
)

# Include clients who acquired but had zero spending
acq_total = acq_clients.groupBy("MNE", "TST_GRP_CD").agg(F.countDistinct("CLNT_NO").alias("total_acquirers"))

acq_result = (
    acq_total.join(acq_summary, ["MNE", "TST_GRP_CD"], "left")
    .withColumn("pct_with_spend", F.col("clients_with_spend") / F.col("total_acquirers") * 100)
    .orderBy("MNE", "TST_GRP_CD")
)

print(f"\nSpending within {POST_WINDOW_DAYS} days of acquisition (Action vs Control):")
acq_result.show(20, truncate=False)


# ============================================================
# 2. POST-ACTIVATION SPENDING (VDT)
# For clients who activated, compare spending in 90 days after
# ============================================================
print("=" * 60)
print("POST-ACTIVATION SPENDING (VDT)")
print("=" * 60)

act_clients = (
    result_df
    .filter(F.col("MNE") == "VDT")
    .filter(F.col("ACTIVATION_SUCCESS") == 1)
    .select("CLNT_NO", "MNE", "TST_GRP_CD", "FIRST_ACTIVATION_SUCCESS_DT")
    .dropDuplicates(["CLNT_NO", "MNE"])
)

act_spending = (
    act_clients
    .join(pos_df, "CLNT_NO", "inner")
    .filter(F.col("TXN_DT") >= F.col("FIRST_ACTIVATION_SUCCESS_DT"))
    .filter(F.col("TXN_DT") <= F.date_add(F.col("FIRST_ACTIVATION_SUCCESS_DT"), POST_WINDOW_DAYS))
)

act_summary = (
    act_spending
    .groupBy("MNE", "TST_GRP_CD")
    .agg(
        F.countDistinct("CLNT_NO").alias("clients_with_spend"),
        F.count("*").alias("txn_count"),
        F.sum("TXN_AMT").alias("total_spend"),
        F.avg("TXN_AMT").alias("avg_txn_amt"),
        (F.count("*") / F.countDistinct("CLNT_NO")).alias("avg_txn_per_client"),
        (F.sum("TXN_AMT") / F.countDistinct("CLNT_NO")).alias("avg_spend_per_client"),
    )
    .orderBy("MNE", "TST_GRP_CD")
)

act_total = act_clients.groupBy("MNE", "TST_GRP_CD").agg(F.countDistinct("CLNT_NO").alias("total_activators"))

act_result = (
    act_total.join(act_summary, ["MNE", "TST_GRP_CD"], "left")
    .withColumn("pct_with_spend", F.col("clients_with_spend") / F.col("total_activators") * 100)
    .orderBy("MNE", "TST_GRP_CD")
)

print(f"\nSpending within {POST_WINDOW_DAYS} days of activation (Action vs Control):")
act_result.show(20, truncate=False)


# ============================================================
# 3. POST-PROVISIONING SPENDING (VUT, VAW) — BEFORE vs AFTER
# These clients were already active. Compare spending patterns
# in the 90 days BEFORE vs 90 days AFTER provisioning.
# ============================================================
print("=" * 60)
print("POST-PROVISIONING SPENDING — BEFORE vs AFTER (VUT, VAW)")
print("=" * 60)

prov_clients = (
    result_df
    .filter(F.col("MNE").isin(["VUT", "VAW"]))
    .filter(F.col("PROVISIONING_SUCCESS") == 1)
    .select("CLNT_NO", "MNE", "TST_GRP_CD", "FIRST_PROVISIONING_SUCCESS_DT")
    .dropDuplicates(["CLNT_NO", "MNE"])
)

prov_txn = prov_clients.join(pos_df, "CLNT_NO", "inner")

# Pre-provisioning: 90 days before
pre_prov = (
    prov_txn
    .filter(F.col("TXN_DT") < F.col("FIRST_PROVISIONING_SUCCESS_DT"))
    .filter(F.col("TXN_DT") >= F.date_sub(F.col("FIRST_PROVISIONING_SUCCESS_DT"), PRE_WINDOW_DAYS))
    .withColumn("PERIOD", F.lit("PRE"))
)

# Post-provisioning: 90 days after
post_prov = (
    prov_txn
    .filter(F.col("TXN_DT") >= F.col("FIRST_PROVISIONING_SUCCESS_DT"))
    .filter(F.col("TXN_DT") <= F.date_add(F.col("FIRST_PROVISIONING_SUCCESS_DT"), PRE_WINDOW_DAYS))
    .withColumn("PERIOD", F.lit("POST"))
)

prov_combined = pre_prov.unionByName(post_prov)

prov_summary = (
    prov_combined
    .groupBy("MNE", "TST_GRP_CD", "PERIOD")
    .agg(
        F.countDistinct("CLNT_NO").alias("clients_with_spend"),
        F.count("*").alias("txn_count"),
        F.sum("TXN_AMT").alias("total_spend"),
        F.avg("TXN_AMT").alias("avg_txn_amt"),
        (F.count("*") / F.countDistinct("CLNT_NO")).alias("avg_txn_per_client"),
        (F.sum("TXN_AMT") / F.countDistinct("CLNT_NO")).alias("avg_spend_per_client"),
    )
    .orderBy("MNE", "TST_GRP_CD", "PERIOD")
)

print(f"\nSpending {PRE_WINDOW_DAYS} days before vs after provisioning:")
prov_summary.show(20, truncate=False)

# Pre vs Post lift per campaign + test group
prov_pivot = (
    prov_summary
    .groupBy("MNE", "TST_GRP_CD")
    .pivot("PERIOD", ["PRE", "POST"])
    .agg(
        F.first("avg_spend_per_client").alias("avg_spend"),
        F.first("avg_txn_per_client").alias("avg_txn"),
    )
    .withColumn("spend_lift_pct", (F.col("POST_avg_spend") - F.col("PRE_avg_spend")) / F.col("PRE_avg_spend") * 100)
    .withColumn("txn_lift_pct", (F.col("POST_avg_txn") - F.col("PRE_avg_txn")) / F.col("PRE_avg_txn") * 100)
    .orderBy("MNE", "TST_GRP_CD")
)

print("Pre vs Post provisioning lift:")
prov_pivot.show(20, truncate=False)

print("\nDone. Key questions to answer:")
print("  - Do Action clients spend MORE after success than Control?")
print("  - Does provisioning change spending patterns (before vs after)?")
print("  - Which campaign drives the highest post-success revenue?")
