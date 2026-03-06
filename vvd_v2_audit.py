#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================================================================
# VVD v2 Audit / Diagnostic Notebook
#
# SEPARATE from the main pipeline. Validates that the pipeline's
# success detection logic is correct by checking internal consistency
# and independently querying raw source data.
#
# Section A: Quick Checks (uses result_df from main pipeline)
# Section B: Raw Source Validation (queries raw tables independently)
#
# Prerequisites: Run vvd_v2_pipeline.py cells 1-4 first so that
# result_df is persisted in memory.
#
# Environment: Lumina (PySpark on YARN, Hive tables, older CDH Spark)
# Rules: PySpark only, no pandas, no function wrappers, flat cells.
# ====================================================================


# --- Cell A1: Imports + Load result_df ---

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import StorageLevel
from collections import defaultdict
import random

# Campaign metadata (mirrored from pipeline Cell 1)
CAMPAIGNS = {
    "VCN": {
        "name": "VVD Contextual Notification",
        "success_type": "ACQUISITION",
        "primary_metric": "card_acquisition",
        "test_control": "95/5",
        "deployment": "Trigger"
    },
    "VDA": {
        "name": "VVD Black Friday Cyber Monday",
        "success_type": "ACQUISITION",
        "primary_metric": "card_acquisition",
        "test_control": "95/5",
        "deployment": "Batch"
    },
    "VDT": {
        "name": "VVD Activation Trigger",
        "success_type": "ACTIVATION",
        "primary_metric": "card_activation",
        "test_control": "90/10",
        "deployment": "Trigger"
    },
    "VUI": {
        "name": "VVD Usage Trigger",
        "success_type": "USAGE",
        "primary_metric": "card_usage",
        "test_control": "95/5",
        "deployment": "Trigger"
    },
    "VUT": {
        "name": "VVD Tokenization Usage",
        "success_type": "TOKENIZATION",
        "primary_metric": "wallet_provisioning",
        "secondary_metric": "card_usage",
        "test_control": "95/5",
        "deployment": "Trigger"
    },
    "VAW": {
        "name": "VVD Add To Wallet",
        "success_type": "TOKENIZATION",
        "primary_metric": "wallet_provisioning",
        "secondary_metric": "card_usage",
        "test_control": "80/20",
        "deployment": "Trigger"
    }
}

METRIC_TO_COLUMN = {
    "card_acquisition": "ACQUISITION_SUCCESS",
    "card_activation": "ACTIVATION_SUCCESS",
    "card_usage": "USAGE_SUCCESS",
    "wallet_provisioning": "PROVISIONING_SUCCESS"
}

# MNE -> expected SUCCESS column (derived from primary_metric)
MNE_TO_SUCCESS_COL = {mne: METRIC_TO_COLUMN[cfg["primary_metric"]] for mne, cfg in CAMPAIGNS.items()}
# Result: VCN->ACQUISITION_SUCCESS, VDA->ACQUISITION_SUCCESS, VDT->ACTIVATION_SUCCESS,
#          VUI->USAGE_SUCCESS, VUT->PROVISIONING_SUCCESS, VAW->PROVISIONING_SUCCESS

METRIC_TO_CAMPAIGNS = defaultdict(list)
for _mne, _cfg in CAMPAIGNS.items():
    METRIC_TO_CAMPAIGNS[_cfg["primary_metric"]].append(_mne)
    if "secondary_metric" in _cfg:
        METRIC_TO_CAMPAIGNS[_cfg["secondary_metric"]].append(_mne)
METRIC_TO_CAMPAIGNS = dict(METRIC_TO_CAMPAIGNS)

# Source table paths (mirrored from pipeline Cell 1)
TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
CARD_DATA_PATH = "/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT={year}*"
POS_TXN_PATH = "/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT={year}*"
YEARS = [2024, 2025, 2026]

VVD_MNES = list(CAMPAIGNS.keys())
ACTION_GROUP = "TG4"
CONTROL_GROUP = "TG7"

# Track pass/fail for summary
audit_results = {}

# Verify result_df is available (should be persisted from main pipeline)
try:
    _rdf_count = result_df.count()
    print(f"result_df found: {_rdf_count:,} rows")
    print(f"Columns: {result_df.columns}")
except NameError:
    print("ERROR: result_df not found in session.")
    print("Run the main pipeline (cells 1-4) first, then come back here.")
    raise RuntimeError("result_df not available. Run main pipeline first.")


# --- Cell A2: TREATMT_END_DT Null Check ---

print("=" * 60)
print("A2: TREATMT_END_DT NULL CHECK")
print("=" * 60)

null_end_dt = result_df.filter(F.col("TREATMT_END_DT").isNull())
null_count = null_end_dt.count()

print(f"Rows with TREATMT_END_DT = NULL: {null_count:,}")

if null_count > 0:
    print(f"\nWARNING: {null_count:,} rows have NULL TREATMT_END_DT.")
    print("These rows will ALWAYS get SUCCESS=0 because the date window")
    print("comparison (SUCCESS_DT <= TREATMT_END_DT) evaluates to NULL.")
    print("\nSample rows:")
    null_end_dt.select(
        "MNE", "CLNT_NO", "TACTIC_ID", "TST_GRP_CD",
        "TREATMT_STRT_DT", "TREATMT_END_DT", "WINDOW_DAYS"
    ).show(20, truncate=False)

    # Break down by campaign
    print("Null TREATMT_END_DT by campaign:")
    null_end_dt.groupBy("MNE").agg(
        F.count("*").alias("null_count")
    ).orderBy("MNE").show(truncate=False)

    audit_results["A2_end_dt_null"] = "FAIL"
else:
    print("PASS: All rows have non-null TREATMT_END_DT.")
    audit_results["A2_end_dt_null"] = "PASS"


# --- Cell A3: TACTIC_CELL_CD (Channel) Distribution ---

print("=" * 60)
print("A3: TACTIC_CELL_CD (CHANNEL) DISTRIBUTION")
print("=" * 60)

print("Full distribution by test group and channel:")
result_df.groupBy(
    "TST_GRP_CD",
    F.trim(F.col("TACTIC_CELL_CD")).alias("TACTIC_CELL_CD_trimmed")
).agg(
    F.count("*").alias("count")
).orderBy("TST_GRP_CD", "TACTIC_CELL_CD_trimmed").show(50, truncate=False)

print("Control group (TG7) channel detail:")
result_df.filter(
    F.col("TST_GRP_CD") == CONTROL_GROUP
).groupBy(
    "MNE",
    F.trim(F.col("TACTIC_CELL_CD")).alias("TACTIC_CELL_CD_trimmed")
).agg(
    F.count("*").alias("count")
).orderBy("MNE", "TACTIC_CELL_CD_trimmed").show(50, truncate=False)

# Check for nulls specifically
null_channel = result_df.filter(
    F.col("TACTIC_CELL_CD").isNull() | (F.trim(F.col("TACTIC_CELL_CD")) == "")
).count()
print(f"Rows with NULL or empty TACTIC_CELL_CD: {null_channel:,}")

audit_results["A3_channel_dist"] = "INFO"


# --- Cell A4: RPT_GRP_CD Distribution ---

print("=" * 60)
print("A4: RPT_GRP_CD DISTRIBUTION")
print("=" * 60)

print("RPT_GRP_CD by campaign:")
result_df.groupBy("MNE", "RPT_GRP_CD").agg(
    F.count("*").alias("count")
).orderBy("MNE", "RPT_GRP_CD").show(100, truncate=False)

print("RPT_GRP_CD by campaign and test group:")
result_df.groupBy("MNE", "TST_GRP_CD", "RPT_GRP_CD").agg(
    F.count("*").alias("count")
).orderBy("MNE", "TST_GRP_CD", "RPT_GRP_CD").show(100, truncate=False)

audit_results["A4_rpt_grp"] = "INFO"


# --- Cell A5: SUCCESS Flag Validation (Internal Consistency) ---

print("=" * 60)
print("A5: SUCCESS FLAG VALIDATION")
print("=" * 60)
print("Verifying SUCCESS == expected metric column for each campaign.\n")

a5_pass = True

for mne, expected_col in MNE_TO_SUCCESS_COL.items():
    mne_df = result_df.filter(F.col("MNE") == mne)
    total = mne_df.count()

    # Count mismatches: SUCCESS != expected metric column
    mismatches = mne_df.filter(F.col("SUCCESS") != F.col(expected_col)).count()

    status = "PASS" if mismatches == 0 else "FAIL"
    print(f"  {mne}: SUCCESS should == {expected_col}")
    print(f"    Total rows: {total:,}, Mismatches: {mismatches:,} [{status}]")

    if mismatches > 0:
        a5_pass = False
        print(f"    SAMPLE MISMATCHES:")
        mne_df.filter(
            F.col("SUCCESS") != F.col(expected_col)
        ).select(
            "CLNT_NO", "MNE", "TST_GRP_CD", "SUCCESS",
            "ACQUISITION_SUCCESS", "ACTIVATION_SUCCESS",
            "USAGE_SUCCESS", "PROVISIONING_SUCCESS"
        ).show(10, truncate=False)

audit_results["A5_success_flag"] = "PASS" if a5_pass else "FAIL"
print(f"\nOverall A5: {'PASS' if a5_pass else 'FAIL'}")


# --- Cell A6: Date Window Validation ---

print("=" * 60)
print("A6: DATE WINDOW VALIDATION (SUCCESS=1 rows)")
print("=" * 60)

# Build the correct date column name for each campaign's primary metric
# For SUCCESS=1 rows, the relevant FIRST_*_DT column should be within the treatment window
a6_pass = True

for mne, cfg in CAMPAIGNS.items():
    success_col = METRIC_TO_COLUMN[cfg["primary_metric"]]
    dt_col = f"FIRST_{success_col}_DT"

    success_rows = result_df.filter(
        (F.col("MNE") == mne) & (F.col("SUCCESS") == 1)
    )
    total_success = success_rows.count()

    if total_success == 0:
        print(f"  {mne}: No SUCCESS=1 rows (skipped)")
        continue

    # Violations: SUCCESS_DT outside [TREATMT_STRT_DT, TREATMT_END_DT]
    before_start = success_rows.filter(F.col(dt_col) < F.col("TREATMT_STRT_DT")).count()
    after_end = success_rows.filter(F.col(dt_col) > F.col("TREATMT_END_DT")).count()
    null_dt = success_rows.filter(F.col(dt_col).isNull()).count()

    # Edge cases
    same_day_start = success_rows.filter(F.col(dt_col) == F.col("TREATMT_STRT_DT")).count()
    same_day_end = success_rows.filter(F.col(dt_col) == F.col("TREATMT_END_DT")).count()

    violations = before_start + after_end + null_dt
    status = "PASS" if violations == 0 else "FAIL"

    print(f"  {mne} ({dt_col}):")
    print(f"    Total SUCCESS=1: {total_success:,}")
    print(f"    Before TREATMT_STRT_DT: {before_start:,}")
    print(f"    After TREATMT_END_DT: {after_end:,}")
    print(f"    NULL date despite SUCCESS=1: {null_dt:,}")
    print(f"    Edge — same day as STRT: {same_day_start:,}")
    print(f"    Edge — same day as END: {same_day_end:,}")
    print(f"    [{status}]")

    if violations > 0:
        a6_pass = False
        print(f"    SAMPLE VIOLATIONS:")
        success_rows.filter(
            (F.col(dt_col) < F.col("TREATMT_STRT_DT")) |
            (F.col(dt_col) > F.col("TREATMT_END_DT")) |
            (F.col(dt_col).isNull())
        ).select(
            "CLNT_NO", "MNE", "TREATMT_STRT_DT", "TREATMT_END_DT",
            dt_col, "SUCCESS"
        ).show(10, truncate=False)

audit_results["A6_date_window"] = "PASS" if a6_pass else "FAIL"
print(f"\nOverall A6: {'PASS' if a6_pass else 'FAIL'}")


# --- Cell A7: Deployment Granularity Check ---

print("=" * 60)
print("A7: DEPLOYMENT GRANULARITY CHECK")
print("=" * 60)
print("Each row should be unique on (CLNT_NO, TACTIC_ID, MNE, TREATMT_STRT_DT).\n")

total_rows = result_df.count()
distinct_rows = result_df.select(
    "CLNT_NO", "TACTIC_ID", "MNE", "TREATMT_STRT_DT"
).distinct().count()
duplicates = total_rows - distinct_rows

print(f"Total rows: {total_rows:,}")
print(f"Distinct (CLNT_NO, TACTIC_ID, MNE, TREATMT_STRT_DT): {distinct_rows:,}")
print(f"Duplicates: {duplicates:,}")

if duplicates > 0:
    print(f"\nWARNING: {duplicates:,} duplicate rows found!")
    print("Sample duplicates:")

    w_dup = Window.partitionBy("CLNT_NO", "TACTIC_ID", "MNE", "TREATMT_STRT_DT")
    result_df.withColumn(
        "_dup_count", F.count("*").over(w_dup)
    ).filter(
        F.col("_dup_count") > 1
    ).drop("_dup_count").show(20, truncate=False)

    audit_results["A7_granularity"] = "FAIL"
else:
    print("PASS: No duplicates found.")
    audit_results["A7_granularity"] = "PASS"


# ============================================================
# SECTION B: RAW SOURCE VALIDATION
# Independent of pipeline logic — queries raw tables directly.
# ============================================================


# --- Cell B1: Sample Selection ---

print("=" * 60)
print("B1: SAMPLE SELECTION")
print("=" * 60)

# Pick SUCCESS=1 clients: at least a few from each campaign, mix of TG4/TG7
# Pick SUCCESS=0 clients: same spread
success_1_samples = []
success_0_samples = []

for mne in VVD_MNES:
    for tg in [ACTION_GROUP, CONTROL_GROUP]:
        # SUCCESS=1
        s1_pool = (
            result_df.filter(
                (F.col("MNE") == mne) &
                (F.col("TST_GRP_CD") == tg) &
                (F.col("SUCCESS") == 1)
            )
            .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
                    "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
            .limit(100)
            .collect()
        )
        if s1_pool:
            picked = random.sample(s1_pool, min(2, len(s1_pool)))
            success_1_samples.extend(picked)

        # SUCCESS=0
        s0_pool = (
            result_df.filter(
                (F.col("MNE") == mne) &
                (F.col("TST_GRP_CD") == tg) &
                (F.col("SUCCESS") == 0)
            )
            .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
                    "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
            .limit(100)
            .collect()
        )
        if s0_pool:
            picked = random.sample(s0_pool, min(2, len(s0_pool)))
            success_0_samples.extend(picked)

# Trim random samples to ~20 each
random.shuffle(success_1_samples)
random.shuffle(success_0_samples)
success_1_samples = success_1_samples[:20]
success_0_samples = success_0_samples[:20]

# --- TARGETED EDGE CASES ---
# These catch boundary conditions that random sampling might miss.
edge_case_samples = []

# 1. Shortest treatment windows (WINDOW_DAYS close to 0 — tight window)
shortest = (
    result_df.filter(F.col("WINDOW_DAYS").isNotNull())
    .orderBy("WINDOW_DAYS")
    .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
            "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
    .limit(3).collect()
)
edge_case_samples.extend(shortest)

# 2. Longest treatment windows
longest = (
    result_df.filter(F.col("WINDOW_DAYS").isNotNull())
    .orderBy(F.col("WINDOW_DAYS").desc())
    .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
            "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
    .limit(3).collect()
)
edge_case_samples.extend(longest)

# 3. Clients in MULTIPLE campaigns (cross-campaign overlap)
multi_camp = (
    result_df.groupBy("CLNT_NO").agg(F.countDistinct("MNE").alias("n_campaigns"))
    .filter(F.col("n_campaigns") > 1)
    .orderBy(F.col("n_campaigns").desc())
    .limit(3)
    .select("CLNT_NO").collect()
)
n_multi = 0
for mc in multi_camp:
    mc_rows = (
        result_df.filter(F.col("CLNT_NO") == str(mc.CLNT_NO))
        .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
                "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
        .limit(3).collect()
    )
    n_multi += len(mc_rows)
    edge_case_samples.extend(mc_rows)

# 4. SUCCESS=1 where success date == TREATMT_STRT_DT (same-day boundary)
n_boundary = 0
for mne, cfg in CAMPAIGNS.items():
    dt_col = f"FIRST_{METRIC_TO_COLUMN[cfg['primary_metric']]}_DT"
    boundary = (
        result_df.filter(
            (F.col("MNE") == mne) &
            (F.col("SUCCESS") == 1) &
            (F.col(dt_col) == F.col("TREATMT_STRT_DT"))
        )
        .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
                "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
        .limit(1).collect()
    )
    n_boundary += len(boundary)
    edge_case_samples.extend(boundary)

print(f"Edge case samples collected: {len(edge_case_samples)}")
print(f"  Shortest windows: {len(shortest)}")
print(f"  Longest windows: {len(longest)}")
print(f"  Multi-campaign clients: {n_multi} rows from {len(multi_camp)} clients")
print(f"  Same-day boundary: {n_boundary}")

all_samples = success_1_samples + success_0_samples + edge_case_samples

# Create DataFrame for easy reuse
sample_schema = StructType([
    StructField("CLNT_NO", StringType(), True),
    StructField("MNE", StringType(), True),
    StructField("TST_GRP_CD", StringType(), True),
    StructField("TACTIC_ID", StringType(), True),
    StructField("TREATMT_STRT_DT", DateType(), True),
    StructField("TREATMT_END_DT", DateType(), True),
    StructField("SUCCESS", IntegerType(), True),
])
sample_clients = spark.createDataFrame(
    [(str(r.CLNT_NO), str(r.MNE), str(r.TST_GRP_CD), str(r.TACTIC_ID),
      r.TREATMT_STRT_DT, r.TREATMT_END_DT, int(r.SUCCESS))
     for r in all_samples],
    schema=sample_schema
)
sample_clients.persist(StorageLevel.MEMORY_AND_DISK)

print(f"Selected {len(success_1_samples)} SUCCESS=1 samples and {len(success_0_samples)} SUCCESS=0 samples")
print(f"Total sample: {sample_clients.count()} rows")
print("\nSUCCESS=1 samples:")
sample_clients.filter(F.col("SUCCESS") == 1).show(25, truncate=False)
print("SUCCESS=0 samples:")
sample_clients.filter(F.col("SUCCESS") == 0).show(25, truncate=False)

# Collect sample CLNT_NOs as a list for raw queries
sample_clnt_list = [str(r.CLNT_NO) for r in sample_clients.select("CLNT_NO").distinct().collect()]
print(f"Unique sample clients: {len(sample_clnt_list)}")


# --- Cell B2: Raw Card Table Validation (VCN, VDA — card_acquisition) ---

print("=" * 60)
print("B2: RAW CARD TABLE VALIDATION (card_acquisition)")
print("=" * 60)
print("Querying DDWTA_VISA_DR_CRD directly for sample clients.\n")

# Read raw card data (same paths as pipeline), filter to sample clients immediately
card_paths = [CARD_DATA_PATH.format(year=y) for y in YEARS]
try:
    raw_card_audit = spark.read.parquet(*card_paths)
except Exception as e:
    print(f"ERROR: Could not read card data: {e}")
    raw_card_audit = None

b2_mismatches = 0
if raw_card_audit is not None:
    # Filter to sample clients FIRST, persist small result for reuse in B3/B6/B7
    raw_card_sample = (
        raw_card_audit
        .filter(F.col("SRVC_ID") == 36)  # Push native filter before derivation
        .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("CLNT_NO")), "^0+", ""))
        .filter(F.col("CLNT_NO").isin(sample_clnt_list))
        .select(
            "CLNT_NO", "CRD_NO", "STS_CD", "SRVC_ID",
            F.col("ISS_DT").cast("date").alias("ISS_DT"),
            F.col("ACTV_DT").cast("date").alias("ACTV_DT"),
            F.col("CAPTR_DT").cast("date").alias("CAPTR_DT")
        )
    )
    raw_card_sample.persist(StorageLevel.MEMORY_AND_DISK)

    print(f"Raw card records for sample clients: {raw_card_sample.count()}")
    print("\nAll raw card records for sample clients:")
    raw_card_sample.show(100, truncate=False)

    # Validate VCN and VDA (card_acquisition) samples
    acq_mnes = ["VCN", "VDA"]
    acq_samples = sample_clients.filter(F.col("MNE").isin(acq_mnes))

    if acq_samples.count() > 0:
        print("\nCard Acquisition validation (VCN, VDA):")
        print("-" * 60)

        acq_rows = acq_samples.collect()
        for sr in acq_rows:
            clnt = str(sr.CLNT_NO)
            strt = sr.TREATMT_STRT_DT
            end = sr.TREATMT_END_DT
            expected_success = int(sr.SUCCESS)

            # Apply same filters as pipeline: STS_CD in ('06','08'), SRVC_ID==36, ISS_DT not null, ISS_DT in window
            qualifying = (
                raw_card_sample
                .filter(F.col("CLNT_NO") == clnt)
                .filter(F.col("STS_CD").isin(["06", "08"]))
                .filter(F.col("SRVC_ID") == 36)
                .filter(F.col("ISS_DT").isNotNull())
                .filter(F.col("ISS_DT") >= F.lit(strt))
                .filter(F.col("ISS_DT") <= F.lit(end))
            )
            found = qualifying.count()
            actual_success = 1 if found > 0 else 0

            match_status = "OK" if actual_success == expected_success else "MISMATCH"
            if match_status == "MISMATCH":
                b2_mismatches += 1

            print(f"  {sr.MNE} {sr.TST_GRP_CD} CLNT={clnt} window=[{strt},{end}] "
                  f"expected={expected_success} raw_found={found} [{match_status}]")

            if match_status == "MISMATCH":
                print(f"    Raw records for this client:")
                raw_card_sample.filter(F.col("CLNT_NO") == clnt).show(truncate=False)
    else:
        print("No VCN/VDA clients in sample.")

    print(f"\nCard acquisition mismatches: {b2_mismatches}")
else:
    print("Skipped — card data not readable.")
    b2_mismatches = -1

audit_results["B2_card_acquisition"] = "PASS" if b2_mismatches == 0 else ("SKIP" if b2_mismatches < 0 else "FAIL")


# --- Cell B3: Raw Card Activation Validation (VDT — card_activation) ---

print("=" * 60)
print("B3: RAW CARD ACTIVATION VALIDATION (VDT)")
print("=" * 60)

b3_mismatches = 0
if raw_card_audit is not None:
    # Re-derive sample for raw card (already loaded above as raw_card_sample)
    act_mnes = ["VDT"]
    act_samples = sample_clients.filter(F.col("MNE").isin(act_mnes))

    if act_samples.count() > 0:
        print("Card Activation validation (VDT):")
        print("-" * 60)

        act_rows = act_samples.collect()
        for sr in act_rows:
            clnt = str(sr.CLNT_NO)
            strt = sr.TREATMT_STRT_DT
            end = sr.TREATMT_END_DT
            expected_success = int(sr.SUCCESS)

            # Pipeline filter: STS_CD in ('06','08'), SRVC_ID==36, ACTV_DT not null, ACTV_DT in window
            qualifying = (
                raw_card_sample
                .filter(F.col("CLNT_NO") == clnt)
                .filter(F.col("STS_CD").isin(["06", "08"]))
                .filter(F.col("SRVC_ID") == 36)
                .filter(F.col("ACTV_DT").isNotNull())
                .filter(F.col("ACTV_DT") >= F.lit(strt))
                .filter(F.col("ACTV_DT") <= F.lit(end))
            )
            found = qualifying.count()
            actual_success = 1 if found > 0 else 0

            match_status = "OK" if actual_success == expected_success else "MISMATCH"
            if match_status == "MISMATCH":
                b3_mismatches += 1

            print(f"  VDT {sr.TST_GRP_CD} CLNT={clnt} window=[{strt},{end}] "
                  f"expected={expected_success} raw_found={found} [{match_status}]")

            if match_status == "MISMATCH":
                print(f"    Raw records for this client:")
                raw_card_sample.filter(F.col("CLNT_NO") == clnt).show(truncate=False)
    else:
        print("No VDT clients in sample.")

    print(f"\nCard activation mismatches: {b3_mismatches}")
else:
    print("Skipped — card data not readable.")
    b3_mismatches = -1

audit_results["B3_card_activation"] = "PASS" if b3_mismatches == 0 else ("SKIP" if b3_mismatches < 0 else "FAIL")


# --- Cell B4: Raw POS Validation (VUI — card_usage, also secondary for VUT/VAW) ---

print("=" * 60)
print("B4: RAW POS VALIDATION (card_usage)")
print("=" * 60)
print("Querying DDWTA_T_PT_OF_SALE_TXN directly for sample clients.\n")

txn_paths = [POS_TXN_PATH.format(year=y) for y in YEARS]
try:
    raw_txn_audit = spark.read.parquet(*txn_paths)
except Exception as e:
    print(f"ERROR: Could not read POS data: {e}")
    raw_txn_audit = None

b4_mismatches = 0
if raw_txn_audit is not None:
    # OPTIMIZATION: Push native filters BEFORE CLNT_NO derivation to reduce scan
    # SRVC_CD==36 is a raw column — pushes down to parquet predicate
    raw_pos_sample = (
        raw_txn_audit
        .filter(F.col("SRVC_CD") == 36)  # Pushdown: eliminates most rows before derivation
        .filter(
            ((F.col("TXN_TP") == 10) & (F.col("MSG_TP") == "0210")) |
            ((F.col("TXN_TP") == 13) & (F.col("MSG_TP") == "0210")) |
            ((F.col("TXN_TP") == 12) & (F.col("MSG_TP") == "0220"))
        )
        .filter(F.col("AMT1") > 0)
        .withColumn("CLNT_NO", F.regexp_replace(F.substring(F.col("CLNT_CRD_NO"), 7, 9), "^0+", ""))
        .filter(F.col("CLNT_NO").isin(sample_clnt_list))
        .select(
            "CLNT_NO", "CLNT_CRD_NO", "TXN_TP", "MSG_TP", "AMT1",
            F.col("TXN_DT").cast("date").alias("TXN_DT"),
            "SRVC_CD"
        )
    )
    raw_pos_sample.persist(StorageLevel.MEMORY_AND_DISK)

    print(f"Raw POS records for sample clients (after all filters): {raw_pos_sample.count()}")

    # raw_pos_filtered is now the same as raw_pos_sample (filters already applied)
    raw_pos_filtered = raw_pos_sample

    print(f"Raw POS records after pipeline filters: {raw_pos_filtered.count()}")
    print("\nFiltered POS records for sample clients:")
    raw_pos_filtered.show(100, truncate=False)

    # Validate VUI (card_usage primary), VUT/VAW (card_usage secondary)
    usage_mnes = METRIC_TO_CAMPAIGNS.get("card_usage", [])  # ['VUI', 'VUT', 'VAW']
    usage_samples = sample_clients.filter(F.col("MNE").isin(usage_mnes))

    if usage_samples.count() > 0:
        print(f"\nCard Usage validation ({usage_mnes}):")
        print("-" * 60)

        usage_rows = usage_samples.collect()
        for sr in usage_rows:
            clnt = str(sr.CLNT_NO)
            strt = sr.TREATMT_STRT_DT
            end = sr.TREATMT_END_DT
            mne = str(sr.MNE)

            # For VUI: SUCCESS is based on USAGE_SUCCESS (primary)
            # For VUT/VAW: SUCCESS is based on PROVISIONING_SUCCESS (primary), but USAGE_SUCCESS is secondary
            # Here we check USAGE_SUCCESS column value from result_df
            result_row = result_df.filter(
                (F.col("CLNT_NO") == clnt) &
                (F.col("MNE") == mne) &
                (F.col("TREATMT_STRT_DT") == F.lit(strt)) &
                (F.col("TACTIC_ID") == str(sr.TACTIC_ID))
            ).select("USAGE_SUCCESS").collect()

            expected_usage = int(result_row[0].USAGE_SUCCESS) if result_row else -1

            qualifying = (
                raw_pos_filtered
                .filter(F.col("CLNT_NO") == clnt)
                .filter(F.col("TXN_DT") >= F.lit(strt))
                .filter(F.col("TXN_DT") <= F.lit(end))
            )
            found = qualifying.count()
            actual_usage = 1 if found > 0 else 0

            match_status = "OK" if actual_usage == expected_usage else "MISMATCH"
            if match_status == "MISMATCH":
                b4_mismatches += 1

            print(f"  {mne} {sr.TST_GRP_CD} CLNT={clnt} window=[{strt},{end}] "
                  f"expected_USAGE={expected_usage} raw_found={found} [{match_status}]")

            if match_status == "MISMATCH":
                print(f"    Raw POS records for this client:")
                raw_pos_filtered.filter(F.col("CLNT_NO") == clnt).show(truncate=False)
    else:
        print("No VUI/VUT/VAW clients in sample.")

    print(f"\nCard usage mismatches: {b4_mismatches}")
else:
    print("Skipped — POS data not readable.")
    b4_mismatches = -1

audit_results["B4_card_usage"] = "PASS" if b4_mismatches == 0 else ("SKIP" if b4_mismatches < 0 else "FAIL")


# --- Cell B5: Raw EDW Validation (VUT, VAW — wallet_provisioning) ---

print("=" * 60)
print("B5: RAW EDW VALIDATION (wallet_provisioning)")
print("=" * 60)
print("Querying EDW (Teradata) for sample VUT/VAW clients.\n")

prov_mnes = ["VUT", "VAW"]
prov_samples = sample_clients.filter(F.col("MNE").isin(prov_mnes))
prov_sample_rows = prov_samples.collect()

b5_mismatches = 0

if len(prov_sample_rows) == 0:
    print("No VUT/VAW clients in sample. Skipping EDW validation.")
    audit_results["B5_wallet_prov"] = "SKIP"
else:
    try:
        cursor = EDW.cursor()

        # Build IN clause for sample clients (EDW CLNT_NO is integer)
        prov_clnt_list = list(set(str(r.CLNT_NO) for r in prov_sample_rows))
        in_clause = ", ".join(prov_clnt_list)
        min_year = min(YEARS)

        # Query raw EDW data — same logic as pipeline wallet_query
        edw_audit_query = f"""
        SELECT DISTINCT
            CAST(SUBSTR(B.CLNT_CRD_NO, 7, 9) AS INTEGER) AS CLNT_NO,
            B.TXN_DT AS SUCCESS_DT,
            B.AMT1,
            SUBSTR(B.CLNT_CRD_NO, 1, 5) AS CARD_PREFIX,
            SUBSTR(B.VISA_DR_CRD_NO, 1, 5) AS VISA_PREFIX,
            SUBSTR(B.TOKN_REQSTR_ID, 1, 1) AS TOKEN_FIRST_CHAR,
            B.POS_ENTR_MODE_CD_NON_EMV,
            B.SRVC_CD,
            C.TOKEN_WALLET_IND
        FROM DDWV05.CLNT_CRD_POS_LOG B
        INNER JOIN DL_DECMAN.TOKEN_LIST C
            ON B.TOKN_REQSTR_ID = C.TOKEN_ID
        WHERE CAST(SUBSTR(B.CLNT_CRD_NO, 7, 9) AS INTEGER) IN ({in_clause})
            AND B.TXN_DT >= DATE '{min_year}-01-01'
        """

        print("Executing EDW query for sample clients...")
        cursor.execute(edw_audit_query)
        edw_results = cursor.fetchall()
        cursor.close()

        print(f"Raw EDW records for sample clients: {len(edw_results)}")

        # Show all raw records
        if edw_results:
            print("\nRaw EDW records (all, before filter):")
            print(f"{'CLNT_NO':>10} {'SUCCESS_DT':>12} {'AMT1':>6} {'CARD_PFX':>8} {'VISA_PFX':>8} "
                  f"{'TKN_1ST':>7} {'POS_MODE':>8} {'SRVC':>5} {'WALLET':>7}")
            print("-" * 90)
            for r in edw_results[:50]:
                print(f"{str(int(r[0])):>10} {str(r[1]):>12} {r[2]:>6} {str(r[3]):>8} {str(r[4]):>8} "
                      f"{str(r[5]):>7} {str(r[6]):>8} {r[7]:>5} {str(r[8]):>7}")

        # Validate each sample
        print(f"\nWallet provisioning validation (VUT, VAW):")
        print("-" * 60)

        for sr in prov_sample_rows:
            clnt = str(sr.CLNT_NO)
            strt = sr.TREATMT_STRT_DT
            end = sr.TREATMT_END_DT

            # Get expected PROVISIONING_SUCCESS from result_df
            result_row = result_df.filter(
                (F.col("CLNT_NO") == clnt) &
                (F.col("MNE") == str(sr.MNE)) &
                (F.col("TREATMT_STRT_DT") == F.lit(strt)) &
                (F.col("TACTIC_ID") == str(sr.TACTIC_ID))
            ).select("PROVISIONING_SUCCESS").collect()

            expected_prov = int(result_row[0].PROVISIONING_SUCCESS) if result_row else -1

            # Apply same filters as pipeline: AMT1=0, card prefix 45190, visa prefix 45199,
            # token first char > '0', POS_ENTR_MODE='000', SRVC_CD=36, TOKEN_WALLET_IND='Y',
            # TXN_DT in window
            qualifying = [
                r for r in edw_results
                if str(int(r[0])) == clnt
                and r[2] == 0                          # AMT1 = 0
                and str(r[3]) == '45190'               # card prefix
                and str(r[4]) == '45199'               # visa prefix
                and str(r[5]) > '0'                    # token first char > '0'
                and str(r[6]) == '000'                 # POS_ENTR_MODE
                and r[7] == 36                         # SRVC_CD
                and str(r[8]) == 'Y'                   # TOKEN_WALLET_IND
                and r[1] is not None
                and r[1] >= strt
                and r[1] <= end
            ]
            found = len(qualifying)
            actual_prov = 1 if found > 0 else 0

            match_status = "OK" if actual_prov == expected_prov else "MISMATCH"
            if match_status == "MISMATCH":
                b5_mismatches += 1

            print(f"  {sr.MNE} {sr.TST_GRP_CD} CLNT={clnt} window=[{strt},{end}] "
                  f"expected_PROV={expected_prov} raw_found={found} [{match_status}]")

            if match_status == "MISMATCH":
                # Show all raw records for this client
                client_records = [r for r in edw_results if str(int(r[0])) == clnt]
                print(f"    All raw EDW records for CLNT_NO={clnt}: {len(client_records)}")
                for cr in client_records:
                    print(f"      DT={cr[1]} AMT1={cr[2]} CARD={cr[3]} VISA={cr[4]} "
                          f"TKN1={cr[5]} POS={cr[6]} SRVC={cr[7]} WALLET={cr[8]}")

        print(f"\nWallet provisioning mismatches: {b5_mismatches}")
        audit_results["B5_wallet_prov"] = "PASS" if b5_mismatches == 0 else "FAIL"

    except NameError:
        print("EDW cursor not available in this session. Skipping.")
        audit_results["B5_wallet_prov"] = "SKIP"
    except Exception as e:
        print(f"EDW query failed: {e}")
        audit_results["B5_wallet_prov"] = "SKIP"


# --- Cell B6: CLNT_NO Derivation Consistency Check ---

print("=" * 60)
print("B6: CLNT_NO DERIVATION CONSISTENCY CHECK")
print("=" * 60)
print("Verifying that CLNT_NO resolves to the same value across all sources.\n")

# Pick a few sample clients to trace through
trace_clients = sample_clnt_list[:5]

# 1. Tactic table: use result_df (already has both raw and derived info)
print("1. Tactic table: TACTIC_ID -> derived MNE, CLNT_NO from result_df")
print("-" * 60)
result_df.filter(
    F.col("CLNT_NO").isin(trace_clients)
).select(
    "CLNT_NO", "TACTIC_ID",
    F.substring(F.col("TACTIC_ID"), 8, 3).alias("DERIVED_MNE"),
    "MNE"
).distinct().show(20, truncate=False)

# 2. Card table: use already-persisted raw_card_sample (from B2)
print("\n2. Card table: CLNT_NO from persisted raw_card_sample")
print("-" * 60)
if raw_card_sample is not None:
    raw_card_sample.filter(
        F.col("CLNT_NO").isin(trace_clients)
    ).select("CLNT_NO", "CRD_NO").distinct().show(20, truncate=False)
else:
    print("Card data not available.")

# 3. POS table: use already-persisted raw_pos_sample (from B4)
print("\n3. POS table: CLNT_CRD_NO -> CLNT_NO from persisted raw_pos_sample")
print("-" * 60)
if raw_pos_sample is not None:
    raw_pos_sample.filter(
        F.col("CLNT_NO").isin(trace_clients)
    ).select("CLNT_CRD_NO", "CLNT_NO").distinct().show(20, truncate=False)
else:
    print("POS data not available.")

print("Review above: all three sources should resolve to the same CLNT_NO for each client.")
audit_results["B6_clnt_no_consistency"] = "INFO"


# --- Cell B7: Deduplication Audit ---

print("=" * 60)
print("B7: DEDUPLICATION AUDIT")
print("=" * 60)
print("Finding clients with MULTIPLE success events in the same treatment window.")
print("Verifying only the FIRST (earliest) is kept in result_df.\n")

b7_pass = True

for mne, cfg in CAMPAIGNS.items():
    success_col = METRIC_TO_COLUMN[cfg["primary_metric"]]
    dt_col = f"FIRST_{success_col}_DT"

    # Find SUCCESS=1 rows for this campaign
    success_rows = result_df.filter(
        (F.col("MNE") == mne) & (F.col("SUCCESS") == 1)
    )

    if success_rows.count() == 0:
        continue

    # Get a few sample success clients for this campaign
    sample_success = success_rows.select(
        "CLNT_NO", "TACTIC_ID", "TREATMT_STRT_DT", "TREATMT_END_DT", dt_col
    ).limit(5).collect()

    if not sample_success:
        continue

    metric = cfg["primary_metric"]
    print(f"\n{mne} ({metric}) — checking deduplication:")
    print("-" * 60)

    for sr in sample_success:
        clnt = str(sr.CLNT_NO)
        strt = sr.TREATMT_STRT_DT
        end = sr.TREATMT_END_DT
        pipeline_dt = sr[dt_col]

        # Use PERSISTED samples from B2/B4 — no re-reading raw tables
        if metric == "card_acquisition" and raw_card_sample is not None:
            raw_events = (
                raw_card_sample
                .filter(F.col("CLNT_NO") == clnt)
                .filter(F.col("STS_CD").isin(["06", "08"]))
                .filter(F.col("ISS_DT").isNotNull())
                .filter(F.col("ISS_DT") >= F.lit(strt))
                .filter(F.col("ISS_DT") <= F.lit(end))
                .select(F.col("ISS_DT").alias("SUCCESS_DT"))
                .orderBy("SUCCESS_DT")
            )
        elif metric == "card_activation" and raw_card_sample is not None:
            raw_events = (
                raw_card_sample
                .filter(F.col("CLNT_NO") == clnt)
                .filter(F.col("STS_CD").isin(["06", "08"]))
                .filter(F.col("ACTV_DT").isNotNull())
                .filter(F.col("ACTV_DT") >= F.lit(strt))
                .filter(F.col("ACTV_DT") <= F.lit(end))
                .select(F.col("ACTV_DT").alias("SUCCESS_DT"))
                .orderBy("SUCCESS_DT")
            )
        elif metric == "card_usage" and raw_pos_sample is not None:
            raw_events = (
                raw_pos_sample
                .filter(F.col("CLNT_NO") == clnt)
                .filter(F.col("TXN_DT") >= F.lit(strt))
                .filter(F.col("TXN_DT") <= F.lit(end))
                .select(F.col("TXN_DT").alias("SUCCESS_DT"))
                .orderBy("SUCCESS_DT")
            )
        else:
            # wallet_provisioning — skip raw check (EDW), just report what's in result_df
            print(f"  {mne} CLNT={clnt}: pipeline_dt={pipeline_dt} (EDW — raw check skipped)")
            continue

        raw_count = raw_events.count()
        raw_dates = [str(r.SUCCESS_DT) for r in raw_events.collect()]

        if raw_count > 1:
            earliest_raw = raw_dates[0]
            pipeline_matches = str(pipeline_dt) == earliest_raw
            status = "OK" if pipeline_matches else "FAIL"
            if not pipeline_matches:
                b7_pass = False
            print(f"  {mne} CLNT={clnt}: {raw_count} raw events {raw_dates}")
            print(f"    pipeline_dt={pipeline_dt}, earliest_raw={earliest_raw} [{status}]")
        else:
            print(f"  {mne} CLNT={clnt}: {raw_count} raw event(s) {raw_dates}, pipeline_dt={pipeline_dt} [OK — single event]")

audit_results["B7_deduplication"] = "PASS" if b7_pass else "FAIL"


# --- Cell B8: Summary ---

print("=" * 60)
print("=" * 60)
print("AUDIT SUMMARY")
print("=" * 60)
print("=" * 60)

total_checks = len(audit_results)
passes = sum(1 for v in audit_results.values() if v == "PASS")
fails = sum(1 for v in audit_results.values() if v == "FAIL")
skips = sum(1 for v in audit_results.values() if v == "SKIP")
infos = sum(1 for v in audit_results.values() if v == "INFO")

print(f"\n{'Check':<30} {'Result':>8}")
print("-" * 40)
for check, result in sorted(audit_results.items()):
    marker = ""
    if result == "FAIL":
        marker = " <<<<<"
    print(f"{check:<30} {result:>8}{marker}")

print(f"\n{'='*40}")
print(f"Total checks: {total_checks}")
print(f"  PASS: {passes}")
print(f"  FAIL: {fails}")
print(f"  SKIP: {skips}")
print(f"  INFO: {infos}")

if fails > 0:
    print(f"\nACTION REQUIRED: {fails} check(s) FAILED. Review details above.")
else:
    print(f"\nAll validation checks passed (excluding {skips} skipped, {infos} info-only).")

# Cleanup all persisted audit DataFrames
sample_clients.unpersist()
try:
    raw_card_sample.unpersist()
except:
    pass
try:
    raw_pos_sample.unpersist()
except:
    pass
print("\nAll audit DataFrames unpersisted.")
print("Audit complete.")
