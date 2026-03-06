#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================================================================
# VVD v2 Audit / Diagnostic Notebook — STANDALONE, SLICE-BASED
#
# Change AUDIT_MNES/AUDIT_YEARS/AUDIT_COHORTS to test different slices.
# Each run validates the pipeline logic on the selected slice.
#
# STANDALONE — no dependency on the main pipeline. Produces its own data
# via the same M1/M3/M6 logic, but scoped to a small configurable slice.
#
# SLICE-BASED — does NOT load all 3 years x 6 campaigns. Loads only the
# campaigns and years specified in the audit config. Should run in under
# 5 minutes on Lumina.
#
# Structure:
#   Cell  0: Audit configuration (edit this to change the slice)
#   Cell  1: M1 — Experiment population (sliced)
#   Cell  2: M3 — Success outcomes (sliced, raw tables kept for Section B)
#   Cell  3: M6 — Success detection (sliced)
#   Cells 4-9: Section A audit checks (A2-A7)
#   Cell 10: Sample selection (B1)
#   Cells 11-16: Section B raw source validation (B2-B7)
#   Cell 17: Summary (B8)
#   Cell 18: Cleanup
#
# Rules: PySpark only, no pandas, no function wrappers, flat cells.
# Environment: Lumina (PySpark on YARN, Hive tables, older CDH Spark)
# ====================================================================


# ============================================================
# CELL 0: AUDIT CONFIGURATION
# Edit AUDIT_MNES, AUDIT_YEARS, AUDIT_COHORTS to change the slice.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import StorageLevel
from collections import defaultdict
import random

try:
    from IPython.display import display, HTML
except ImportError:
    pass

# --- AUDIT SLICE CONTROLS ---
# Pick 2-3 campaigns covering different metric types.
# VDA = card_acquisition, VDT = card_activation, VUI = card_usage
AUDIT_MNES = ["VDA", "VDT", "VUI"]

# Restrict to a single year (or two). Fewer years = faster.
AUDIT_YEARS = [2025]

# Optional: restrict to specific cohorts (yyyy-MM). Set to None to skip.
# e.g., ["2025-07", "2025-08"] for a very tight scope.
AUDIT_COHORTS = None

# --- FULL CAMPAIGN METADATA (same as main pipeline) ---
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

# Metric -> success column mapping
METRIC_TO_COLUMN = {
    "card_acquisition": "ACQUISITION_SUCCESS",
    "card_activation": "ACTIVATION_SUCCESS",
    "card_usage": "USAGE_SUCCESS",
    "wallet_provisioning": "PROVISIONING_SUCCESS"
}

# --- SCOPED CONFIG (derived from AUDIT_MNES) ---
AUDIT_CAMPAIGNS = {k: v for k, v in CAMPAIGNS.items() if k in AUDIT_MNES}

# Reverse mapping: metric -> list of campaigns that use it (scoped to audit)
METRIC_TO_CAMPAIGNS = defaultdict(list)
for _mne, _cfg in AUDIT_CAMPAIGNS.items():
    METRIC_TO_CAMPAIGNS[_cfg["primary_metric"]].append(_mne)
    if "secondary_metric" in _cfg:
        METRIC_TO_CAMPAIGNS[_cfg["secondary_metric"]].append(_mne)
METRIC_TO_CAMPAIGNS = dict(METRIC_TO_CAMPAIGNS)

# Which metrics are actually needed by the audit slice?
AUDIT_METRICS = set(METRIC_TO_CAMPAIGNS.keys())

# MNE -> expected SUCCESS column (derived from primary_metric)
MNE_TO_SUCCESS_COL = {mne: METRIC_TO_COLUMN[cfg["primary_metric"]] for mne, cfg in AUDIT_CAMPAIGNS.items()}

# Source table paths (Hive parquet on HDFS)
TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
CARD_DATA_PATH = "/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT={year}*"
POS_TXN_PATH = "/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT={year}*"

ACTION_GROUP = "TG4"
CONTROL_GROUP = "TG7"

# Track pass/fail for summary
audit_results = {}

print("Audit configuration loaded.")
print(f"  AUDIT_MNES: {AUDIT_MNES}")
print(f"  AUDIT_YEARS: {AUDIT_YEARS}")
print(f"  AUDIT_COHORTS: {AUDIT_COHORTS}")
print(f"  Metrics needed: {sorted(AUDIT_METRICS)}")
print(f"  Metric -> campaigns: {METRIC_TO_CAMPAIGNS}")


# ============================================================
# CELL 1: M1 — EXPERIMENT POPULATION (SLICED)
# Same logic as pipeline Cell 2, scoped to AUDIT_MNES + AUDIT_YEARS.
# Should produce ~50K-200K rows, not 38M.
# ============================================================

tactic_paths = [TACTIC_EVNT_HIST_BASE + f"EVNT_STRT_DT={y}*" for y in AUDIT_YEARS]
raw_tactic = spark.read.option("basePath", TACTIC_EVNT_HIST_BASE).parquet(*tactic_paths)

tactic_df = (
    raw_tactic
    .filter(F.substring(F.col("TACTIC_ID"), 8, 3).isin(AUDIT_MNES))
    .withColumn("MNE", F.substring(F.col("TACTIC_ID"), 8, 3))
    .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("TACTIC_EVNT_ID")), "^0+", ""))
    .withColumn("TST_GRP_CD", F.trim(F.col("TST_GRP_CD")))
    .withColumn("RPT_GRP_CD", F.trim(F.col("RPT_GRP_CD")))
    .filter(F.col("TST_GRP_CD").isin([ACTION_GROUP, CONTROL_GROUP]))
    .withColumn("WINDOW_DAYS", F.datediff(F.col("TREATMT_END_DT"), F.col("TREATMT_STRT_DT")))
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .select(
        "CLNT_NO", "TACTIC_ID", "MNE", "TST_GRP_CD", "RPT_GRP_CD",
        "TREATMT_STRT_DT", "TREATMT_END_DT", "TREATMT_MN",
        "TACTIC_CELL_CD", "WINDOW_DAYS", "COHORT",
    )
    .distinct()
)

# Optional cohort filter for even tighter scope
if AUDIT_COHORTS is not None:
    tactic_df = tactic_df.filter(F.col("COHORT").isin(AUDIT_COHORTS))

tactic_df.persist(StorageLevel.MEMORY_AND_DISK)

tactic_count = tactic_df.count()
tactic_clients = tactic_df.select("CLNT_NO").distinct().count()
print(f"M1: Experiment population (sliced):")
print(f"  Total rows: {tactic_count:,}")
print(f"  Unique clients: {tactic_clients:,}")
print(f"  Campaigns: {sorted([r.MNE for r in tactic_df.select('MNE').distinct().collect()])}")
print(f"  Date range: {tactic_df.agg(F.min('TREATMT_STRT_DT')).collect()[0][0]} to {tactic_df.agg(F.max('TREATMT_STRT_DT')).collect()[0][0]}")
print(f"  Test groups: {[r.TST_GRP_CD for r in tactic_df.select('TST_GRP_CD').distinct().collect()]}")


# ============================================================
# CELL 2: M3 — SUCCESS OUTCOMES (SLICED)
# Only loads metrics needed by AUDIT_MNES.
# Only loads AUDIT_YEARS partitions.
# Pre-filters to experiment clients (left_semi).
# Raw tables (raw_card, raw_txn) are kept as module-level
# variables for reuse in Section B.
# ============================================================

experiment_clients = tactic_df.select("CLNT_NO").distinct()

# --- M3a: Card acquisition + activation ---
# Needed if card_acquisition or card_activation in AUDIT_METRICS
raw_card = None
card_acquisition_df = None
card_activation_df = None

if "card_acquisition" in AUDIT_METRICS or "card_activation" in AUDIT_METRICS:
    card_paths = [CARD_DATA_PATH.format(year=y) for y in AUDIT_YEARS]
    try:
        raw_card = spark.read.parquet(*card_paths)
    except Exception as e:
        print(f"Warning: Could not read card data: {e}")
        raw_card = None

if raw_card is not None:
    card_base = (
        raw_card
        .filter(F.col("STS_CD").isin(["06", "08"]))
        .filter(F.col("SRVC_ID") == 36)
        .filter(F.col("ISS_DT").isNotNull())
        .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("CLNT_NO")), "^0+", ""))
    )

    if "card_acquisition" in AUDIT_METRICS:
        card_acquisition_df = (
            card_base
            .select("CLNT_NO", F.col("ISS_DT").cast("date").alias("SUCCESS_DT"))
            .filter(F.col("SUCCESS_DT").isNotNull())
            .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
            .join(experiment_clients, "CLNT_NO", "left_semi")
        )
        print(f"M3a: card_acquisition: {card_acquisition_df.count():,} events (pre-filtered)")

    if "card_activation" in AUDIT_METRICS:
        card_activation_df = (
            card_base
            .filter(F.col("ACTV_DT").isNotNull())
            .select("CLNT_NO", F.col("ACTV_DT").cast("date").alias("SUCCESS_DT"))
            .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
            .join(experiment_clients, "CLNT_NO", "left_semi")
        )
        print(f"M3a: card_activation: {card_activation_df.count():,} events (pre-filtered)")
else:
    if "card_acquisition" in AUDIT_METRICS or "card_activation" in AUDIT_METRICS:
        print("WARNING: Card data not available. card_acquisition / card_activation will be empty.")

# Fill in empties for any metrics that are needed but missing
if card_acquisition_df is None and "card_acquisition" in AUDIT_METRICS:
    card_acquisition_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")
if card_activation_df is None and "card_activation" in AUDIT_METRICS:
    card_activation_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")

# --- M3b: Card usage ---
raw_txn = None
card_usage_df = None

if "card_usage" in AUDIT_METRICS:
    txn_paths = [POS_TXN_PATH.format(year=y) for y in AUDIT_YEARS]
    try:
        raw_txn = spark.read.parquet(*txn_paths)
    except Exception as e:
        print(f"Warning: Could not read POS data: {e}")
        raw_txn = None

    if raw_txn is not None:
        card_usage_df = (
            raw_txn
            .filter(F.col("SRVC_CD") == 36)
            .filter(
                ((F.col("TXN_TP") == 10) & (F.col("MSG_TP") == "0210")) |
                ((F.col("TXN_TP") == 13) & (F.col("MSG_TP") == "0210")) |
                ((F.col("TXN_TP") == 12) & (F.col("MSG_TP") == "0220"))
            )
            .filter(F.col("AMT1") > 0)
            .withColumn("CLNT_NO", F.regexp_replace(F.substring(F.col("CLNT_CRD_NO"), 7, 9), "^0+", ""))
            .select("CLNT_NO", F.col("TXN_DT").cast("date").alias("SUCCESS_DT"))
            .filter(F.col("SUCCESS_DT").isNotNull())
            .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
            .join(experiment_clients, "CLNT_NO", "left_semi")
        )
        print(f"M3b: card_usage: {card_usage_df.count():,} events (pre-filtered)")
    else:
        print("WARNING: POS data not available. card_usage will be empty.")

if card_usage_df is None and "card_usage" in AUDIT_METRICS:
    card_usage_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")

# --- M3c: Wallet provisioning (EDW) ---
wallet_provisioning_df = None

if "wallet_provisioning" in AUDIT_METRICS:
    min_year = min(AUDIT_YEARS)
    wallet_query = f"""
    SELECT DISTINCT
        CAST(SUBSTR(B.CLNT_CRD_NO, 7, 9) AS INTEGER) AS CLNT_NO,
        B.TXN_DT AS SUCCESS_DT
    FROM DDWV05.CLNT_CRD_POS_LOG B
    INNER JOIN DL_DECMAN.TOKEN_LIST C
        ON B.TOKN_REQSTR_ID = C.TOKEN_ID
    WHERE B.AMT1 = 0
        AND SUBSTR(B.CLNT_CRD_NO, 1, 5) = '45190'
        AND SUBSTR(B.VISA_DR_CRD_NO, 1, 5) = '45199'
        AND SUBSTR(B.TOKN_REQSTR_ID, 1, 1) > '0'
        AND B.POS_ENTR_MODE_CD_NON_EMV = '000'
        AND B.SRVC_CD = 36
        AND C.TOKEN_WALLET_IND = 'Y'
        AND B.TXN_DT >= DATE '{min_year}-01-01'
    """

    try:
        cursor = EDW.cursor()
        cursor.execute(wallet_query)
        results = cursor.fetchall()
        cursor.close()
        wallet_provisioning_df = spark.createDataFrame(
            [(str(int(r[0])), r[1]) for r in results],
            "CLNT_NO STRING, SUCCESS_DT DATE"
        )
        wallet_provisioning_df = wallet_provisioning_df.join(experiment_clients, "CLNT_NO", "left_semi")
        print(f"M3c: wallet_provisioning: {wallet_provisioning_df.count():,} events (pre-filtered)")
    except NameError:
        print("WARNING: EDW cursor not available. wallet_provisioning will be empty.")
    except Exception as e:
        print(f"WARNING: wallet_provisioning extraction failed: {e}")

if wallet_provisioning_df is None and "wallet_provisioning" in AUDIT_METRICS:
    wallet_provisioning_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")

# Build success_dfs dict (only metrics needed by audit)
success_dfs = {}
if "card_acquisition" in AUDIT_METRICS:
    success_dfs["card_acquisition"] = card_acquisition_df
if "card_activation" in AUDIT_METRICS:
    success_dfs["card_activation"] = card_activation_df
if "card_usage" in AUDIT_METRICS:
    success_dfs["card_usage"] = card_usage_df
if "wallet_provisioning" in AUDIT_METRICS:
    success_dfs["wallet_provisioning"] = wallet_provisioning_df

print(f"\nSuccess DFs loaded for metrics: {sorted(success_dfs.keys())}")
print(f"Experiment clients: {experiment_clients.count():,}")


# ============================================================
# CELL 3: M6 — SUCCESS DETECTION (SLICED)
# Same logic as pipeline Cell 4, operating on sliced data.
# Persists result_df, unpersists tactic_df.
# ============================================================

result_df = tactic_df

for metric_name in METRIC_TO_COLUMN:
    success_col_name = METRIC_TO_COLUMN[metric_name]
    relevant_mnes = METRIC_TO_CAMPAIGNS.get(metric_name, [])

    if not relevant_mnes:
        # No audit campaigns use this metric — add zero columns for schema consistency
        result_df = (
            result_df
            .withColumn(success_col_name, F.lit(0))
            .withColumn(f"FIRST_{success_col_name}_DT", F.lit(None).cast("date"))
        )
        continue

    sdf = success_dfs[metric_name]

    # Split: campaigns that need this metric vs campaigns that don't
    needs_metric = result_df.filter(F.col("MNE").isin(relevant_mnes))
    skip_metric = result_df.filter(~F.col("MNE").isin(relevant_mnes))

    # Join only relevant campaign rows (column renaming avoids ambiguity)
    sdf_renamed = sdf.withColumnRenamed("CLNT_NO", "S_CLNT_NO").withColumnRenamed("SUCCESS_DT", "S_SUCCESS_DT")

    joined = (
        needs_metric
        .join(
            sdf_renamed,
            (F.col("CLNT_NO") == F.col("S_CLNT_NO")) &
            (F.col("S_SUCCESS_DT") >= F.col("TREATMT_STRT_DT")) &
            (F.col("S_SUCCESS_DT") <= F.col("TREATMT_END_DT")),
            "left"
        )
        .withColumn(success_col_name, F.when(F.col("S_SUCCESS_DT").isNotNull(), 1).otherwise(0))
        .withColumn(f"FIRST_{success_col_name}_DT", F.col("S_SUCCESS_DT"))
    )

    # Keep first success per deployment (deduplicate from multiple success events)
    w = Window.partitionBy("CLNT_NO", "TACTIC_ID", "MNE", "TREATMT_STRT_DT").orderBy(F.col("S_SUCCESS_DT").asc_nulls_last())
    joined = joined.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    joined = joined.drop("S_CLNT_NO", "S_SUCCESS_DT")

    # Skipped campaigns get 0 / NULL for this metric (no shuffle cost)
    skip_metric = (
        skip_metric
        .withColumn(success_col_name, F.lit(0))
        .withColumn(f"FIRST_{success_col_name}_DT", F.lit(None).cast("date"))
    )

    result_df = joined.unionByName(skip_metric)

# Assign each row its campaign-specific SUCCESS flag (driven by config)
success_expr = F.lit(0)
for mne, cfg in AUDIT_CAMPAIGNS.items():
    col_name = METRIC_TO_COLUMN[cfg["primary_metric"]]
    success_expr = F.when(F.col("MNE") == mne, F.col(col_name)).otherwise(success_expr)
result_df = result_df.withColumn("SUCCESS", success_expr)

# Persist result_df for audit checks, release tactic_df
result_df.persist(StorageLevel.MEMORY_AND_DISK)
result_count = result_df.count()
tactic_df.unpersist()

print(f"M6: Result DataFrame: {result_count:,} rows")
print(f"Columns: {result_df.columns}")
print(f"\nSuccess flag distribution:")
result_df.groupBy("MNE", "TST_GRP_CD").agg(
    F.count("*").alias("total"),
    F.sum("SUCCESS").alias("successes"),
    (F.sum("SUCCESS") / F.count("*") * 100).alias("success_rate_pct")
).orderBy("MNE", "TST_GRP_CD").show(50, truncate=False)

print("Data pipeline (sliced) complete. result_df is persisted.")


# ============================================================
# CELL 4: A2 — TREATMT_END_DT NULL CHECK
# ============================================================

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

    print("Null TREATMT_END_DT by campaign:")
    null_end_dt.groupBy("MNE").agg(
        F.count("*").alias("null_count")
    ).orderBy("MNE").show(truncate=False)

    audit_results["A2_end_dt_null"] = "FAIL"
else:
    print("PASS: All rows have non-null TREATMT_END_DT.")
    audit_results["A2_end_dt_null"] = "PASS"


# ============================================================
# CELL 5: A3 — TACTIC_CELL_CD (CHANNEL) DISTRIBUTION
# ============================================================

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

null_channel = result_df.filter(
    F.col("TACTIC_CELL_CD").isNull() | (F.trim(F.col("TACTIC_CELL_CD")) == "")
).count()
print(f"Rows with NULL or empty TACTIC_CELL_CD: {null_channel:,}")

audit_results["A3_channel_dist"] = "INFO"


# ============================================================
# CELL 6: A4 — RPT_GRP_CD DISTRIBUTION
# ============================================================

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


# ============================================================
# CELL 7: A5 — SUCCESS FLAG VALIDATION (INTERNAL CONSISTENCY)
# ============================================================

print("=" * 60)
print("A5: SUCCESS FLAG VALIDATION")
print("=" * 60)
print("Verifying SUCCESS == expected metric column for each campaign.\n")

a5_pass = True

for mne, expected_col in MNE_TO_SUCCESS_COL.items():
    mne_df = result_df.filter(F.col("MNE") == mne)
    total = mne_df.count()

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


# ============================================================
# CELL 8: A6 — DATE WINDOW VALIDATION
# ============================================================

print("=" * 60)
print("A6: DATE WINDOW VALIDATION (SUCCESS=1 rows)")
print("=" * 60)

a6_pass = True

for mne, cfg in AUDIT_CAMPAIGNS.items():
    success_col = METRIC_TO_COLUMN[cfg["primary_metric"]]
    dt_col = f"FIRST_{success_col}_DT"

    success_rows = result_df.filter(
        (F.col("MNE") == mne) & (F.col("SUCCESS") == 1)
    )
    total_success = success_rows.count()

    if total_success == 0:
        print(f"  {mne}: No SUCCESS=1 rows (skipped)")
        continue

    before_start = success_rows.filter(F.col(dt_col) < F.col("TREATMT_STRT_DT")).count()
    after_end = success_rows.filter(F.col(dt_col) > F.col("TREATMT_END_DT")).count()
    null_dt = success_rows.filter(F.col(dt_col).isNull()).count()

    same_day_start = success_rows.filter(F.col(dt_col) == F.col("TREATMT_STRT_DT")).count()
    same_day_end = success_rows.filter(F.col(dt_col) == F.col("TREATMT_END_DT")).count()

    violations = before_start + after_end + null_dt
    status = "PASS" if violations == 0 else "FAIL"

    print(f"  {mne} ({dt_col}):")
    print(f"    Total SUCCESS=1: {total_success:,}")
    print(f"    Before TREATMT_STRT_DT: {before_start:,}")
    print(f"    After TREATMT_END_DT: {after_end:,}")
    print(f"    NULL date despite SUCCESS=1: {null_dt:,}")
    print(f"    Edge -- same day as STRT: {same_day_start:,}")
    print(f"    Edge -- same day as END: {same_day_end:,}")
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


# ============================================================
# CELL 9: A7 — DEPLOYMENT GRANULARITY CHECK
# ============================================================

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
# CELL 10: B1 — SAMPLE SELECTION
# Since the data is already small (sliced), we can be more
# thorough with samples.
# ============================================================

print("=" * 60)
print("B1: SAMPLE SELECTION")
print("=" * 60)

success_1_samples = []
success_0_samples = []

for mne in AUDIT_MNES:
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
            picked = random.sample(s1_pool, min(3, len(s1_pool)))
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
            picked = random.sample(s0_pool, min(3, len(s0_pool)))
            success_0_samples.extend(picked)

random.shuffle(success_1_samples)
random.shuffle(success_0_samples)
success_1_samples = success_1_samples[:25]
success_0_samples = success_0_samples[:25]

# --- TARGETED EDGE CASES ---
edge_case_samples = []

# 1. Shortest treatment windows (WINDOW_DAYS close to 0)
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
        .limit(5).collect()
    )
    n_multi += len(mc_rows)
    edge_case_samples.extend(mc_rows)

# 4. SUCCESS=1 where success date == TREATMT_STRT_DT (same-day boundary)
n_boundary = 0
for mne, cfg in AUDIT_CAMPAIGNS.items():
    dt_col = f"FIRST_{METRIC_TO_COLUMN[cfg['primary_metric']]}_DT"
    boundary = (
        result_df.filter(
            (F.col("MNE") == mne) &
            (F.col("SUCCESS") == 1) &
            (F.col(dt_col) == F.col("TREATMT_STRT_DT"))
        )
        .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
                "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
        .limit(2).collect()
    )
    n_boundary += len(boundary)
    edge_case_samples.extend(boundary)

# 5. SUCCESS=1 where success date == TREATMT_END_DT (end boundary)
n_end_boundary = 0
for mne, cfg in AUDIT_CAMPAIGNS.items():
    dt_col = f"FIRST_{METRIC_TO_COLUMN[cfg['primary_metric']]}_DT"
    end_boundary = (
        result_df.filter(
            (F.col("MNE") == mne) &
            (F.col("SUCCESS") == 1) &
            (F.col(dt_col) == F.col("TREATMT_END_DT"))
        )
        .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
                "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
        .limit(2).collect()
    )
    n_end_boundary += len(end_boundary)
    edge_case_samples.extend(end_boundary)

print(f"Edge case samples collected: {len(edge_case_samples)}")
print(f"  Shortest windows: {len(shortest)}")
print(f"  Longest windows: {len(longest)}")
print(f"  Multi-campaign clients: {n_multi} rows from {len(multi_camp)} clients")
print(f"  Same-day start boundary: {n_boundary}")
print(f"  Same-day end boundary: {n_end_boundary}")

all_samples = success_1_samples + success_0_samples + edge_case_samples

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

print(f"\nSelected {len(success_1_samples)} SUCCESS=1 and {len(success_0_samples)} SUCCESS=0 samples")
print(f"Total sample: {sample_clients.count()} rows")
print("\nSUCCESS=1 samples:")
sample_clients.filter(F.col("SUCCESS") == 1).show(30, truncate=False)
print("SUCCESS=0 samples:")
sample_clients.filter(F.col("SUCCESS") == 0).show(30, truncate=False)

sample_clnt_list = [str(r.CLNT_NO) for r in sample_clients.select("CLNT_NO").distinct().collect()]
print(f"Unique sample clients: {len(sample_clnt_list)}")


# ============================================================
# CELL 11: B2 — RAW CARD TABLE VALIDATION (card_acquisition)
# REUSES raw_card from M3 (Cell 2) — no re-reading raw tables.
# ============================================================

print("=" * 60)
print("B2: RAW CARD TABLE VALIDATION (card_acquisition)")
print("=" * 60)

b2_mismatches = 0
raw_card_sample = None

if raw_card is not None:
    # Filter raw_card (already loaded in M3) to sample clients, persist for reuse in B3/B6/B7
    raw_card_sample = (
        raw_card
        .filter(F.col("SRVC_ID") == 36)
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

    # Validate acquisition campaigns in audit scope
    acq_mnes = [m for m in AUDIT_MNES if AUDIT_CAMPAIGNS[m]["primary_metric"] == "card_acquisition"]
    acq_samples = sample_clients.filter(F.col("MNE").isin(acq_mnes))

    if acq_samples.count() > 0:
        print(f"\nCard Acquisition validation ({acq_mnes}):")
        print("-" * 60)

        acq_rows = acq_samples.collect()
        for sr in acq_rows:
            clnt = str(sr.CLNT_NO)
            strt = sr.TREATMT_STRT_DT
            end = sr.TREATMT_END_DT
            expected_success = int(sr.SUCCESS)

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
        print("No card_acquisition campaigns in audit scope or sample.")

    print(f"\nCard acquisition mismatches: {b2_mismatches}")
else:
    if "card_acquisition" in AUDIT_METRICS:
        print("Skipped -- card data not readable.")
        b2_mismatches = -1
    else:
        print("Skipped -- card_acquisition not in audit metrics.")
        b2_mismatches = 0

audit_results["B2_card_acquisition"] = "PASS" if b2_mismatches == 0 else ("SKIP" if b2_mismatches < 0 else "FAIL")


# ============================================================
# CELL 12: B3 — RAW CARD ACTIVATION VALIDATION (VDT)
# REUSES raw_card_sample from B2 — no re-reading.
# ============================================================

print("=" * 60)
print("B3: RAW CARD ACTIVATION VALIDATION (card_activation)")
print("=" * 60)

b3_mismatches = 0

if raw_card_sample is not None and "card_activation" in AUDIT_METRICS:
    act_mnes = [m for m in AUDIT_MNES if AUDIT_CAMPAIGNS[m]["primary_metric"] == "card_activation"]
    act_samples = sample_clients.filter(F.col("MNE").isin(act_mnes))

    if act_samples.count() > 0:
        print(f"Card Activation validation ({act_mnes}):")
        print("-" * 60)

        act_rows = act_samples.collect()
        for sr in act_rows:
            clnt = str(sr.CLNT_NO)
            strt = sr.TREATMT_STRT_DT
            end = sr.TREATMT_END_DT
            expected_success = int(sr.SUCCESS)

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

            print(f"  {sr.MNE} {sr.TST_GRP_CD} CLNT={clnt} window=[{strt},{end}] "
                  f"expected={expected_success} raw_found={found} [{match_status}]")

            if match_status == "MISMATCH":
                print(f"    Raw records for this client:")
                raw_card_sample.filter(F.col("CLNT_NO") == clnt).show(truncate=False)
    else:
        print("No card_activation campaigns in sample.")

    print(f"\nCard activation mismatches: {b3_mismatches}")
elif "card_activation" not in AUDIT_METRICS:
    print("Skipped -- card_activation not in audit metrics.")
else:
    print("Skipped -- card data not readable.")
    b3_mismatches = -1

audit_results["B3_card_activation"] = "PASS" if b3_mismatches == 0 else ("SKIP" if b3_mismatches < 0 else "FAIL")


# ============================================================
# CELL 13: B4 — RAW POS VALIDATION (card_usage)
# REUSES raw_txn from M3 (Cell 2) — no re-reading raw tables.
# ============================================================

print("=" * 60)
print("B4: RAW POS VALIDATION (card_usage)")
print("=" * 60)

b4_mismatches = 0
raw_pos_sample = None

if raw_txn is not None and "card_usage" in AUDIT_METRICS:
    # Filter raw_txn (already loaded in M3) to sample clients with pipeline filters
    raw_pos_sample = (
        raw_txn
        .filter(F.col("SRVC_CD") == 36)
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
    print("\nFiltered POS records for sample clients:")
    raw_pos_sample.show(100, truncate=False)

    # Validate campaigns that use card_usage (primary or secondary)
    usage_mnes = METRIC_TO_CAMPAIGNS.get("card_usage", [])
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

            # Get USAGE_SUCCESS from result_df for this specific row
            result_row = result_df.filter(
                (F.col("CLNT_NO") == clnt) &
                (F.col("MNE") == mne) &
                (F.col("TREATMT_STRT_DT") == F.lit(strt)) &
                (F.col("TACTIC_ID") == str(sr.TACTIC_ID))
            ).select("USAGE_SUCCESS").collect()

            expected_usage = int(result_row[0].USAGE_SUCCESS) if result_row else -1

            qualifying = (
                raw_pos_sample
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
                raw_pos_sample.filter(F.col("CLNT_NO") == clnt).show(truncate=False)
    else:
        print("No card_usage campaigns in sample.")

    print(f"\nCard usage mismatches: {b4_mismatches}")
elif "card_usage" not in AUDIT_METRICS:
    print("Skipped -- card_usage not in audit metrics.")
else:
    print("Skipped -- POS data not readable.")
    b4_mismatches = -1

audit_results["B4_card_usage"] = "PASS" if b4_mismatches == 0 else ("SKIP" if b4_mismatches < 0 else "FAIL")


# ============================================================
# CELL 14: B5 — RAW EDW VALIDATION (wallet_provisioning)
# Only runs if wallet_provisioning is in AUDIT_METRICS.
# ============================================================

print("=" * 60)
print("B5: RAW EDW VALIDATION (wallet_provisioning)")
print("=" * 60)

b5_mismatches = 0

if "wallet_provisioning" not in AUDIT_METRICS:
    print("Skipped -- wallet_provisioning not in audit metrics.")
    audit_results["B5_wallet_prov"] = "SKIP"
else:
    prov_mnes = [m for m in AUDIT_MNES if AUDIT_CAMPAIGNS[m]["primary_metric"] == "wallet_provisioning"]
    prov_samples = sample_clients.filter(F.col("MNE").isin(prov_mnes))
    prov_sample_rows = prov_samples.collect()

    if len(prov_sample_rows) == 0:
        print("No wallet_provisioning clients in sample. Skipping EDW validation.")
        audit_results["B5_wallet_prov"] = "SKIP"
    else:
        try:
            cursor = EDW.cursor()

            prov_clnt_list = list(set(str(r.CLNT_NO) for r in prov_sample_rows))
            in_clause = ", ".join(prov_clnt_list)
            min_year = min(AUDIT_YEARS)

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

            if edw_results:
                print("\nRaw EDW records (all, before filter):")
                print(f"{'CLNT_NO':>10} {'SUCCESS_DT':>12} {'AMT1':>6} {'CARD_PFX':>8} {'VISA_PFX':>8} "
                      f"{'TKN_1ST':>7} {'POS_MODE':>8} {'SRVC':>5} {'WALLET':>7}")
                print("-" * 90)
                for r in edw_results[:50]:
                    print(f"{str(int(r[0])):>10} {str(r[1]):>12} {r[2]:>6} {str(r[3]):>8} {str(r[4]):>8} "
                          f"{str(r[5]):>7} {str(r[6]):>8} {r[7]:>5} {str(r[8]):>7}")

            print(f"\nWallet provisioning validation ({prov_mnes}):")
            print("-" * 60)

            for sr in prov_sample_rows:
                clnt = str(sr.CLNT_NO)
                strt = sr.TREATMT_STRT_DT
                end = sr.TREATMT_END_DT

                result_row = result_df.filter(
                    (F.col("CLNT_NO") == clnt) &
                    (F.col("MNE") == str(sr.MNE)) &
                    (F.col("TREATMT_STRT_DT") == F.lit(strt)) &
                    (F.col("TACTIC_ID") == str(sr.TACTIC_ID))
                ).select("PROVISIONING_SUCCESS").collect()

                expected_prov = int(result_row[0].PROVISIONING_SUCCESS) if result_row else -1

                qualifying = [
                    r for r in edw_results
                    if str(int(r[0])) == clnt
                    and r[2] == 0
                    and str(r[3]) == '45190'
                    and str(r[4]) == '45199'
                    and str(r[5]) > '0'
                    and str(r[6]) == '000'
                    and r[7] == 36
                    and str(r[8]) == 'Y'
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


# ============================================================
# CELL 15: B6 — CLNT_NO DERIVATION CONSISTENCY CHECK
# REUSES raw_card_sample and raw_pos_sample — no re-reading.
# ============================================================

print("=" * 60)
print("B6: CLNT_NO DERIVATION CONSISTENCY CHECK")
print("=" * 60)
print("Verifying that CLNT_NO resolves to the same value across all sources.\n")

trace_clients = sample_clnt_list[:5]

# 1. Tactic table: from result_df
print("1. Tactic table: TACTIC_ID -> derived MNE, CLNT_NO from result_df")
print("-" * 60)
result_df.filter(
    F.col("CLNT_NO").isin(trace_clients)
).select(
    "CLNT_NO", "TACTIC_ID",
    F.substring(F.col("TACTIC_ID"), 8, 3).alias("DERIVED_MNE"),
    "MNE"
).distinct().show(20, truncate=False)

# 2. Card table: from persisted raw_card_sample (B2)
print("\n2. Card table: CLNT_NO from persisted raw_card_sample")
print("-" * 60)
if raw_card_sample is not None:
    raw_card_sample.filter(
        F.col("CLNT_NO").isin(trace_clients)
    ).select("CLNT_NO", "CRD_NO").distinct().show(20, truncate=False)
else:
    print("Card data not available.")

# 3. POS table: from persisted raw_pos_sample (B4)
print("\n3. POS table: CLNT_CRD_NO -> CLNT_NO from persisted raw_pos_sample")
print("-" * 60)
if raw_pos_sample is not None:
    raw_pos_sample.filter(
        F.col("CLNT_NO").isin(trace_clients)
    ).select("CLNT_CRD_NO", "CLNT_NO").distinct().show(20, truncate=False)
else:
    print("POS data not available.")

print("Review above: all sources should resolve to the same CLNT_NO for each client.")
audit_results["B6_clnt_no_consistency"] = "INFO"


# ============================================================
# CELL 16: B7 — DEDUPLICATION AUDIT
# REUSES raw_card_sample and raw_pos_sample — no re-reading.
# ============================================================

print("=" * 60)
print("B7: DEDUPLICATION AUDIT")
print("=" * 60)
print("Finding clients with MULTIPLE success events in the same treatment window.")
print("Verifying only the FIRST (earliest) is kept in result_df.\n")

b7_pass = True

for mne, cfg in AUDIT_CAMPAIGNS.items():
    success_col = METRIC_TO_COLUMN[cfg["primary_metric"]]
    dt_col = f"FIRST_{success_col}_DT"

    success_rows = result_df.filter(
        (F.col("MNE") == mne) & (F.col("SUCCESS") == 1)
    )

    if success_rows.count() == 0:
        continue

    # With sliced data we can check more samples
    sample_success = success_rows.select(
        "CLNT_NO", "TACTIC_ID", "TREATMT_STRT_DT", "TREATMT_END_DT", dt_col
    ).limit(10).collect()

    if not sample_success:
        continue

    metric = cfg["primary_metric"]
    print(f"\n{mne} ({metric}) -- checking deduplication:")
    print("-" * 60)

    for sr in sample_success:
        clnt = str(sr.CLNT_NO)
        strt = sr.TREATMT_STRT_DT
        end = sr.TREATMT_END_DT
        pipeline_dt = sr[dt_col]

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
            print(f"  {mne} CLNT={clnt}: pipeline_dt={pipeline_dt} (raw check skipped -- data not available)")
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
            print(f"  {mne} CLNT={clnt}: {raw_count} raw event(s) {raw_dates}, pipeline_dt={pipeline_dt} [OK -- single event]")

audit_results["B7_deduplication"] = "PASS" if b7_pass else "FAIL"


# ============================================================
# CELL 17: B8 — SUMMARY
# ============================================================

print("=" * 60)
print("=" * 60)
print("AUDIT SUMMARY")
print("=" * 60)
print("=" * 60)

print(f"\nAudit scope: MNES={AUDIT_MNES}, YEARS={AUDIT_YEARS}, COHORTS={AUDIT_COHORTS}")
print(f"Result rows: {result_count:,}")

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


# ============================================================
# CELL 18: CLEANUP
# Unpersist everything.
# ============================================================

result_df.unpersist()
sample_clients.unpersist()
try:
    raw_card_sample.unpersist()
except:
    pass
try:
    raw_pos_sample.unpersist()
except:
    pass
print("All DataFrames unpersisted. Audit complete.")
