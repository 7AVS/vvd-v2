#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================================================================
# VVD v2 Analysis Pipeline
#
# Pipeline for VVD campaign measurement. Extracts experiment population,
# detects success outcomes, and runs campaign performance +
# overcontacting analysis.
#
# Campaigns: VCN, VDA, VDT, VUI, VUT, VAW
# No UCP enrichment -- demographics excluded (blocked on data engineering)
# Environment: Lumina (PySpark on YARN)
# ====================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from IPython.display import display, HTML
import json

# ============================================================
# CONFIGURATION — Edit these values before running
# ============================================================

# Campaign metadata
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
        "test_control": "95/5",
        "deployment": "Trigger"
    },
    "VAW": {
        "name": "VVD Add To Wallet",
        "success_type": "TOKENIZATION",
        "primary_metric": "wallet_provisioning",
        "test_control": "80/20",
        "deployment": "Trigger"
    }
}

# Metric → success column mapping for output
METRIC_TO_COLUMN = {
    "card_acquisition": "ACQUISITION_SUCCESS",
    "card_activation": "ACTIVATION_SUCCESS",
    "card_usage": "USAGE_SUCCESS",
    "wallet_provisioning": "PROVISIONING_SUCCESS"
}

# Source table paths (Hive parquet on HDFS)
TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
CARD_DATA_PATH = "/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT={year}*"
POS_TXN_PATH = "/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT={year}*"

# Years to include
YEARS = [2024, 2025, 2026]

# VVD MNE codes — NOTE: MNE is DERIVED from TACTIC_ID via substring(TACTIC_ID, 8, 3)
# It does NOT exist as a raw column in tactic_evnt_hist
VVD_MNES = list(CAMPAIGNS.keys())

# Test group codes
ACTION_GROUP = "TG4"
CONTROL_GROUP = "TG7"

# ====================================================================
# 1. Extract Experiment Population (M1)
#
# Read from tactic_evnt_hist, filter to VVD campaigns, extract client
# population with treatment dates and test groups.
# ====================================================================

# ============================================================
# M1: EXPERIMENT POPULATION
# Mirrors Vintage/Vvd vintage_engine.py v2.5 (lines 261-306)
#
# KEY: MNE does NOT exist as a raw column in tactic_evnt_hist.
#      It is DERIVED: MNE = substring(TACTIC_ID, 8, 3)
#      CLNT_NO is DERIVED: regexp_replace(trim(TACTIC_EVNT_ID), "^0+", "")
# ============================================================

# Read parquet — single read across year partitions (no unionByName needed)
tactic_paths = [TACTIC_EVNT_HIST_BASE + f"EVNT_STRT_DT={y}*" for y in YEARS]
raw_tactic = spark.read.option("basePath", TACTIC_EVNT_HIST_BASE).parquet(*tactic_paths)

# Derive MNE from TACTIC_ID and filter to VVD campaigns
# Derive CLNT_NO from TACTIC_EVNT_ID (strip leading zeros)
# Trim TST_GRP_CD and RPT_GRP_CD (whitespace in raw data)
tactic_df = (
    raw_tactic
    .filter(F.substring(F.col("TACTIC_ID"), 8, 3).isin(VVD_MNES))
    .withColumn("MNE", F.substring(F.col("TACTIC_ID"), 8, 3))
    .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("TACTIC_EVNT_ID")), "^0+", ""))
    .withColumn("TST_GRP_CD", F.trim(F.col("TST_GRP_CD")))
    .withColumn("RPT_GRP_CD", F.trim(F.col("RPT_GRP_CD")))
    .filter(F.col("TST_GRP_CD").isin([ACTION_GROUP, CONTROL_GROUP]))
    .withColumn("WINDOW_DAYS", F.datediff(F.col("TREATMT_END_DT"), F.col("TREATMT_STRT_DT")))
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .select(
        "CLNT_NO",
        "TACTIC_ID",
        "MNE",
        "TST_GRP_CD",
        "RPT_GRP_CD",
        "TREATMT_STRT_DT",
        "TREATMT_END_DT",
        "TREATMT_MN",
        "TACTIC_CELL_CD",
        "WINDOW_DAYS",
        "COHORT",
    )
    .distinct()
)

tactic_df.cache()

print(f"Experiment population loaded:")
print(f"  Total rows: {tactic_df.count():,}")
print(f"  Unique clients: {tactic_df.select('CLNT_NO').distinct().count():,}")
print(f"  Campaigns: {sorted([r.MNE for r in tactic_df.select('MNE').distinct().collect()])}")
print(f"  Date range: {tactic_df.agg(F.min('TREATMT_STRT_DT')).collect()[0][0]} to {tactic_df.agg(F.max('TREATMT_STRT_DT')).collect()[0][0]}")
print(f"  Test groups: {[r.TST_GRP_CD for r in tactic_df.select('TST_GRP_CD').distinct().collect()]}")

# ====================================================================
# 2. Extract Success Outcomes (M3)
#
# Four success metrics from their respective source tables. Each
# produces a DataFrame of (CLNT_NO, SUCCESS_DT).
# ====================================================================

# ============================================================
# M3a: CARD ACQUISITION & CARD ACTIVATION
# Source: DDWTA_VISA_DR_CRD (Hive parquet)
# ============================================================

card_paths = [CARD_DATA_PATH.format(year=y) for y in YEARS]
try:
    raw_card = spark.read.parquet(*card_paths)
except Exception as e:
    print(f"Warning: Could not read card data: {e}")
    raw_card = None

if raw_card is None:
    print("WARNING: Card data not found. card_acquisition and card_activation will be empty.")
    card_acquisition_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")
    card_activation_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")
else:
    # Base filter: active/approved VVD cards
    card_base = (
        raw_card
        .filter(F.col("STS_CD").isin(["06", "08"]))
        .filter(F.col("SRVC_ID") == 36)
        .filter(F.col("ISS_DT").isNotNull())
        .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("CLNT_NO")), "^0+", ""))
    )

    # card_acquisition: success date = ISS_DT (issue date)
    card_acquisition_df = (
        card_base
        .select(
            "CLNT_NO",
            F.col("ISS_DT").cast("date").alias("SUCCESS_DT")
        )
        .filter(F.col("SUCCESS_DT").isNotNull())
        .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
    )

    # card_activation: success date = ACTV_DT (activation date)
    card_activation_df = (
        card_base
        .filter(F.col("ACTV_DT").isNotNull())
        .select(
            "CLNT_NO",
            F.col("ACTV_DT").cast("date").alias("SUCCESS_DT")
        )
        .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
    )

    print(f"card_acquisition: {card_acquisition_df.count():,} events")
    print(f"card_activation: {card_activation_df.count():,} events")

# ============================================================
# M3b: CARD USAGE
# Source: DDWTA_T_PT_OF_SALE_TXN (Hive parquet)
# ============================================================

txn_paths = [POS_TXN_PATH.format(year=y) for y in YEARS]
try:
    raw_txn = spark.read.parquet(*txn_paths)
except Exception as e:
    print(f"Warning: Could not read POS data: {e}")
    raw_txn = None

if raw_txn is None:
    print("WARNING: POS transaction data not found. card_usage will be empty.")
    card_usage_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")
else:
    card_usage_df = (
        raw_txn
        .filter(F.col("SRVC_CD") == 36)
        .filter(
            ((F.col("TXN_TP") == 10) & (F.col("MSG_TP") == "0210")) |
            ((F.col("TXN_TP") == 13) & (F.col("MSG_TP") == "0210")) |
            ((F.col("TXN_TP") == 12) & (F.col("MSG_TP") == "0220"))
        )
        .filter(F.col("AMT1") > 0)
        .withColumn(
            "CLNT_NO",
            F.regexp_replace(F.substring(F.col("CLNT_CRD_NO"), 7, 9), "^0+", "")
        )
        .select(
            "CLNT_NO",
            F.col("TXN_DT").cast("date").alias("SUCCESS_DT")
        )
        .filter(F.col("SUCCESS_DT").isNotNull())
        .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
    )

    print(f"card_usage: {card_usage_df.count():,} events")

# ============================================================
# M3c: WALLET PROVISIONING
# Source: EDW (Teradata) — DDWV05.CLNT_CRD_POS_LOG + DL_DECMAN.TOKEN_LIST
# NOTE: This requires EDW cursor access. If not available, skip.
# ============================================================

try:
    # EDW query for wallet provisioning
    wallet_query = """
    SELECT
        CAST(SUBSTR(B.CLNT_CRD_NO, 7, 9) AS INTEGER) AS CLNT_NO_INT,
        B.TXN_DT AS SUCCESS_DT
    FROM DDWV05.CLNT_CRD_POS_LOG B
    JOIN DL_DECMAN.TOKEN_LIST C
        ON B.TOKN_REQSTR_ID = C.TOKEN_ID
    WHERE B.AMT1 = 0
        AND SUBSTR(B.CLNT_CRD_NO, 1, 5) = '45190'
        AND SUBSTR(B.VISA_DR_CRD_NO, 1, 5) = '45199'
        AND SUBSTR(B.TOKN_REQSTR_ID, 1, 1) > '0'
        AND B.POS_ENTR_MODE_CD_NON_EMV = '000'
        AND B.SRVC_CD = 36
        AND C.TOKEN_WALLET_IND = 'Y'
    """

    # Attempt EDW connection (environment-specific)
    # Option A: If EDW cursor is available
    # cursor = EDW.cursor()
    # cursor.execute(wallet_query)
    # results = cursor.fetchall()
    # wallet_provisioning_df = spark.createDataFrame(
    #     [(str(r[0]), r[1]) for r in results],
    #     ["CLNT_NO", "SUCCESS_DT"]
    # )

    # Option B: If EDW data is available as Spark table
    # wallet_provisioning_df = spark.sql(wallet_query_adapted)

    # Placeholder — uncomment the appropriate option above
    print("WARNING: wallet_provisioning requires EDW access. Using empty DataFrame.")
    print("Uncomment Option A (cursor) or Option B (Spark SQL) above based on your environment.")
    wallet_provisioning_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")

except Exception as e:
    print(f"WARNING: wallet_provisioning extraction failed: {e}")
    wallet_provisioning_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")

# ====================================================================
# 3. Detect Success (M6)
#
# Left join experiment population to success outcomes. For each
# deployment, determine if the client achieved success within the
# measurement window.
# ====================================================================

# ============================================================
# M6: SUCCESS DETECTION
# Left join tactic_df to each success outcome DataFrame
# Success = event occurred between TREATMT_STRT_DT and TREATMT_END_DT
# ============================================================

# Map metric name to its DataFrame
success_dfs = {
    "card_acquisition": card_acquisition_df,
    "card_activation": card_activation_df,
    "card_usage": card_usage_df,
    "wallet_provisioning": wallet_provisioning_df,
}

# Start with tactic_df, add success columns per campaign's primary metric
# Mirrors Vintage/Vvd vintage_engine.py detect_success() (lines 470-490)
result_df = tactic_df

for metric_name, success_col_name in METRIC_TO_COLUMN.items():
    sdf = success_dfs[metric_name]

    # Left join: client had a success event within treatment window
    # Use column renaming to avoid ambiguity (not alias prefixes)
    sdf_renamed = sdf.withColumnRenamed("CLNT_NO", "S_CLNT_NO").withColumnRenamed("SUCCESS_DT", "S_SUCCESS_DT")

    joined = (
        result_df
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

    # Drop the join key columns to keep schema clean
    result_df = joined.drop("S_CLNT_NO", "S_SUCCESS_DT")

# Assign each row its campaign-specific SUCCESS flag
result_df = result_df.withColumn(
    "SUCCESS",
    F.when(F.col("MNE").isin(["VCN", "VDA"]), F.col("ACQUISITION_SUCCESS"))
     .when(F.col("MNE") == "VDT", F.col("ACTIVATION_SUCCESS"))
     .when(F.col("MNE") == "VUI", F.col("USAGE_SUCCESS"))
     .when(F.col("MNE").isin(["VUT", "VAW"]), F.col("PROVISIONING_SUCCESS"))
     .otherwise(0)
)

result_df.cache()
tactic_df.unpersist()

print(f"Result DataFrame: {result_df.count():,} rows")
print(f"Columns: {result_df.columns}")
print(f"\nSuccess flag distribution:")
result_df.groupBy("MNE", "TST_GRP_CD").agg(
    F.count("*").alias("total"),
    F.sum("SUCCESS").alias("successes"),
    (F.sum("SUCCESS") / F.count("*") * 100).alias("success_rate_pct")
).orderBy("MNE", "TST_GRP_CD").show(50, truncate=False)

# ====================================================================
# 4. Phase 1: Universe Overview
#
# Baseline metrics: total volume, unique clients, test group splits,
# SRM checks, campaign volume distribution.
# ====================================================================

# ============================================================
# PHASE 1: UNIVERSE OVERVIEW
# ============================================================

from scipy import stats as scipy_stats

# --- 1.1 Overall Universe ---
total_rows = result_df.count()
unique_clients = result_df.select("CLNT_NO").distinct().count()
campaigns = sorted([str(r.MNE) for r in result_df.select("MNE").distinct().collect()])
date_range = result_df.agg(F.min("TREATMT_STRT_DT"), F.max("TREATMT_STRT_DT")).collect()[0]

print("=" * 60)
print("UNIVERSE OVERVIEW")
print("=" * 60)
print(f"Total deployments:     {total_rows:,}")
print(f"Unique clients:        {unique_clients:,}")
print(f"Avg deploys/client:    {total_rows / unique_clients:.2f}")
print(f"Campaigns:             {campaigns}")
print(f"Date range:            {date_range[0]} to {date_range[1]}")

# --- 1.2 Test Group Split + SRM ---
print(f"\n{'='*60}")
print("TEST GROUP SPLIT & SRM CHECK")
print("=" * 60)

tg_split = (
    result_df.groupBy("TST_GRP_CD")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("unique_clients")
    )
    .orderBy("TST_GRP_CD")
    .collect()
)

for row in tg_split:
    print(f"  {str(row.TST_GRP_CD)}: {int(row.deployments):,} deployments, {int(row.unique_clients):,} clients")

# SRM test per campaign
print(f"\n{'='*60}")
print("SRM CHECK BY CAMPAIGN")
print("=" * 60)
print(f"{'Campaign':<8} {'Action':>10} {'Control':>10} {'Ratio':>8} {'Expected':>10} {'Chi-sq':>10} {'p-value':>10} {'Status':>8}")
print("-" * 76)

srm_results = (
    result_df.groupBy("MNE", "TST_GRP_CD")
    .agg(F.countDistinct("CLNT_NO").alias("clients"))
    .collect()
)

# Pivot into per-campaign action/control counts
from collections import defaultdict
srm_data = defaultdict(dict)
for row in srm_results:
    srm_data[str(row.MNE)][str(row.TST_GRP_CD)] = int(row.clients)

for mne in sorted(srm_data.keys()):
    action = srm_data[mne].get(ACTION_GROUP, 0)
    control = srm_data[mne].get(CONTROL_GROUP, 0)
    total = action + control
    if total == 0:
        continue

    # Get expected ratio from config
    split_str = CAMPAIGNS[mne]["test_control"]
    expected_action_pct = int(split_str.split("/")[0]) / 100
    expected_action = total * expected_action_pct
    expected_control = total * (1 - expected_action_pct)

    chi_sq = ((action - expected_action) ** 2 / expected_action +
              (control - expected_control) ** 2 / expected_control)
    p_value = 1 - scipy_stats.chi2.cdf(chi_sq, df=1)

    status = "PASS" if p_value > 0.05 else "SRM !"
    actual_ratio = f"{action / total * 100:.1f}/{control / total * 100:.1f}"
    print(f"{mne:<8} {action:>10,} {control:>10,} {actual_ratio:>8} {split_str:>10} {chi_sq:>10.2f} {p_value:>10.4f} {status:>8}")

# --- 1.3 Campaign Volume Distribution ---
print(f"\n{'='*60}")
print("CAMPAIGN VOLUME DISTRIBUTION")
print("=" * 60)

campaign_vol = (
    result_df
    .filter(F.col("TST_GRP_CD") == ACTION_GROUP)
    .groupBy("MNE")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("unique_clients"),
    )
    .withColumn("avg_contacts", F.col("deployments") / F.col("unique_clients"))
    .orderBy(F.desc("deployments"))
    .collect()
)

total_action = sum(int(r.deployments) for r in campaign_vol)
print(f"{'Campaign':<8} {'Deployments':>12} {'% Total':>8} {'Clients':>10} {'Avg Contacts':>13}")
print("-" * 55)
for row in campaign_vol:
    deploys = int(row.deployments)
    pct = deploys / total_action * 100
    clients = int(row.unique_clients)
    avg = float(row.avg_contacts)
    print(f"{str(row.MNE):<8} {deploys:>12,} {pct:>7.1f}% {clients:>10,} {avg:>13.2f}")

# ====================================================================
# 5. Phase 2: Campaign Performance
#
# Success rates by campaign x test group, absolute lift, statistical
# significance.
# ====================================================================

# ============================================================
# PHASE 2: CAMPAIGN PERFORMANCE
# ============================================================
from scipy.stats import norm as scipy_norm

print("=" * 60)
print("CAMPAIGN PERFORMANCE — LIFT & SIGNIFICANCE")
print("=" * 60)

# By MNE x TST_GRP_CD x COHORT
perf = (
    result_df
    .groupBy("MNE", "TST_GRP_CD", "COHORT")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("deployments") * 100)
    .orderBy("MNE", "COHORT", "TST_GRP_CD")
    .collect()
)

# Pivot into action vs control per campaign x cohort
from collections import defaultdict
perf_data = defaultdict(lambda: defaultdict(dict))
for row in perf:
    key = (str(row.MNE), str(row.COHORT))
    perf_data[key][str(row.TST_GRP_CD)] = {
        "deployments": int(row.deployments),
        "clients": int(row.clients),
        "successes": int(row.successes),
        "rate": float(row.success_rate)
    }

print(f"{'Campaign':<8} {'Cohort':<8} {'Action Rate':>12} {'Control Rate':>13} {'Abs Lift':>9} {'p-value':>9} {'Sig':>6}")
print("-" * 70)

for (mne, cohort) in sorted(perf_data.keys()):
    action = perf_data[(mne, cohort)].get(ACTION_GROUP, {})
    control = perf_data[(mne, cohort)].get(CONTROL_GROUP, {})

    if not action or not control:
        continue

    a_rate = action["rate"]
    c_rate = control["rate"]
    lift = a_rate - c_rate

    # Z-test for proportions
    a_n = action["deployments"]
    c_n = control["deployments"]
    a_p = action["successes"] / a_n if a_n > 0 else 0
    c_p = control["successes"] / c_n if c_n > 0 else 0
    pooled = (action["successes"] + control["successes"]) / (a_n + c_n) if (a_n + c_n) > 0 else 0

    if pooled > 0 and pooled < 1:
        se = (pooled * (1 - pooled) * (1/a_n + 1/c_n)) ** 0.5
        z = (a_p - c_p) / se if se > 0 else 0
        p_val = 2 * (1 - scipy_norm.cdf(abs(z)))
    else:
        p_val = 1.0

    if p_val < 0.001:
        sig = "99.9%"
    elif p_val < 0.01:
        sig = "99%"
    elif p_val < 0.05:
        sig = "95%"
    else:
        sig = "No"

    print(f"{mne:<8} {cohort:<8} {a_rate:>11.2f}% {c_rate:>12.2f}% {lift:>+8.2f}% {p_val:>9.4f} {sig:>6}")

# --- Overall campaign summary (across all cohorts) ---
print(f"\n{'='*60}")
print("CAMPAIGN PERFORMANCE SUMMARY (ALL COHORTS)")
print("=" * 60)

overall_perf = (
    result_df
    .groupBy("MNE", "TST_GRP_CD")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("deployments") * 100)
    .orderBy("MNE", "TST_GRP_CD")
    .collect()
)

overall_data = defaultdict(dict)
for row in overall_perf:
    overall_data[str(row.MNE)][str(row.TST_GRP_CD)] = {
        "deployments": int(row.deployments),
        "clients": int(row.clients),
        "successes": int(row.successes),
        "rate": float(row.success_rate)
    }

print(f"{'Campaign':<8} {'Action Rate':>12} {'Control Rate':>13} {'Abs Lift':>9} {'Rel Lift':>9} {'Sig':>6} {'Incr. Clients':>14}")
print("-" * 75)

for mne in sorted(overall_data.keys()):
    action = overall_data[mne].get(ACTION_GROUP, {})
    control = overall_data[mne].get(CONTROL_GROUP, {})
    if not action or not control:
        continue

    a_rate = action["rate"]
    c_rate = control["rate"]
    lift = a_rate - c_rate
    rel_lift = ((a_rate - c_rate) / c_rate * 100) if c_rate > 0 else 0

    a_n = action["deployments"]
    c_n = control["deployments"]
    a_p = action["successes"] / a_n if a_n > 0 else 0
    c_p = control["successes"] / c_n if c_n > 0 else 0
    pooled = (action["successes"] + control["successes"]) / (a_n + c_n) if (a_n + c_n) > 0 else 0

    if pooled > 0 and pooled < 1:
        se = (pooled * (1 - pooled) * (1/a_n + 1/c_n)) ** 0.5
        z = (a_p - c_p) / se if se > 0 else 0
        p_val = 2 * (1 - scipy_norm.cdf(abs(z)))
    else:
        p_val = 1.0

    sig = "99.9%" if p_val < 0.001 else "99%" if p_val < 0.01 else "95%" if p_val < 0.05 else "No"

    # Incremental clients = lift * action_clients
    incr = int(lift / 100 * action["clients"])

    print(f"{mne:<8} {a_rate:>11.2f}% {c_rate:>12.2f}% {lift:>+8.2f}% {rel_lift:>+8.1f}% {sig:>6} {incr:>14,}")

# ====================================================================
# 6. Phase 3: Overcontacting & Orchestration Analysis
#
# Contact frequency distribution, diminishing returns, campaign gap
# analysis, campaign-to-campaign transitions.
# ====================================================================

# ============================================================
# PHASE 3a: CONTACT FREQUENCY DISTRIBUTION
# Action group only
# ============================================================

action_df = result_df.filter(F.col("TST_GRP_CD") == ACTION_GROUP)

# Contacts per client per campaign
client_contacts = (
    action_df
    .groupBy("CLNT_NO", "MNE")
    .agg(
        F.count("*").alias("contact_count"),
        F.max("SUCCESS").alias("any_success")
    )
)

# Overall contacts per client (across all campaigns)
client_total = (
    action_df
    .groupBy("CLNT_NO")
    .agg(
        F.count("*").alias("total_contacts"),
        F.countDistinct("MNE").alias("distinct_campaigns"),
        F.max("SUCCESS").alias("any_success")
    )
)

# Contact frequency buckets
client_buckets = (
    client_total
    .withColumn("bucket",
        F.when(F.col("total_contacts") == 1, "1")
         .when(F.col("total_contacts") == 2, "2")
         .when(F.col("total_contacts") == 3, "3")
         .when(F.col("total_contacts").between(4, 5), "4-5")
         .when(F.col("total_contacts").between(6, 10), "6-10")
         .otherwise("11+")
    )
)

print("=" * 60)
print("CONTACT FREQUENCY DISTRIBUTION (Action Group)")
print("=" * 60)

bucket_stats = (
    client_buckets
    .groupBy("bucket")
    .agg(
        F.count("*").alias("clients"),
        F.avg("total_contacts").alias("avg_contacts"),
        F.sum("any_success").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("clients") * 100)
    .orderBy("bucket")
    .collect()
)

total_clients_action = sum(int(r.clients) for r in bucket_stats)
print(f"{'Bucket':<8} {'Clients':>10} {'% Total':>8} {'Avg Contacts':>13} {'Success Rate':>13}")
print("-" * 55)
for row in bucket_stats:
    clients = int(row.clients)
    pct = clients / total_clients_action * 100
    print(f"{str(row.bucket):<8} {clients:>10,} {pct:>7.1f}% {float(row.avg_contacts):>13.1f} {float(row.success_rate):>12.2f}%")

# Per-campaign contact stats
print(f"\n{'='*60}")
print("CONTACTS PER CLIENT BY CAMPAIGN")
print("=" * 60)

campaign_contact_stats = (
    client_contacts
    .groupBy("MNE")
    .agg(
        F.count("*").alias("clients"),
        F.avg("contact_count").alias("avg_contacts"),
        F.max("contact_count").alias("max_contacts"),
        F.expr("percentile_approx(contact_count, 0.5)").alias("median_contacts"),
        F.expr("percentile_approx(contact_count, 0.9)").alias("p90_contacts"),
    )
    .orderBy(F.desc("avg_contacts"))
    .collect()
)

print(f"{'Campaign':<8} {'Clients':>10} {'Avg':>6} {'Median':>7} {'P90':>5} {'Max':>5}")
print("-" * 45)
for row in campaign_contact_stats:
    print(f"{str(row.MNE):<8} {int(row.clients):>10,} {float(row.avg_contacts):>6.1f} {int(row.median_contacts):>7} {int(row.p90_contacts):>5} {int(row.max_contacts):>5}")

# ============================================================
# PHASE 3b: DIMINISHING RETURNS
# Success rate by deployment number (contact sequence)
# ============================================================

# Add contact sequence per client per campaign
w_seq = Window.partitionBy("CLNT_NO", "MNE").orderBy("TREATMT_STRT_DT")
action_seq = action_df.withColumn("contact_seq", F.row_number().over(w_seq))

print("=" * 60)
print("DIMINISHING RETURNS — SUCCESS BY CONTACT SEQUENCE")
print("=" * 60)

# Overall (all campaigns)
seq_overall = (
    action_seq
    .filter(F.col("contact_seq") <= 10)
    .groupBy("contact_seq")
    .agg(
        F.count("*").alias("deployments"),
        F.sum("SUCCESS").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("deployments") * 100)
    .orderBy("contact_seq")
    .collect()
)

print(f"\nOverall (all campaigns):")
print(f"{'Contact #':<10} {'Deployments':>12} {'Successes':>10} {'Success Rate':>13} {'vs #1':>8}")
print("-" * 55)
first_rate = float(seq_overall[0].success_rate) if seq_overall else 0
for row in seq_overall:
    rate = float(row.success_rate)
    vs_first = ((rate - first_rate) / first_rate * 100) if first_rate > 0 else 0
    print(f"{int(row.contact_seq):<10} {int(row.deployments):>12,} {int(row.successes):>10,} {rate:>12.2f}% {vs_first:>+7.1f}%")

# Per campaign
for mne in sorted(CAMPAIGNS.keys()):
    seq_campaign = (
        action_seq
        .filter(F.col("MNE") == mne)
        .filter(F.col("contact_seq") <= 10)
        .groupBy("contact_seq")
        .agg(
            F.count("*").alias("deployments"),
            F.sum("SUCCESS").alias("successes"),
        )
        .withColumn("success_rate", F.col("successes") / F.col("deployments") * 100)
        .orderBy("contact_seq")
        .collect()
    )

    if not seq_campaign:
        continue

    print(f"\n{mne} ({CAMPAIGNS[mne]['name']}):")
    print(f"{'Contact #':<10} {'Deployments':>12} {'Successes':>10} {'Success Rate':>13}")
    print("-" * 48)
    for row in seq_campaign:
        print(f"{int(row.contact_seq):<10} {int(row.deployments):>12,} {int(row.successes):>10,} {float(row.success_rate):>12.2f}%")

# ============================================================
# PHASE 3c: CAMPAIGN GAP ANALYSIS
# How closely are campaigns deployed to the same client?
# ============================================================

print("=" * 60)
print("CAMPAIGN GAP ANALYSIS")
print("=" * 60)

# Self-join to find consecutive deployments per client
w_all = Window.partitionBy("CLNT_NO").orderBy("TREATMT_STRT_DT")
action_ordered = (
    action_df
    .withColumn("prev_start", F.lag("TREATMT_STRT_DT").over(w_all))
    .withColumn("prev_mne", F.lag("MNE").over(w_all))
    .filter(F.col("prev_start").isNotNull())
    .withColumn("gap_days", F.datediff("TREATMT_STRT_DT", "prev_start"))
)

# Gap distribution
gap_buckets = (
    action_ordered
    .withColumn("gap_bucket",
        F.when(F.col("gap_days") <= 0, "Same day")
         .when(F.col("gap_days").between(1, 7), "1-7 days")
         .when(F.col("gap_days").between(8, 15), "8-15 days")
         .when(F.col("gap_days").between(16, 25), "16-25 days")
         .when(F.col("gap_days").between(26, 30), "26-30 days")
         .when(F.col("gap_days").between(31, 60), "31-60 days")
         .when(F.col("gap_days").between(61, 90), "61-90 days")
         .otherwise("90+ days")
    )
    .groupBy("gap_bucket")
    .agg(
        F.count("*").alias("transitions"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.avg("gap_days").alias("avg_gap")
    )
    .orderBy("gap_bucket")
    .collect()
)

total_transitions = sum(int(r.transitions) for r in gap_buckets)
print(f"{'Gap Bucket':<12} {'Transitions':>12} {'% Total':>8} {'Clients':>10} {'Avg Gap':>8}")
print("-" * 55)
for row in gap_buckets:
    t = int(row.transitions)
    pct = t / total_transitions * 100
    print(f"{str(row.gap_bucket):<12} {t:>12,} {pct:>7.1f}% {int(row.clients):>10,} {float(row.avg_gap):>7.1f}d")

# Overlap flag: transitions with gap < 25 days
overlap_count = sum(int(r.transitions) for r in gap_buckets if "Same" in str(r.gap_bucket) or any(x in str(r.gap_bucket) for x in ["1-7", "8-15", "16-25"]))
overlap_clients = action_ordered.filter(F.col("gap_days") < 25).select("CLNT_NO").distinct().count()
print(f"\nOverlapping deployments (gap < 25 days): {overlap_count:,} transitions, {overlap_clients:,} clients")

# ============================================================
# PHASE 3d: CAMPAIGN TRANSITION MATRIX
# What campaign follows what? (same client, ordered by date)
# ============================================================

print("=" * 60)
print("CAMPAIGN TRANSITION MATRIX")
print("=" * 60)

transition_matrix = (
    action_ordered
    .groupBy("prev_mne", "MNE")
    .agg(
        F.count("*").alias("transitions"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.avg("gap_days").alias("avg_gap_days")
    )
    .orderBy(F.desc("transitions"))
    .collect()
)

print(f"{'From':<8} {'To':<8} {'Transitions':>12} {'Clients':>10} {'Avg Gap':>9}")
print("-" * 50)
for row in transition_matrix[:20]:  # Top 20
    print(f"{str(row.prev_mne):<8} {str(row.MNE):<8} {int(row.transitions):>12,} {int(row.clients):>10,} {float(row.avg_gap_days):>8.1f}d")

# Self-loop analysis
print(f"\nSELF-LOOP ANALYSIS (same campaign -> same campaign):")
print("-" * 50)
for row in transition_matrix:
    if str(row.prev_mne) == str(row.MNE):
        print(f"  {str(row.MNE)}->{str(row.MNE)}: {int(row.transitions):,} transitions, {int(row.clients):,} clients, avg gap {float(row.avg_gap_days):.1f} days")

# ====================================================================
# 7. Data Export
#
# Save results for further analysis or presentation.
# ====================================================================

# ============================================================
# DATA EXPORT
# Save key DataFrames for downstream use
# ============================================================

# Option 1: Save to HDFS (uncomment as needed)
# result_df.write.mode("overwrite").parquet("/user/{username}/vvd_v2/result_df.parquet")

# Option 2: Save summary to CSV for download
# Small summary tables can be converted to pandas for CSV export
import pandas as pd

# Campaign performance summary
perf_summary = (
    result_df
    .groupBy("MNE", "TST_GRP_CD", "COHORT")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
        (F.sum("SUCCESS") / F.count("*") * 100).alias("success_rate"),
    )
    .orderBy("MNE", "COHORT", "TST_GRP_CD")
    .toPandas()
)

print("Campaign performance summary (first 20 rows):")
print(perf_summary.head(20).to_string(index=False))

# Save to CSV (uncomment)
# perf_summary.to_csv("/tmp/vvd_v2_performance_summary.csv", index=False)

print("\nPipeline complete. Review results above and export as needed.")

# ============================================================
# CLEANUP
# ============================================================
result_df.unpersist()
print("Cache cleared. Pipeline complete.")
