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
# Environment: Lumina (PySpark on YARN, Jupyter notebook)
#
# Structure: Each section below is one notebook cell.
# Cells 1-4: Data pipeline (run once per session)
# Cell 4b: Email engagement (optional, decoupled from critical path)
# Cells 5-9: Analysis (re-run as many times as you want)
# Cell 10: Vintage curve construction
# Cell 11: Cleanup
# ====================================================================


# ============================================================
# CELL 1: IMPORTS + CONFIGURATION
# Paste this cell first. Sets up all constants.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import StorageLevel
from collections import defaultdict

try:
    from IPython.display import display, HTML
except ImportError:
    pass

try:
    from scipy import stats as scipy_stats
    from scipy.stats import norm as scipy_norm
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False
    print("WARNING: scipy not available. SRM and significance tests will use approximation.")

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

# Reverse mapping: metric -> list of campaigns that use it
# Drives campaign-scoped joins in M6 (only shuffle relevant campaigns per metric)
METRIC_TO_CAMPAIGNS = defaultdict(list)
for _mne, _cfg in CAMPAIGNS.items():
    METRIC_TO_CAMPAIGNS[_cfg["primary_metric"]].append(_mne)
    if "secondary_metric" in _cfg:
        METRIC_TO_CAMPAIGNS[_cfg["secondary_metric"]].append(_mne)
METRIC_TO_CAMPAIGNS = dict(METRIC_TO_CAMPAIGNS)
# Result: {'card_acquisition': ['VCN','VDA'], 'card_activation': ['VDT'],
#           'card_usage': ['VUI','VUT','VAW'], 'wallet_provisioning': ['VUT','VAW']}

# Source table paths (Hive parquet on HDFS)
TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
CARD_DATA_PATH = "/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT={year}*"
POS_TXN_PATH = "/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT={year}*"

YEARS = [2024, 2025, 2026]

# VVD MNE codes -- DERIVED from TACTIC_ID via substring(TACTIC_ID, 8, 3)
# Does NOT exist as a raw column in tactic_evnt_hist
VVD_MNES = list(CAMPAIGNS.keys())

ACTION_GROUP = "TG4"
CONTROL_GROUP = "TG7"

# Exclude incomplete months from analysis
DATA_END_DATE = "2026-03-01"

print("Configuration loaded.")
print(f"Campaigns: {VVD_MNES}")
print(f"Years: {YEARS}")
print(f"Metric mapping: {METRIC_TO_CAMPAIGNS}")


# ============================================================
# CELL 2: M1 — EXPERIMENT POPULATION
# Reads tactic_evnt_hist, derives MNE + CLNT_NO, persists.
# Run once per session.
# ============================================================

# Mirrors Vintage/Vvd vintage_engine.py v2.5 (lines 261-306)
# KEY: MNE = substring(TACTIC_ID, 8, 3), CLNT_NO = regexp_replace(trim(TACTIC_EVNT_ID), "^0+", "")
tactic_paths = [TACTIC_EVNT_HIST_BASE + f"EVNT_STRT_DT={y}*" for y in YEARS]
raw_tactic = spark.read.option("basePath", TACTIC_EVNT_HIST_BASE).parquet(*tactic_paths)

tactic_df = (
    raw_tactic
    .filter(F.substring(F.col("TACTIC_ID"), 8, 3).isin(VVD_MNES))
    .withColumn("MNE", F.substring(F.col("TACTIC_ID"), 8, 3))
    .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("TACTIC_EVNT_ID")), "^0+", ""))
    .withColumn("TST_GRP_CD", F.trim(F.col("TST_GRP_CD")))
    .withColumn("RPT_GRP_CD", F.trim(F.col("RPT_GRP_CD")))
    .filter(F.col("TST_GRP_CD").isin([ACTION_GROUP, CONTROL_GROUP]))
    .filter(F.col("TREATMT_STRT_DT") < DATA_END_DATE)
    .withColumn("WINDOW_DAYS", F.datediff(F.col("TREATMT_END_DT"), F.col("TREATMT_STRT_DT")))
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .select(
        "CLNT_NO", "TACTIC_ID", "MNE", "TST_GRP_CD", "RPT_GRP_CD",
        "TREATMT_STRT_DT", "TREATMT_END_DT", "TREATMT_MN",
        "TACTIC_CELL_CD", "WINDOW_DAYS", "COHORT",
    )
    .distinct()
)

tactic_df.persist(StorageLevel.MEMORY_AND_DISK)

print(f"M1: Experiment population loaded:")
print(f"  Total rows: {tactic_df.count():,}")
print(f"  Unique clients: {tactic_df.select('CLNT_NO').distinct().count():,}")
print(f"  Campaigns: {sorted([r.MNE for r in tactic_df.select('MNE').distinct().collect()])}")
print(f"  Date range: {tactic_df.agg(F.min('TREATMT_STRT_DT')).collect()[0][0]} to {tactic_df.agg(F.max('TREATMT_STRT_DT')).collect()[0][0]}")
print(f"  Test groups: {[r.TST_GRP_CD for r in tactic_df.select('TST_GRP_CD').distinct().collect()]}")


# ============================================================
# CELL 3: M3 — SUCCESS OUTCOMES + PRE-FILTER
# Extracts card, usage, wallet data. Pre-filters to experiment
# clients only (left_semi join) before M6.
# Run once per session.
# ============================================================

# --- M3a: Card acquisition + activation ---
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
    card_base = (
        raw_card
        .filter(F.col("STS_CD").isin(["06", "08"]))
        .filter(F.col("SRVC_ID") == 36)
        .filter(F.col("ISS_DT").isNotNull())
        .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("CLNT_NO")), "^0+", ""))
    )

    card_acquisition_df = (
        card_base
        .select("CLNT_NO", F.col("ISS_DT").cast("date").alias("SUCCESS_DT"))
        .filter(F.col("SUCCESS_DT").isNotNull())
        .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
    )

    card_activation_df = (
        card_base
        .filter(F.col("ACTV_DT").isNotNull())
        .select("CLNT_NO", F.col("ACTV_DT").cast("date").alias("SUCCESS_DT"))
        .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
    )

    print(f"M3a: card_acquisition: {card_acquisition_df.count():,} events")
    print(f"M3a: card_activation: {card_activation_df.count():,} events")

# --- M3b: Card usage ---
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
        .withColumn("CLNT_NO", F.regexp_replace(F.substring(F.col("CLNT_CRD_NO"), 7, 9), "^0+", ""))
        .select("CLNT_NO", F.col("TXN_DT").cast("date").alias("SUCCESS_DT"))
        .filter(F.col("SUCCESS_DT").isNotNull())
        .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
    )

    print(f"M3b: card_usage: {card_usage_df.count():,} events")

# --- M3c: Wallet provisioning (EDW) ---
# Mirrors Vintage/Vvd vintage_engine.py load_token_from_edw() (v2.5, lines 392-417)
min_year = min(YEARS)
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
    # CLNT_NO comes back as INTEGER from Teradata — convert to STRING
    wallet_provisioning_df = spark.createDataFrame(
        [(str(int(r[0])), r[1]) for r in results],
        "CLNT_NO STRING, SUCCESS_DT DATE"
    )
    print(f"M3c: wallet_provisioning: {wallet_provisioning_df.count():,} events")
except NameError:
    print("WARNING: EDW cursor not available. wallet_provisioning will be empty.")
    wallet_provisioning_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")
except Exception as e:
    print(f"WARNING: wallet_provisioning extraction failed: {e}")
    wallet_provisioning_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")

# --- Pre-filter success DFs to experiment participants only ---
# Eliminates non-experiment rows BEFORE the M6 shuffle.
# Critical for POS transactions (card_usage) — tens of millions of rows down to ~2-3M.
experiment_clients = tactic_df.select("CLNT_NO").distinct()
card_acquisition_df = card_acquisition_df.join(experiment_clients, "CLNT_NO", "left_semi")
card_activation_df = card_activation_df.join(experiment_clients, "CLNT_NO", "left_semi")
card_usage_df = card_usage_df.join(experiment_clients, "CLNT_NO", "left_semi")
wallet_provisioning_df = wallet_provisioning_df.join(experiment_clients, "CLNT_NO", "left_semi")
print(f"\nPre-filtered success DFs to {experiment_clients.count():,} experiment clients")

success_dfs = {
    "card_acquisition": card_acquisition_df,
    "card_activation": card_activation_df,
    "card_usage": card_usage_df,
    "wallet_provisioning": wallet_provisioning_df,
}


# ============================================================
# CELL 4: M6 — SUCCESS DETECTION
# Campaign-scoped left joins. Persists result_df, unpersists tactic_df.
# Run once per session. After this, result_df is ready for analysis.
# ============================================================

# For each metric, only join against campaigns that use it (driven by METRIC_TO_CAMPAIGNS).
# Campaigns that don't use a given metric skip the shuffle entirely.
result_df = tactic_df

for metric_name, success_col_name in METRIC_TO_COLUMN.items():
    sdf = success_dfs[metric_name]
    relevant_mnes = METRIC_TO_CAMPAIGNS.get(metric_name, [])

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
for mne, cfg in CAMPAIGNS.items():
    col_name = METRIC_TO_COLUMN[cfg["primary_metric"]]
    success_expr = F.when(F.col("MNE") == mne, F.col(col_name)).otherwise(success_expr)
result_df = result_df.withColumn("SUCCESS", success_expr)

# Persist result_df for analysis reuse, release tactic_df
result_df.persist(StorageLevel.MEMORY_AND_DISK)
result_count = result_df.count()  # Force materialization
tactic_df.unpersist()

print(f"M6: Result DataFrame: {result_count:,} rows")
print(f"Columns: {result_df.columns}")
print(f"\nSuccess flag distribution:")
result_df.groupBy("MNE", "TST_GRP_CD").agg(
    F.count("*").alias("total"),
    F.sum("SUCCESS").alias("successes"),
    (F.sum("SUCCESS") / F.count("*") * 100).alias("success_rate_pct")
).orderBy("MNE", "TST_GRP_CD").show(50, truncate=False)

print("Data pipeline complete. result_df is persisted.")
print("Re-run cells 5-9 below as many times as needed.")


# ============================================================
# CELL 4b: EMAIL ENGAGEMENT (OPTIONAL)
# Decoupled from critical path — runs AFTER result_df is persisted.
# Loads send/open/click/unsub from EDW feedback tables, joins to result_df.
# Skip this cell if EDW is slow or email data not needed yet.
# ============================================================

# Derive email RPT_GRP_CDs dynamically — contains("EM") on ~30 distinct rows, not 38M
channel_map = (
    result_df.select("RPT_GRP_CD", "TACTIC_CELL_CD").distinct()
    .filter(F.col("TACTIC_CELL_CD").contains("EM"))
    .select("RPT_GRP_CD").distinct().collect()
)
EMAIL_RPT_GRPS = sorted([str(r.RPT_GRP_CD) for r in channel_map])
print(f"Email RPT_GRP_CDs: {EMAIL_RPT_GRPS}")
print("Loading email engagement from EDW (per-RPT_GRP)...")

EMAIL_SCHEMA = (
    "CLNT_NO STRING, TREATMENT_ID STRING, "
    "EMAIL_SENT INT, EMAIL_OPENED INT, EMAIL_CLICKED INT, EMAIL_UNSUBSCRIBED INT, "
    "EMAIL_SENT_DT DATE, EMAIL_OPENED_DT DATE, EMAIL_CLICKED_DT DATE, EMAIL_UNSUBSCRIBED_DT DATE"
)

email_engagement_df = spark.createDataFrame([], EMAIL_SCHEMA)

for rpt_grp in EMAIL_RPT_GRPS:
    tactic_ids = [
        str(r.TACTIC_ID) for r in
        result_df.filter(F.col("RPT_GRP_CD") == rpt_grp)
        .select("TACTIC_ID").distinct().collect()
    ]
    if not tactic_ids:
        continue

    tactic_id_list = "','".join(tactic_ids)
    email_query = f"""
SELECT
    CAST(FEEDBACK_MASTER.CLNT_NO AS VARCHAR(20)) AS CLNT_NO,
    FEEDBACK_MASTER.TREATMENT_ID,
    MAX(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS EMAIL_SENT,
    MAX(CASE WHEN disposition_cd = 2 THEN 1 ELSE 0 END) AS EMAIL_OPENED,
    MAX(CASE WHEN disposition_cd = 3 THEN 1 ELSE 0 END) AS EMAIL_CLICKED,
    MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS EMAIL_UNSUBSCRIBED,
    MAX(CASE WHEN disposition_cd = 1 THEN CAST(disposition_dt_tm AS DATE) END) AS EMAIL_SENT_DT,
    MAX(CASE WHEN disposition_cd = 2 THEN CAST(disposition_dt_tm AS DATE) END) AS EMAIL_OPENED_DT,
    MAX(CASE WHEN disposition_cd = 3 THEN CAST(disposition_dt_tm AS DATE) END) AS EMAIL_CLICKED_DT,
    MAX(CASE WHEN disposition_cd = 4 THEN CAST(disposition_dt_tm AS DATE) END) AS EMAIL_UNSUBSCRIBED_DT
FROM DTZV01.VENDOR_FEEDBACK_MASTER FEEDBACK_MASTER
INNER JOIN DTZV01.VENDOR_FEEDBACK_EVENT FEEDBACK_EVENT
    ON FEEDBACK_MASTER.consumer_id_hashed = FEEDBACK_EVENT.consumer_id_hashed
    AND FEEDBACK_MASTER.TREATMENT_ID = FEEDBACK_EVENT.TREATMENT_ID
WHERE FEEDBACK_MASTER.TREATMENT_ID IN ('{tactic_id_list}')
GROUP BY FEEDBACK_MASTER.CLNT_NO, FEEDBACK_MASTER.TREATMENT_ID
"""
    try:
        print(f"  {rpt_grp}: {len(tactic_ids)} tactic IDs...", end=" ")
        cursor = EDW.cursor()
        cursor.execute(email_query)
        email_results = cursor.fetchall()
        cursor.close()
        if email_results:
            grp_df = spark.createDataFrame(
                [
                    (str(r[0]).strip().lstrip('0'), str(r[1]),
                     int(r[2]), int(r[3]), int(r[4]), int(r[5]),
                     r[6], r[7], r[8], r[9])
                    for r in email_results
                ],
                EMAIL_SCHEMA
            )
            print(f"{grp_df.count():,} rows")
            email_engagement_df = email_engagement_df.unionByName(grp_df)
        else:
            print("0 rows")
    except NameError:
        print("EDW cursor not available, skipping.")
        break
    except Exception as e:
        print(f"failed: {e}")

print(f"Email engagement total: {email_engagement_df.count():,} rows")

# Join email to persisted result_df, re-persist with email columns
email_renamed = (
    email_engagement_df
    .withColumnRenamed("CLNT_NO", "E_CLNT_NO")
    .withColumnRenamed("TREATMENT_ID", "E_TREATMENT_ID")
)
result_df_with_email = (
    result_df
    .join(
        email_renamed,
        (F.col("CLNT_NO") == F.col("E_CLNT_NO")) &
        (F.col("TACTIC_ID") == F.col("E_TREATMENT_ID")),
        "left"
    )
    .withColumn("EMAIL_SENT", F.coalesce(F.col("EMAIL_SENT"), F.lit(0)))
    .withColumn("EMAIL_OPENED", F.coalesce(F.col("EMAIL_OPENED"), F.lit(0)))
    .withColumn("EMAIL_CLICKED", F.coalesce(F.col("EMAIL_CLICKED"), F.lit(0)))
    .withColumn("EMAIL_UNSUBSCRIBED", F.coalesce(F.col("EMAIL_UNSUBSCRIBED"), F.lit(0)))
    .drop("E_CLNT_NO", "E_TREATMENT_ID")
)

result_df.unpersist()
result_df = result_df_with_email
result_df.persist(StorageLevel.MEMORY_AND_DISK)
print(f"result_df re-persisted with email columns: {result_df.count():,} rows")


# ============================================================
# CELL 5: PHASE 1 — UNIVERSE OVERVIEW
# Re-runnable. Uses persisted result_df.
# ============================================================

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

srm_data = defaultdict(dict)
for row in srm_results:
    srm_data[str(row.MNE)][str(row.TST_GRP_CD)] = int(row.clients)

for mne in sorted(srm_data.keys()):
    action = srm_data[mne].get(ACTION_GROUP, 0)
    control = srm_data[mne].get(CONTROL_GROUP, 0)
    total = action + control
    if total == 0:
        continue

    split_str = CAMPAIGNS[mne]["test_control"]
    expected_action_pct = int(split_str.split("/")[0]) / 100
    expected_action = total * expected_action_pct
    expected_control = total * (1 - expected_action_pct)

    chi_sq = ((action - expected_action) ** 2 / expected_action +
              (control - expected_control) ** 2 / expected_control)
    if HAS_SCIPY:
        p_value = 1 - scipy_stats.chi2.cdf(chi_sq, df=1)
    else:
        import math
        p_value = math.exp(-chi_sq / 2) if chi_sq < 20 else 0.0

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


# ============================================================
# CELL 6: PHASE 2 — CAMPAIGN PERFORMANCE
# Re-runnable. Uses persisted result_df.
# ============================================================

print("=" * 60)
print("CAMPAIGN PERFORMANCE -- LIFT & SIGNIFICANCE")
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

    a_n = action["deployments"]
    c_n = control["deployments"]
    a_p = action["successes"] / a_n if a_n > 0 else 0
    c_p = control["successes"] / c_n if c_n > 0 else 0
    pooled = (action["successes"] + control["successes"]) / (a_n + c_n) if (a_n + c_n) > 0 else 0

    if pooled > 0 and pooled < 1:
        se = (pooled * (1 - pooled) * (1/a_n + 1/c_n)) ** 0.5
        z = (a_p - c_p) / se if se > 0 else 0
        if HAS_SCIPY:
            p_val = 2 * (1 - scipy_norm.cdf(abs(z)))
        else:
            import math
            p_val = 2 * math.exp(-0.5 * z * z) / (1 + 0.3275911 * abs(z)) if abs(z) < 10 else 0.0
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
        if HAS_SCIPY:
            p_val = 2 * (1 - scipy_norm.cdf(abs(z)))
        else:
            import math
            p_val = 2 * math.exp(-0.5 * z * z) / (1 + 0.3275911 * abs(z)) if abs(z) < 10 else 0.0
    else:
        p_val = 1.0

    sig = "99.9%" if p_val < 0.001 else "99%" if p_val < 0.01 else "95%" if p_val < 0.05 else "No"

    incr = int(lift / 100 * action["clients"])

    print(f"{mne:<8} {a_rate:>11.2f}% {c_rate:>12.2f}% {lift:>+8.2f}% {rel_lift:>+8.1f}% {sig:>6} {incr:>14,}")


# ============================================================
# CELL 5.5: MEGA-OUTPUT — CONSOLIDATED CAMPAIGN METRICS
# Rows: MNE × COHORT × RPT_GRP_CD (action detail) + ALL rollups.
# Includes channel, email metrics (correct denominator), NIBT.
# Exports as CSV — the auditable artifact for slides.
# Re-runnable. Uses persisted result_df.
# ============================================================

import pandas as pd
import base64
import math

try:
    from IPython.display import display, HTML
except ImportError:
    pass

def download_csv(data, filename="results.csv"):
    csv_data = data.to_csv(index=False)
    size_mb = len(csv_data.encode('utf-8')) / (1024 * 1024)
    if size_mb > 50:
        print(f"Data too large ({size_mb:.1f} MB). Filter before exporting.")
        return
    b64 = base64.b64encode(csv_data.encode()).decode()
    link = (
        f'<a download="{filename}" href="data:text/csv;base64,{b64}" '
        f'style="padding:6px 12px; background:#2196F3; color:white; '
        f'text-decoration:none; border-radius:3px; margin:2px; display:inline-block;">'
        f'Download {filename}</a>'
    )
    display(HTML(f'<div style="margin:5px 0;">{link} <span style="color:#666;">({size_mb:.2f} MB)</span></div>'))

NIBT_PER_CLIENT = 78.21
# NIBT only applies where $78.21 "new client onboarding" model fits
NIBT_ELIGIBLE = ["VCN", "VDA"]       # Acquisition = new client. Clean fit.
NIBT_ARGUABLE = ["VDT"]              # Activation = existing client acts. Arguable.
# VUI (usage nudge), VUT/VAW (provisioning) = NOT new client. NIBT does not apply.

def compute_sig(a_successes, a_n, c_successes, c_n):
    if a_n == 0 or c_n == 0:
        return 1.0
    a_p = a_successes / a_n
    c_p = c_successes / c_n
    pooled = (a_successes + c_successes) / (a_n + c_n)
    if pooled <= 0 or pooled >= 1:
        return 1.0
    se = (pooled * (1 - pooled) * (1/a_n + 1/c_n)) ** 0.5
    if se == 0:
        return 1.0
    z = (a_p - c_p) / se
    if HAS_SCIPY:
        return float(2 * (1 - scipy_norm.cdf(abs(z))))
    else:
        return float(2 * math.exp(-0.5 * z * z) / (1 + 0.3275911 * abs(z))) if abs(z) < 10 else 0.0

# --- Step 1: Action group by MNE × COHORT × RPT_GRP_CD ---
action_only = result_df.filter(F.col("TST_GRP_CD") == ACTION_GROUP)

action_detail_raw = (
    action_only
    .groupBy("MNE", "COHORT", "RPT_GRP_CD")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
        # Dominant channel for this RPT_GRP_CD
        F.first("TACTIC_CELL_CD").alias("TACTIC_CELL_CD"),
    )
    .collect()
)

# Also get per-cohort action aggregates (RPT_GRP_CD = ALL)
action_cohort_raw = (
    action_only
    .groupBy("MNE", "COHORT")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
    )
    .collect()
)

# Overall action aggregates (COHORT = ALL, RPT_GRP_CD = ALL)
action_overall_raw = (
    action_only
    .groupBy("MNE")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
    )
    .collect()
)

# --- Step 2: Control group by MNE × COHORT (no RPT_GRP_CD breakdown) ---
control_only = result_df.filter(F.col("TST_GRP_CD") == CONTROL_GROUP)

control_cohort = {}
for row in control_only.groupBy("MNE", "COHORT").agg(
    F.count("*").alias("deployments"),
    F.countDistinct("CLNT_NO").alias("clients"),
    F.sum("SUCCESS").alias("successes"),
).collect():
    control_cohort[(str(row.MNE), str(row.COHORT))] = {
        "deployments": int(row.deployments),
        "clients": int(row.clients),
        "successes": int(row.successes),
    }

control_overall = {}
for row in control_only.groupBy("MNE").agg(
    F.count("*").alias("deployments"),
    F.countDistinct("CLNT_NO").alias("clients"),
    F.sum("SUCCESS").alias("successes"),
).collect():
    control_overall[str(row.MNE)] = {
        "deployments": int(row.deployments),
        "clients": int(row.clients),
        "successes": int(row.successes),
    }

# --- Step 3: Contact stats (MNE-level, action group only) ---
contact_stats_raw = (
    action_only
    .groupBy("CLNT_NO", "MNE")
    .agg(F.count("*").alias("contact_count"))
    .groupBy("MNE")
    .agg(
        F.round(F.avg("contact_count"), 2).alias("avg_contacts"),
        F.expr("percentile_approx(contact_count, 0.5)").alias("median_contacts"),
        F.expr("percentile_approx(contact_count, 0.9)").alias("p90_contacts"),
        F.max("contact_count").alias("max_contacts"),
    )
    .collect()
)
contact_stats = {}
for row in contact_stats_raw:
    contact_stats[str(row.MNE)] = {
        "avg_contacts": float(row.avg_contacts),
        "median_contacts": int(row.median_contacts),
        "p90_contacts": int(row.p90_contacts),
        "max_contacts": int(row.max_contacts),
    }

# --- Step 4: Email metrics — denominator = email-channel deployments only ---
email_cols_exist = "EMAIL_SENT_DT" in [f.name for f in result_df.schema.fields]

email_stats = {}
if email_cols_exist:
    # Only count email metrics for deployments in email channels
    email_action = action_only.filter(F.col("TACTIC_CELL_CD").contains("EM"))

    # Per RPT_GRP_CD
    for row in email_action.groupBy("MNE", "COHORT", "RPT_GRP_CD").agg(
        F.count("*").alias("em_deploys"),
        F.sum(F.when(F.col("EMAIL_SENT_DT").isNotNull(), 1).otherwise(0)).alias("em_sent"),
        F.sum(F.when(F.col("EMAIL_OPENED_DT").isNotNull(), 1).otherwise(0)).alias("em_opened"),
        F.sum(F.when(F.col("EMAIL_CLICKED_DT").isNotNull(), 1).otherwise(0)).alias("em_clicked"),
        F.sum(F.when(F.col("EMAIL_UNSUBSCRIBED_DT").isNotNull(), 1).otherwise(0)).alias("em_unsub"),
    ).collect():
        sent = int(row.em_sent)
        email_stats[(str(row.MNE), str(row.COHORT), str(row.RPT_GRP_CD))] = {
            "email_deploys": int(row.em_deploys),
            "email_sent": sent,
            "email_open_rate": round(int(row.em_opened) / sent * 100, 2) if sent > 0 else None,
            "email_click_rate": round(int(row.em_clicked) / sent * 100, 2) if sent > 0 else None,
            "email_unsub_rate": round(int(row.em_unsub) / sent * 100, 2) if sent > 0 else None,
        }

    # Per MNE × COHORT (ALL RPT_GRP_CD)
    for row in email_action.groupBy("MNE", "COHORT").agg(
        F.count("*").alias("em_deploys"),
        F.sum(F.when(F.col("EMAIL_SENT_DT").isNotNull(), 1).otherwise(0)).alias("em_sent"),
        F.sum(F.when(F.col("EMAIL_OPENED_DT").isNotNull(), 1).otherwise(0)).alias("em_opened"),
        F.sum(F.when(F.col("EMAIL_CLICKED_DT").isNotNull(), 1).otherwise(0)).alias("em_clicked"),
        F.sum(F.when(F.col("EMAIL_UNSUBSCRIBED_DT").isNotNull(), 1).otherwise(0)).alias("em_unsub"),
    ).collect():
        sent = int(row.em_sent)
        email_stats[(str(row.MNE), str(row.COHORT), "ALL")] = {
            "email_deploys": int(row.em_deploys),
            "email_sent": sent,
            "email_open_rate": round(int(row.em_opened) / sent * 100, 2) if sent > 0 else None,
            "email_click_rate": round(int(row.em_clicked) / sent * 100, 2) if sent > 0 else None,
            "email_unsub_rate": round(int(row.em_unsub) / sent * 100, 2) if sent > 0 else None,
        }

    # Per MNE overall (ALL COHORT, ALL RPT_GRP_CD)
    for row in email_action.groupBy("MNE").agg(
        F.count("*").alias("em_deploys"),
        F.sum(F.when(F.col("EMAIL_SENT_DT").isNotNull(), 1).otherwise(0)).alias("em_sent"),
        F.sum(F.when(F.col("EMAIL_OPENED_DT").isNotNull(), 1).otherwise(0)).alias("em_opened"),
        F.sum(F.when(F.col("EMAIL_CLICKED_DT").isNotNull(), 1).otherwise(0)).alias("em_clicked"),
        F.sum(F.when(F.col("EMAIL_UNSUBSCRIBED_DT").isNotNull(), 1).otherwise(0)).alias("em_unsub"),
    ).collect():
        sent = int(row.em_sent)
        email_stats[(str(row.MNE), "ALL", "ALL")] = {
            "email_deploys": int(row.em_deploys),
            "email_sent": sent,
            "email_open_rate": round(int(row.em_opened) / sent * 100, 2) if sent > 0 else None,
            "email_click_rate": round(int(row.em_clicked) / sent * 100, 2) if sent > 0 else None,
            "email_unsub_rate": round(int(row.em_unsub) / sent * 100, 2) if sent > 0 else None,
        }

# --- Step 5: Assemble rows ---
def build_row(mne, cohort, rpt_grp, channel, a, c, cs, em):
    a_rate = a["successes"] / a["deployments"] * 100 if a["deployments"] > 0 else 0
    c_rate = c["successes"] / c["deployments"] * 100 if c["deployments"] > 0 else 0
    abs_lift = a_rate - c_rate
    rel_lift = ((a_rate - c_rate) / c_rate * 100) if c_rate > 0 else 0
    p_val = compute_sig(a["successes"], a["deployments"], c["successes"], c["deployments"])
    sig = "99.9%" if p_val < 0.001 else "99%" if p_val < 0.01 else "95%" if p_val < 0.05 else "No"
    incr = int(abs_lift / 100 * a["clients"])

    # NIBT: only for acquisition campaigns
    if mne in NIBT_ELIGIBLE and sig != "No" and abs_lift > 0:
        nibt = round(incr * NIBT_PER_CLIENT, 2)
        nibt_flag = "eligible"
    elif mne in NIBT_ARGUABLE and sig != "No" and abs_lift > 0:
        nibt = round(incr * NIBT_PER_CLIENT, 2)
        nibt_flag = "arguable"
    else:
        nibt = 0
        nibt_flag = "n/a" if mne not in NIBT_ELIGIBLE + NIBT_ARGUABLE else "not_sig"

    return {
        "MNE": mne,
        "COHORT": cohort,
        "RPT_GRP_CD": rpt_grp,
        "TACTIC_CELL_CD": channel,
        "action_deployments": a["deployments"],
        "control_deployments": c["deployments"],
        "action_clients": a["clients"],
        "control_clients": c["clients"],
        "action_successes": a["successes"],
        "control_successes": c["successes"],
        "action_rate_pct": round(a_rate, 4),
        "control_rate_pct": round(c_rate, 4),
        "abs_lift_pp": round(abs_lift, 4),
        "rel_lift_pct": round(rel_lift, 2),
        "p_value": round(p_val, 6),
        "sig_flag": sig,
        "incremental_clients": incr,
        "nibt_revenue": nibt,
        "nibt_flag": nibt_flag,
        "avg_contacts": cs.get("avg_contacts"),
        "median_contacts": cs.get("median_contacts"),
        "p90_contacts": cs.get("p90_contacts"),
        "max_contacts": cs.get("max_contacts"),
        "email_deploys": em.get("email_deploys"),
        "email_sent": em.get("email_sent"),
        "email_open_rate": em.get("email_open_rate"),
        "email_click_rate": em.get("email_click_rate"),
        "email_unsub_rate": em.get("email_unsub_rate"),
    }

rows = []

# Per RPT_GRP_CD detail rows (action detail vs cohort-level control baseline)
for row in action_detail_raw:
    mne, cohort, rpt = str(row.MNE), str(row.COHORT), str(row.RPT_GRP_CD)
    a = {"deployments": int(row.deployments), "clients": int(row.clients), "successes": int(row.successes)}
    c = control_cohort.get((mne, cohort), {"deployments": 0, "clients": 0, "successes": 0})
    cs = contact_stats.get(mne, {})
    em = email_stats.get((mne, cohort, rpt), {})
    rows.append(build_row(mne, cohort, rpt, str(row.TACTIC_CELL_CD), a, c, cs, em))

# Per-cohort ALL rollup (all RPT_GRP_CDs combined)
for row in action_cohort_raw:
    mne, cohort = str(row.MNE), str(row.COHORT)
    a = {"deployments": int(row.deployments), "clients": int(row.clients), "successes": int(row.successes)}
    c = control_cohort.get((mne, cohort), {"deployments": 0, "clients": 0, "successes": 0})
    cs = contact_stats.get(mne, {})
    em = email_stats.get((mne, cohort, "ALL"), {})
    rows.append(build_row(mne, cohort, "ALL", "ALL", a, c, cs, em))

# Overall rollup (ALL cohorts, ALL RPT_GRP_CDs)
for row in action_overall_raw:
    mne = str(row.MNE)
    a = {"deployments": int(row.deployments), "clients": int(row.clients), "successes": int(row.successes)}
    c = control_overall.get(mne, {"deployments": 0, "clients": 0, "successes": 0})
    cs = contact_stats.get(mne, {})
    em = email_stats.get((mne, "ALL", "ALL"), {})
    rows.append(build_row(mne, "ALL", "ALL", "ALL", a, c, cs, em))

mega_output = pd.DataFrame(rows)
mega_output = mega_output.sort_values(["MNE", "COHORT", "RPT_GRP_CD"]).reset_index(drop=True)

print("=" * 75)
print(f"MEGA-OUTPUT: {len(mega_output)} rows × {len(mega_output.columns)} columns")
print("=" * 75)

# Summary view: overall ALL rows only
all_rows = mega_output[(mega_output["COHORT"] == "ALL") & (mega_output["RPT_GRP_CD"] == "ALL")]
print(f"\n{'MNE':<6} {'A_Rate':>7} {'C_Rate':>7} {'Lift':>7} {'Sig':>6} "
      f"{'Incr':>7} {'NIBT':>10} {'Flag':>9} {'AvgC':>5}")
print("-" * 72)
for _, r in all_rows.iterrows():
    print(f"{r['MNE']:<6} {r['action_rate_pct']:>6.2f}% {r['control_rate_pct']:>6.2f}% "
          f"{r['abs_lift_pp']:>+6.2f}% {r['sig_flag']:>6} {r['incremental_clients']:>7,} "
          f"${r['nibt_revenue']:>9,.0f} {r['nibt_flag']:>9} {r['avg_contacts']:>5.1f}")

eligible_nibt = all_rows[all_rows["nibt_flag"] == "eligible"]["nibt_revenue"].sum()
arguable_nibt = all_rows[all_rows["nibt_flag"] == "arguable"]["nibt_revenue"].sum()
print(f"\nNIBT (eligible — VCN, VDA):     ${eligible_nibt:,.0f}")
print(f"NIBT (arguable — VDT):          ${arguable_nibt:,.0f}")
print(f"NIBT (combined):                ${eligible_nibt + arguable_nibt:,.0f}")

# RPT_GRP_CD breakdown for reference
print(f"\n{'='*75}")
print("RPT_GRP_CD × CHANNEL BREAKDOWN (overall)")
print("=" * 75)
rpt_rows = mega_output[(mega_output["COHORT"] == "ALL") & (mega_output["RPT_GRP_CD"] != "ALL")]
print(f"{'MNE':<6} {'RPT_GRP_CD':<14} {'Channel':<8} {'Deploys':>10} {'Clients':>10} {'Rate%':>8}")
print("-" * 60)
for _, r in rpt_rows.sort_values(["MNE", "action_deployments"], ascending=[True, False]).iterrows():
    print(f"{r['MNE']:<6} {r['RPT_GRP_CD']:<14} {r['TACTIC_CELL_CD']:<8} "
          f"{r['action_deployments']:>10,} {r['action_clients']:>10,} "
          f"{r['action_rate_pct']:>7.2f}%")

download_csv(mega_output, "vvd_v2_mega_output.csv")


# ============================================================
# CELL 7: PHASE 3 — OVERCONTACTING & ORCHESTRATION
# Re-runnable. Uses persisted result_df.
# ============================================================

action_df = result_df.filter(F.col("TST_GRP_CD") == ACTION_GROUP)

# --- 3a: Contact Frequency Distribution ---
client_contacts = (
    action_df
    .groupBy("CLNT_NO", "MNE")
    .agg(
        F.count("*").alias("contact_count"),
        F.max("SUCCESS").alias("any_success")
    )
)

client_total = (
    action_df
    .groupBy("CLNT_NO")
    .agg(
        F.count("*").alias("total_contacts"),
        F.countDistinct("MNE").alias("distinct_campaigns"),
        F.max("SUCCESS").alias("any_success")
    )
)

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

# --- 3b: Diminishing Returns ---
w_seq = Window.partitionBy("CLNT_NO", "MNE").orderBy("TREATMT_STRT_DT")
action_seq = action_df.withColumn("contact_seq", F.row_number().over(w_seq))

print("=" * 60)
print("DIMINISHING RETURNS -- SUCCESS BY CONTACT SEQUENCE")
print("=" * 60)

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

# --- 3c: Campaign Gap Analysis ---
print("=" * 60)
print("CAMPAIGN GAP ANALYSIS")
print("=" * 60)

w_all = Window.partitionBy("CLNT_NO").orderBy("TREATMT_STRT_DT")
action_ordered = (
    action_df
    .withColumn("prev_start", F.lag("TREATMT_STRT_DT").over(w_all))
    .withColumn("prev_mne", F.lag("MNE").over(w_all))
    .filter(F.col("prev_start").isNotNull())
    .withColumn("gap_days", F.datediff("TREATMT_STRT_DT", "prev_start"))
)

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

overlap_count = sum(int(r.transitions) for r in gap_buckets if "Same" in str(r.gap_bucket) or any(x in str(r.gap_bucket) for x in ["1-7", "8-15", "16-25"]))
overlap_clients = action_ordered.filter(F.col("gap_days") < 25).select("CLNT_NO").distinct().count()
print(f"\nOverlapping deployments (gap < 25 days): {overlap_count:,} transitions, {overlap_clients:,} clients")

# --- 3d: Campaign Transition Matrix ---
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
for row in transition_matrix[:20]:
    print(f"{str(row.prev_mne):<8} {str(row.MNE):<8} {int(row.transitions):>12,} {int(row.clients):>10,} {float(row.avg_gap_days):>8.1f}d")

print(f"\nSELF-LOOP ANALYSIS (same campaign -> same campaign):")
print("-" * 50)
for row in transition_matrix:
    if str(row.prev_mne) == str(row.MNE):
        print(f"  {str(row.MNE)}->{str(row.MNE)}: {int(row.transitions):,} transitions, {int(row.clients):,} clients, avg gap {float(row.avg_gap_days):.1f} days")


# ============================================================
# CELL 8: PHASE 4 — CHANNEL ANALYSIS (TACTIC_CELL_CD)
# Re-runnable. Uses persisted result_df.
# ============================================================

# TACTIC_CELL_CD encodes the delivery channel for each deployment.
# Known codes: EM (email), IM (in-app message), MB (mobile), XX (control holdout),
# plus combos like EM_IM, IM_MB, IM_EM_MB.
# Understanding channel mix is critical for knowing which touchpoints drive success.

# --- 4.1 Channel Distribution by Campaign and Test Group ---
# Shows how deployments are split across channels within each campaign.
# Control group (TG7) is typically all XX (holdout), while action group (TG4) has real channels.
print("=" * 60)
print("CHANNEL DISTRIBUTION BY CAMPAIGN & TEST GROUP")
print("=" * 60)

channel_dist = (
    result_df
    .withColumn("CHANNEL", F.trim(F.col("TACTIC_CELL_CD")))
    .groupBy("MNE", "TST_GRP_CD", "CHANNEL")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
    )
    .orderBy("MNE", "TST_GRP_CD", F.desc("deployments"))
    .collect()
)

print(f"{'MNE':<6} {'Group':<6} {'Channel':<12} {'Deployments':>12} {'Clients':>10}")
print("-" * 50)
for row in channel_dist:
    print(f"{str(row.MNE):<6} {str(row.TST_GRP_CD):<6} {str(row.CHANNEL):<12} {int(row.deployments):>12,} {int(row.clients):>10,}")

# --- 4.2 Channel Balance Check ---
# For each campaign, compare the channel mix between action and control.
# If control is all XX, that's the expected holdout pattern.
# If action has a very different channel mix across campaigns, flag it.
print(f"\n{'='*60}")
print("CHANNEL BALANCE CHECK")
print("=" * 60)

# Build per-campaign channel profile
channel_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
for row in channel_dist:
    channel_data[str(row.MNE)][str(row.TST_GRP_CD)][str(row.CHANNEL)] += int(row.deployments)

for mne in sorted(channel_data.keys()):
    action_channels = channel_data[mne].get(ACTION_GROUP, {})
    control_channels = channel_data[mne].get(CONTROL_GROUP, {})
    action_total = sum(action_channels.values())
    control_total = sum(control_channels.values())

    # Check if control is all XX
    control_xx_pct = control_channels.get("XX", 0) / control_total * 100 if control_total > 0 else 0
    control_pattern = "XX-only holdout" if control_xx_pct > 95 else "mixed channels"
    print(f"\n{mne} ({CAMPAIGNS[mne]['name']}):")
    print(f"  Control pattern: {control_pattern} ({control_xx_pct:.1f}% XX)")
    print(f"  Action channels:")
    for ch in sorted(action_channels.keys(), key=lambda c: action_channels[c], reverse=True):
        ch_pct = action_channels[ch] / action_total * 100
        print(f"    {ch:<12} {action_channels[ch]:>10,} ({ch_pct:>5.1f}%)")

# --- 4.3 Success Rate by Channel (Action Group Only) ---
# Since control is typically XX (holdout), channel-level success rates are only
# meaningful for the action group. This shows which channels drive conversion.
print(f"\n{'='*60}")
print("SUCCESS RATE BY CHANNEL (Action Group)")
print("=" * 60)

channel_success = (
    result_df
    .filter(F.col("TST_GRP_CD") == ACTION_GROUP)
    .withColumn("CHANNEL", F.trim(F.col("TACTIC_CELL_CD")))
    .groupBy("MNE", "CHANNEL")
    .agg(
        F.count("*").alias("deployments"),
        F.sum("SUCCESS").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("deployments") * 100)
    .orderBy("MNE", F.desc("success_rate"))
    .collect()
)

print(f"{'MNE':<6} {'Channel':<12} {'Deployments':>12} {'Successes':>10} {'Success Rate':>13}")
print("-" * 57)
for row in channel_success:
    print(f"{str(row.MNE):<6} {str(row.CHANNEL):<12} {int(row.deployments):>12,} {int(row.successes):>10,} {float(row.success_rate):>12.2f}%")

# --- 4.4 Lift by Channel ---
# If control group shares the same channel codes as action (not XX-only),
# we can compute per-channel lift. Otherwise, show action-only rates with a note.
print(f"\n{'='*60}")
print("LIFT BY CHANNEL")
print("=" * 60)

# Build action and control rates by MNE x channel
channel_rates = defaultdict(lambda: defaultdict(dict))
channel_all = (
    result_df
    .withColumn("CHANNEL", F.trim(F.col("TACTIC_CELL_CD")))
    .groupBy("MNE", "TST_GRP_CD", "CHANNEL")
    .agg(
        F.count("*").alias("deployments"),
        F.sum("SUCCESS").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("deployments") * 100)
    .collect()
)

for row in channel_all:
    channel_rates[str(row.MNE)][(str(row.TST_GRP_CD), str(row.CHANNEL))] = {
        "deployments": int(row.deployments),
        "successes": int(row.successes),
        "rate": float(row.success_rate)
    }

for mne in sorted(channel_rates.keys()):
    rates = channel_rates[mne]
    control_channels_set = {ch for (tg, ch) in rates.keys() if tg == CONTROL_GROUP}
    action_channels_set = {ch for (tg, ch) in rates.keys() if tg == ACTION_GROUP}
    control_is_xx = control_channels_set == {"XX"} or (len(control_channels_set) == 1 and "XX" in control_channels_set)

    print(f"\n{mne} ({CAMPAIGNS[mne]['name']}):")
    if control_is_xx:
        # Control is holdout only -- cannot compute per-channel lift
        overall_ctrl = rates.get((CONTROL_GROUP, "XX"), {})
        ctrl_rate = overall_ctrl.get("rate", 0)
        print(f"  Control is XX-only (rate: {ctrl_rate:.2f}%). Showing action rates vs overall control:")
        print(f"  {'Channel':<12} {'Action Rate':>12} {'vs Control':>11}")
        print(f"  {'-'*38}")
        for ch in sorted(action_channels_set):
            a = rates.get((ACTION_GROUP, ch), {})
            a_rate = a.get("rate", 0)
            diff = a_rate - ctrl_rate
            print(f"  {ch:<12} {a_rate:>11.2f}% {diff:>+10.2f}%")
    else:
        # Both groups have matching channels -- compute per-channel lift
        all_channels = action_channels_set | control_channels_set
        print(f"  {'Channel':<12} {'Action Rate':>12} {'Control Rate':>13} {'Abs Lift':>9} {'Rel Lift':>9}")
        print(f"  {'-'*58}")
        for ch in sorted(all_channels):
            a = rates.get((ACTION_GROUP, ch), {})
            c = rates.get((CONTROL_GROUP, ch), {})
            a_rate = a.get("rate", 0)
            c_rate = c.get("rate", 0)
            abs_lift = a_rate - c_rate
            rel_lift = ((a_rate - c_rate) / c_rate * 100) if c_rate > 0 else 0
            note = ""
            if not a:
                note = " (action N/A)"
            elif not c:
                note = " (control N/A)"
            print(f"  {ch:<12} {a_rate:>11.2f}% {c_rate:>12.2f}% {abs_lift:>+8.2f}% {rel_lift:>+8.1f}%{note}")

# --- 4.5 Multi-Channel Clients ---
# Clients who received contacts via multiple channels within the same campaign.
# High multi-channel rates suggest orchestrated multi-touch or data issues.
print(f"\n{'='*60}")
print("MULTI-CHANNEL CLIENTS (Action Group)")
print("=" * 60)

multi_channel = (
    result_df
    .filter(F.col("TST_GRP_CD") == ACTION_GROUP)
    .withColumn("CHANNEL", F.trim(F.col("TACTIC_CELL_CD")))
    .groupBy("CLNT_NO", "MNE")
    .agg(F.countDistinct("CHANNEL").alias("distinct_channels"))
)

multi_channel_dist = (
    multi_channel
    .groupBy("MNE", "distinct_channels")
    .agg(F.count("*").alias("clients"))
    .orderBy("MNE", "distinct_channels")
    .collect()
)

print(f"{'MNE':<6} {'Channels':>9} {'Clients':>10}")
print("-" * 28)
for row in multi_channel_dist:
    print(f"{str(row.MNE):<6} {int(row.distinct_channels):>9} {int(row.clients):>10,}")

multi_total = (
    multi_channel
    .filter(F.col("distinct_channels") > 1)
    .groupBy("MNE")
    .agg(F.count("*").alias("multi_clients"))
    .collect()
)

if multi_total:
    print(f"\nClients with 2+ channels per campaign:")
    for row in multi_total:
        print(f"  {str(row.MNE)}: {int(row.multi_clients):,}")
else:
    print("\nNo multi-channel clients found (each client used a single channel per campaign).")


# ============================================================
# CELL 9: PHASE 5 — REPORT GROUP ANALYSIS (RPT_GRP_CD)
# Re-runnable. Uses persisted result_df.
# ============================================================

# RPT_GRP_CD is a report group code assigned during experiment setup.
# It can represent sub-segments (e.g., risk tier, customer lifecycle stage) used
# for stratified analysis. Understanding how success varies by report group
# reveals whether certain segments respond better to campaigns.

# --- 5.1 RPT_GRP_CD Distribution ---
# How many deployments and clients fall into each report group, by campaign.
print("=" * 60)
print("REPORT GROUP DISTRIBUTION")
print("=" * 60)

rpt_dist = (
    result_df
    .groupBy("MNE", "RPT_GRP_CD")
    .agg(
        F.count("*").alias("deployments"),
        F.countDistinct("CLNT_NO").alias("clients"),
    )
    .orderBy("MNE", "RPT_GRP_CD")
    .collect()
)

print(f"{'MNE':<6} {'RPT_GRP_CD':<14} {'Deployments':>12} {'Clients':>10}")
print("-" * 45)
for row in rpt_dist:
    print(f"{str(row.MNE):<6} {str(row.RPT_GRP_CD):<14} {int(row.deployments):>12,} {int(row.clients):>10,}")

# --- 5.2 Success Rate by Report Group ---
# Per campaign, show how success rate differs across report groups and test groups.
# This is the key table for stratified performance analysis.
print(f"\n{'='*60}")
print("SUCCESS RATE BY REPORT GROUP & TEST GROUP")
print("=" * 60)

rpt_success = (
    result_df
    .groupBy("MNE", "RPT_GRP_CD", "TST_GRP_CD")
    .agg(
        F.count("*").alias("deployments"),
        F.sum("SUCCESS").alias("successes"),
    )
    .withColumn("success_rate", F.col("successes") / F.col("deployments") * 100)
    .orderBy("MNE", "RPT_GRP_CD", "TST_GRP_CD")
    .collect()
)

print(f"{'MNE':<6} {'RPT_GRP_CD':<14} {'Group':<6} {'Deployments':>12} {'Successes':>10} {'Rate':>8}")
print("-" * 60)
for row in rpt_success:
    print(f"{str(row.MNE):<6} {str(row.RPT_GRP_CD):<14} {str(row.TST_GRP_CD):<6} {int(row.deployments):>12,} {int(row.successes):>10,} {float(row.success_rate):>7.2f}%")

# --- 5.3 Lift by Report Group ---
# For each campaign x report group, compute action vs control rates and lift.
# Identifies which sub-segments benefit most from the campaign treatment.
print(f"\n{'='*60}")
print("LIFT BY REPORT GROUP")
print("=" * 60)

# Build lookup: (MNE, RPT_GRP_CD, TST_GRP_CD) -> stats
rpt_data = defaultdict(lambda: defaultdict(dict))
for row in rpt_success:
    rpt_data[(str(row.MNE), str(row.RPT_GRP_CD))][str(row.TST_GRP_CD)] = {
        "deployments": int(row.deployments),
        "successes": int(row.successes),
        "rate": float(row.success_rate)
    }

print(f"{'MNE':<6} {'RPT_GRP_CD':<14} {'Action Rate':>12} {'Control Rate':>13} {'Abs Lift':>9} {'Rel Lift':>9}")
print("-" * 66)

for (mne, rpt) in sorted(rpt_data.keys()):
    action = rpt_data[(mne, rpt)].get(ACTION_GROUP, {})
    control = rpt_data[(mne, rpt)].get(CONTROL_GROUP, {})

    a_rate = action.get("rate", 0)

    if not control:
        # No control clients for this report group
        print(f"{mne:<6} {rpt:<14} {a_rate:>11.2f}% {'N/A':>13} {'N/A':>9} {'N/A':>9}  (no control)")
        continue

    c_rate = control.get("rate", 0)
    abs_lift = a_rate - c_rate
    rel_lift = ((a_rate - c_rate) / c_rate * 100) if c_rate > 0 else 0

    print(f"{mne:<6} {rpt:<14} {a_rate:>11.2f}% {c_rate:>12.2f}% {abs_lift:>+8.2f}% {rel_lift:>+8.1f}%")


# ============================================================
# CELL 10: VINTAGE CURVE CONSTRUCTION
# Builds day-by-day cumulative success curves per campaign x cohort x test group.
# Output schema matches Vintage/Vvd pipeline:
#   MNE | COHORT | TST_GRP_CD | RPT_GRP_CD | METRIC | DAY | WINDOW_DAYS | CLIENT_CNT | SUCCESS_CNT | RATE
# ============================================================

import pandas as pd
import base64

def download_csv(data, filename="results.csv"):
    """Create a clickable download link for a DataFrame in Jupyter."""
    csv_data = data.to_csv(index=False)
    size_mb = len(csv_data.encode('utf-8')) / (1024 * 1024)
    if size_mb > 50:
        print(f"Data too large ({size_mb:.1f} MB). Filter before exporting.")
        return
    b64 = base64.b64encode(csv_data.encode()).decode()
    link = (
        f'<a download="{filename}" href="data:text/csv;base64,{b64}" '
        f'style="padding:6px 12px; background:#2196F3; color:white; '
        f'text-decoration:none; border-radius:3px; margin:2px; display:inline-block;">'
        f'Download {filename}</a>'
    )
    display(HTML(f'<div style="margin:5px 0;">{link} <span style="color:#666;">({size_mb:.2f} MB)</span></div>'))

# Map each metric to its FIRST_*_DT column in result_df
METRIC_TO_DATE_COL = {
    metric: f"FIRST_{col}_DT"
    for metric, col in METRIC_TO_COLUMN.items()
}
# Email engagement metrics use their own date columns
METRIC_TO_DATE_COL["email_sent"] = "EMAIL_SENT_DT"
METRIC_TO_DATE_COL["email_open"] = "EMAIL_OPENED_DT"
METRIC_TO_DATE_COL["email_click"] = "EMAIL_CLICKED_DT"
METRIC_TO_DATE_COL["email_unsub"] = "EMAIL_UNSUBSCRIBED_DT"

# Determine which metrics apply to each campaign
# PRIMARY always applies; SECONDARY if configured; EMAIL_OPEN/EMAIL_CLICK if campaign has email clients
campaign_metrics = {}
for mne, cfg in CAMPAIGNS.items():
    metrics = [cfg["primary_metric"]]
    if "secondary_metric" in cfg:
        metrics.append(cfg["secondary_metric"])
    campaign_metrics[mne] = metrics

# Check which campaigns have email-channel deployments
email_mnes = [
    str(r.MNE) for r in
    result_df.filter(F.col("TACTIC_CELL_CD").contains("EM"))
    .select("MNE").distinct().collect()
]
for mne in email_mnes:
    campaign_metrics[mne].extend(["email_sent", "email_open", "email_click", "email_unsub"])

# Build one long DataFrame: one row per (deployment, metric) with days_to_success computed
# Then explode day range 0..WINDOW_DAYS and aggregate
curve_parts = []

for mne, metrics in campaign_metrics.items():
    mne_df = result_df.filter(F.col("MNE") == mne)
    for metric in metrics:
        date_col = METRIC_TO_DATE_COL[metric]

        metric_curve = (
            mne_df
            .withColumn("METRIC", F.lit(metric))
            .withColumn("DAYS_TO_SUCCESS",
                F.when(F.col(date_col).isNotNull(),
                       F.datediff(F.col(date_col), F.col("TREATMT_STRT_DT")))
            )
        )
        curve_parts.append(
            metric_curve.select(
                "MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD",
                "METRIC", "WINDOW_DAYS", "DAYS_TO_SUCCESS"
            )
        )

# Union all campaign x metric combinations
all_curves = curve_parts[0]
for part in curve_parts[1:]:
    all_curves = all_curves.unionByName(part)

# Compute CLIENT_CNT and median WINDOW_DAYS per group
group_cols = ["MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "METRIC"]
group_stats = (
    all_curves
    .groupBy(*group_cols)
    .agg(
        F.count("*").alias("CLIENT_CNT"),
        F.expr("percentile_approx(WINDOW_DAYS, 0.5)").alias("WINDOW_DAYS"),
    )
)

# Generate day range 0..WINDOW_DAYS per group, then count successes at each day
# explode(sequence(0, WINDOW_DAYS)) produces one row per day value
day_range = (
    group_stats
    .withColumn("DAY", F.explode(F.sequence(F.lit(0), F.col("WINDOW_DAYS"))))
)

# Compute per-group success counts at each day threshold
# A client counts as success at DAY d if DAYS_TO_SUCCESS <= d
success_at_day = (
    all_curves
    .groupBy(*group_cols, "DAYS_TO_SUCCESS")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("DAYS_TO_SUCCESS").isNotNull())
)

# Cross join day range with success counts, then sum where DAYS_TO_SUCCESS <= DAY
vintage_curves = (
    day_range
    .join(success_at_day, on=group_cols, how="left")
    .filter(
        F.col("DAYS_TO_SUCCESS").isNull() |
        (F.col("DAYS_TO_SUCCESS") <= F.col("DAY"))
    )
    .groupBy(*group_cols, "DAY", "WINDOW_DAYS", "CLIENT_CNT")
    .agg(F.coalesce(F.sum("cnt"), F.lit(0)).alias("SUCCESS_CNT"))
    .withColumn("RATE", F.col("SUCCESS_CNT") / F.col("CLIENT_CNT") * 100)
    .select("MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "METRIC",
            "DAY", "WINDOW_DAYS", "CLIENT_CNT", "SUCCESS_CNT", "RATE")
    .orderBy("MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "METRIC", "DAY")
)

vintage_curves_pd = vintage_curves.toPandas()
print(f"Vintage curves: {len(vintage_curves_pd):,} rows")
print(vintage_curves_pd.head(20).to_string(index=False))

# Clickable download link — same as Vintage/Vvd pipeline
download_csv(vintage_curves_pd, "vvd_v2_vintage_curves.csv")


# ============================================================
# CELL 11: CLEANUP
# Run when done to release cluster memory.
# ============================================================

result_df.unpersist()
print("result_df unpersisted. Cluster memory released.")
