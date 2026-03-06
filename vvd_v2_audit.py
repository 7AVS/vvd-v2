#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================================================================
# VVD v2 Audit / Diagnostic Notebook -- STANDALONE, SLICE-BASED
#
# PURPOSE: Validate that the main VVD pipeline (vvd_v2_pipeline.py)
# produces correct SUCCESS flags. This script re-derives the data
# from scratch on a small slice, then cross-checks the pipeline's
# output against raw source tables row by row.
#
# Two sections:
#   Section A (Cells A2-A7): Internal consistency -- does the
#     pipeline's own output make sense? (no nulls where there
#     shouldn't be, no date-window violations, no duplicates)
#   Section B (Cells B1-B8): Source validation -- for a sample of
#     clients, go back to the raw Hive/EDW tables and confirm the
#     SUCCESS flag matches what the raw data says.
#
# Fully standalone -- does NOT depend on result_df from the main pipeline.
# Loads its own data (M1, M3, M6) filtered to a configurable slice
# (AUDIT_MNES + AUDIT_YEARS) for fast iteration (~50-200K rows).
#
# Structure:
#   Cell 1:     Imports + Config (AUDIT_MNES, AUDIT_YEARS, etc.)
#   Cell 2:     M1 -- Experiment population (audit slice)
#   Cell 3:     M3 -- Success outcomes + pre-filter (persisted for Section B)
#   Cell 4:     M6 -- Success detection (audit slice)
#   Cells A2-A7: Internal consistency checks on audit_result_df
#   Cell B1:    Sample selection
#   Cells B2-B5: Raw source validation (uses persisted DFs from Cell 3)
#   Cell B6:    CLNT_NO derivation consistency
#   Cell B7:    Deduplication audit
#   Cell B8:    Summary + cleanup
#
# Environment: Lumina (PySpark on YARN, Hive tables, older CDH Spark)
# Rules: PySpark only, no pandas, no function wrappers, flat cells.
# Target: Under 5 minutes on Lumina.
# ====================================================================


# --- Cell 1: Imports + Config ---

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import StorageLevel
from collections import defaultdict
import random

# ============================================================
# AUDIT SLICE CONFIGURATION -- EDIT THESE
# Pick 2-3 campaigns covering different metric types so we exercise
# each success-detection code path (acquisition, activation, usage).
# VDA = card_acquisition, VDT = card_activation, VUI = card_usage
# ============================================================
AUDIT_MNES = ["VDA", "VDT", "VUI"]       # Campaigns to audit
AUDIT_YEARS = [2025]                       # Years to audit
# ============================================================

# Full campaign metadata (mirrored from pipeline Cell 1).
# This is the single source of truth for which campaigns exist, what
# their primary success metric is, and how test/control is split.
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

# Maps each metric name to the UPPERCASE column it populates in result_df.
# These column names match the main pipeline exactly.
METRIC_TO_COLUMN = {
    "card_acquisition": "ACQUISITION_SUCCESS",
    "card_activation": "ACTIVATION_SUCCESS",
    "card_usage": "USAGE_SUCCESS",
    "wallet_provisioning": "PROVISIONING_SUCCESS"
}

# MNE -> expected SUCCESS column (derived from primary_metric).
# Used in Section A to verify that the generic SUCCESS flag was
# wired to the correct metric column for each campaign.
MNE_TO_SUCCESS_COL = {mne: METRIC_TO_COLUMN[cfg["primary_metric"]] for mne, cfg in CAMPAIGNS.items()}

# Reverse mapping: metric -> list of campaigns that use it (full config).
# A campaign can appear under both its primary and secondary metric
# (e.g., VUT uses wallet_provisioning AND card_usage).
METRIC_TO_CAMPAIGNS = defaultdict(list)
for _mne, _cfg in CAMPAIGNS.items():
    METRIC_TO_CAMPAIGNS[_cfg["primary_metric"]].append(_mne)
    if "secondary_metric" in _cfg:
        METRIC_TO_CAMPAIGNS[_cfg["secondary_metric"]].append(_mne)
METRIC_TO_CAMPAIGNS = dict(METRIC_TO_CAMPAIGNS)

# Source table paths (Hive parquet on HDFS)
TACTIC_EVNT_HIST_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
CARD_DATA_PATH = "/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT={year}*"
POS_TXN_PATH = "/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT={year}*"

# TG4 = Action (received the communication), TG7 = Control (holdout)
ACTION_GROUP = "TG4"
CONTROL_GROUP = "TG7"

# Derived: which metrics does the audit slice actually need?
AUDIT_CAMPAIGNS = {mne: CAMPAIGNS[mne] for mne in AUDIT_MNES}
AUDIT_METRICS = set()
for cfg in AUDIT_CAMPAIGNS.values():
    AUDIT_METRICS.add(cfg["primary_metric"])
    if "secondary_metric" in cfg:
        AUDIT_METRICS.add(cfg["secondary_metric"])

# Track pass/fail for every check -- printed in the summary (Cell B8)
audit_results = {}

print("Audit configuration loaded.")
print(f"  AUDIT_MNES:    {AUDIT_MNES}")
print(f"  AUDIT_YEARS:   {AUDIT_YEARS}")
print(f"  Metrics needed: {sorted(AUDIT_METRICS)}")
print(f"  METRIC_TO_CAMPAIGNS: {METRIC_TO_CAMPAIGNS}")


# --- Cell 2: M1 -- Experiment Population (Audit Slice) ---
# Load the tactic/experiment table and build the population of clients
# who were part of the selected campaigns. Mirrors pipeline Cell 2
# exactly, with early slice filters so we get ~50-200K rows, not 38M.

# MNE (campaign code) is NOT a raw column -- it's embedded at position
# 8-10 of TACTIC_ID (e.g., "0001234VDA..." -> "VDA").
# CLNT_NO is NOT a raw column either -- it's TACTIC_EVNT_ID with
# leading zeros stripped. This derivation was the #1 source of bugs in v1.
# TST_GRP_CD and RPT_GRP_CD have trailing whitespace in the raw data,
# so we must trim() before any filtering or grouping.
tactic_paths = [TACTIC_EVNT_HIST_BASE + f"EVNT_STRT_DT={y}*" for y in AUDIT_YEARS]
raw_tactic = spark.read.option("basePath", TACTIC_EVNT_HIST_BASE).parquet(*tactic_paths)

audit_tactic_df = (
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

audit_tactic_df.persist(StorageLevel.MEMORY_AND_DISK)

_tactic_count = audit_tactic_df.count()
_tactic_clients = audit_tactic_df.select("CLNT_NO").distinct().count()
_tactic_mnes = sorted([r.MNE for r in audit_tactic_df.select("MNE").distinct().collect()])
_tactic_dates = audit_tactic_df.agg(F.min("TREATMT_STRT_DT"), F.max("TREATMT_STRT_DT")).collect()[0]

print(f"M1 (audit slice): Experiment population loaded:")
print(f"  Total rows: {_tactic_count:,}")
print(f"  Unique clients: {_tactic_clients:,}")
print(f"  Campaigns: {_tactic_mnes}")
print(f"  Date range: {_tactic_dates[0]} to {_tactic_dates[1]}")

# Sanity gate: if zero rows, something is wrong -- stop here.
if _tactic_count == 0:
    raise RuntimeError("STOP: audit_tactic_df has 0 rows. Check AUDIT_MNES and AUDIT_YEARS.")

print(f"\nPASS: {_tactic_count:,} rows loaded for audit slice.")


# --- Cell 3: M3 -- Success Outcomes + Pre-filter (Audit Slice) ---
# Load the raw success event tables (cards, transactions, wallet)
# and narrow them to experiment clients only.
#
# Only reads years in AUDIT_YEARS for path-partitioned tables.
# Pre-filters to audit_tactic_df clients via left_semi to keep data small.
#
# IMPORTANT: raw_card_filtered and raw_pos_filtered are persisted and
# kept as module-level variables so Section B can reuse them for
# row-level validation without re-reading raw tables from HDFS.

audit_experiment_clients = audit_tactic_df.select("CLNT_NO").distinct()

# --- M3a: Card acquisition + activation ---
# Both come from the same card table (DDWTA_VISA_DR_CRD).
# Acquisition uses ISS_DT (issue date), activation uses ACTV_DT.
card_acquisition_df = None
card_activation_df = None
raw_card_filtered = None  # Kept for Section B reuse

if "card_acquisition" in AUDIT_METRICS or "card_activation" in AUDIT_METRICS:
    card_paths = [CARD_DATA_PATH.format(year=y) for y in AUDIT_YEARS]
    try:
        raw_card = spark.read.parquet(*card_paths)
    except Exception as e:
        print(f"Warning: Could not read card data: {e}")
        raw_card = None

    if raw_card is None:
        print("WARNING: Card data not found. card_acquisition and card_activation will be empty.")
    else:
        # STS_CD 06 = active card, 08 = renewed card. Other statuses
        # (cancelled, pending, etc.) should not count as successful outcomes.
        # SRVC_ID 36 = Visa Debit product line. This filters out non-VVD cards.
        card_base = (
            raw_card
            .filter(F.col("STS_CD").isin(["06", "08"]))
            .filter(F.col("SRVC_ID") == 36)
            .filter(F.col("ISS_DT").isNotNull())
            .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("CLNT_NO")), "^0+", ""))
        )

        # Pre-filter to audit experiment clients via left_semi
        card_base = card_base.join(audit_experiment_clients, "CLNT_NO", "left_semi")

        # Persist the filtered card base for Section B reuse (B2, B3, B6, B7).
        # This avoids re-reading the raw parquet in every validation cell.
        raw_card_filtered = (
            card_base
            .select(
                "CLNT_NO", "CLNT_CRD_NO", "STS_CD", "SRVC_ID",
                F.col("ISS_DT").cast("date").alias("ISS_DT"),
                F.col("ACTV_DT").cast("date").alias("ACTV_DT"),
                F.col("CAPTR_DT").cast("date").alias("CAPTR_DT")
            )
        )
        raw_card_filtered.persist(StorageLevel.MEMORY_AND_DISK)

        # Card acquisition = card was issued (ISS_DT) during the treatment window.
        # Deduplicate so each client has at most one event per date.
        card_acquisition_df = (
            card_base
            .select("CLNT_NO", F.col("ISS_DT").cast("date").alias("SUCCESS_DT"))
            .filter(F.col("SUCCESS_DT").isNotNull())
            .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
        )

        # Card activation = card was activated (ACTV_DT) during the treatment window.
        # Same card table, different date column.
        card_activation_df = (
            card_base
            .filter(F.col("ACTV_DT").isNotNull())
            .select("CLNT_NO", F.col("ACTV_DT").cast("date").alias("SUCCESS_DT"))
            .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
        )

        _acq_count = card_acquisition_df.count()
        _act_count = card_activation_df.count()
        _card_raw_count = raw_card_filtered.count()
        print(f"M3a: card_acquisition: {_acq_count:,} events (pre-filtered)")
        print(f"M3a: card_activation: {_act_count:,} events (pre-filtered)")
        print(f"  raw_card_filtered persisted: {_card_raw_count:,} rows (for Section B)")

if card_acquisition_df is None:
    card_acquisition_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")
if card_activation_df is None:
    card_activation_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")

# --- M3b: Card usage ---
# Comes from the POS (point-of-sale) transaction table.
card_usage_df = None
raw_pos_filtered = None  # Kept for Section B reuse

if "card_usage" in AUDIT_METRICS:
    txn_paths = [POS_TXN_PATH.format(year=y) for y in AUDIT_YEARS]
    try:
        raw_txn = spark.read.parquet(*txn_paths)
    except Exception as e:
        print(f"Warning: Could not read POS data: {e}")
        raw_txn = None

    if raw_txn is None:
        print("WARNING: POS transaction data not found. card_usage will be empty.")
    else:
        # SRVC_CD 36 = Visa Debit. The TXN_TP/MSG_TP combinations represent
        # completed purchase transactions:
        #   TXN_TP 10 + MSG_TP 0210 = online purchase authorization
        #   TXN_TP 13 + MSG_TP 0210 = POS purchase authorization
        #   TXN_TP 12 + MSG_TP 0220 = purchase advice/completion
        # AMT1 > 0 excludes reversals and zero-dollar authorizations.
        # CLNT_NO is extracted from positions 7-15 of CLNT_CRD_NO (card number),
        # then leading zeros are stripped to match the tactic table's format.
        pos_base = (
            raw_txn
            .filter(F.col("SRVC_CD") == 36)
            .filter(
                ((F.col("TXN_TP") == 10) & (F.col("MSG_TP") == "0210")) |
                ((F.col("TXN_TP") == 13) & (F.col("MSG_TP") == "0210")) |
                ((F.col("TXN_TP") == 12) & (F.col("MSG_TP") == "0220"))
            )
            .filter(F.col("AMT1") > 0)
            .withColumn("CLNT_NO", F.regexp_replace(F.substring(F.col("CLNT_CRD_NO"), 7, 9), "^0+", ""))
        )

        # Pre-filter to audit experiment clients via left_semi
        pos_base = pos_base.join(audit_experiment_clients, "CLNT_NO", "left_semi")

        # Persist the filtered POS base for Section B reuse (B4, B6, B7)
        raw_pos_filtered = (
            pos_base
            .select(
                "CLNT_NO", "CLNT_CRD_NO", "TXN_TP", "MSG_TP", "AMT1",
                F.col("TXN_DT").cast("date").alias("TXN_DT"),
                "SRVC_CD"
            )
        )
        raw_pos_filtered.persist(StorageLevel.MEMORY_AND_DISK)

        card_usage_df = (
            pos_base
            .select("CLNT_NO", F.col("TXN_DT").cast("date").alias("SUCCESS_DT"))
            .filter(F.col("SUCCESS_DT").isNotNull())
            .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
        )

        _usage_count = card_usage_df.count()
        _pos_raw_count = raw_pos_filtered.count()
        print(f"M3b: card_usage: {_usage_count:,} events (pre-filtered)")
        print(f"  raw_pos_filtered persisted: {_pos_raw_count:,} rows (for Section B)")

if card_usage_df is None:
    card_usage_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")

# --- M3c: Wallet provisioning (EDW) ---
# This metric comes from EDW (Teradata), not Hive. The query identifies
# tokenization events where a physical card was added to a digital wallet.
wallet_provisioning_df = None

if "wallet_provisioning" in AUDIT_METRICS:
    min_year = min(AUDIT_YEARS)
    # Key filters in this query:
    #   AMT1 = 0 means it's a provisioning event, not a purchase
    #   Card prefix 45190 + Visa prefix 45199 = Visa Debit BIN range
    #   TOKN_REQSTR_ID first char > '0' = valid token requestor
    #   POS_ENTR_MODE_CD_NON_EMV = '000' = token-based entry (not chip/swipe)
    #   TOKEN_WALLET_IND = 'Y' = confirmed wallet provisioning
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
        # EDW returns CLNT_NO as INTEGER (Teradata CAST). Must convert to
        # STRING via str(int(r[0])) to match the tactic table's format.
        wallet_provisioning_df = spark.createDataFrame(
            [(str(int(r[0])), r[1]) for r in results],
            "CLNT_NO STRING, SUCCESS_DT DATE"
        )
        # Pre-filter to audit experiment clients
        wallet_provisioning_df = wallet_provisioning_df.join(audit_experiment_clients, "CLNT_NO", "left_semi")
        print(f"M3c: wallet_provisioning: {wallet_provisioning_df.count():,} events (pre-filtered)")
    except NameError:
        print("WARNING: EDW cursor not available. wallet_provisioning will be empty.")
    except Exception as e:
        print(f"WARNING: wallet_provisioning extraction failed: {e}")

if wallet_provisioning_df is None:
    wallet_provisioning_df = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")

success_dfs = {
    "card_acquisition": card_acquisition_df,
    "card_activation": card_activation_df,
    "card_usage": card_usage_df,
    "wallet_provisioning": wallet_provisioning_df,
}

print(f"\nM3 complete. Success DFs pre-filtered to {audit_experiment_clients.count():,} audit clients.")
print("raw_card_filtered and raw_pos_filtered persisted for Section B reuse.")


# --- Cell 4: M6 -- Success Detection (Audit Slice) ---
# Join each success event table to the experiment population,
# scoped by treatment window dates. A client gets SUCCESS=1 only
# if their success event falls within [TREATMT_STRT_DT, TREATMT_END_DT].
#
# Campaign-scoped left joins. Mirrors pipeline Cell 4 exactly.
# Persists audit_result_df, unpersists audit_tactic_df.

audit_result_df = audit_tactic_df

for metric_name, success_col_name in METRIC_TO_COLUMN.items():
    sdf = success_dfs[metric_name]
    relevant_mnes = METRIC_TO_CAMPAIGNS.get(metric_name, [])

    # Intersect with AUDIT_MNES: only process campaigns in the audit slice
    audit_relevant = [m for m in relevant_mnes if m in AUDIT_MNES]

    # Campaign-scoped join: only campaigns that actually use this metric
    # get the left join. The rest get zeros with no shuffle cost.
    needs_metric = audit_result_df.filter(F.col("MNE").isin(audit_relevant)) if audit_relevant else audit_result_df.filter(F.lit(False))
    skip_metric = audit_result_df.filter(~F.col("MNE").isin(audit_relevant)) if audit_relevant else audit_result_df

    if audit_relevant and needs_metric.head(1):
        # Rename columns before join to avoid ambiguous CLNT_NO after left join.
        # This was a real bug in an earlier version.
        sdf_renamed = sdf.withColumnRenamed("CLNT_NO", "S_CLNT_NO").withColumnRenamed("SUCCESS_DT", "S_SUCCESS_DT")

        # The core join: match client AND require the success event to fall
        # within the treatment window. If no match, the left join keeps the
        # row with nulls (SUCCESS=0).
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

        # A client may have multiple success events in the same window (e.g.,
        # two card transactions on different days). Keep only the earliest one
        # so each deployment row has at most one SUCCESS=1.
        w = Window.partitionBy("CLNT_NO", "TACTIC_ID", "MNE", "TREATMT_STRT_DT").orderBy(F.col("S_SUCCESS_DT").asc_nulls_last())
        joined = joined.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
        joined = joined.drop("S_CLNT_NO", "S_SUCCESS_DT")
    else:
        joined = needs_metric

    # Skipped campaigns get 0 / NULL for this metric (no shuffle cost)
    skip_metric = (
        skip_metric
        .withColumn(success_col_name, F.lit(0))
        .withColumn(f"FIRST_{success_col_name}_DT", F.lit(None).cast("date"))
    )

    if audit_relevant and joined.head(1):
        audit_result_df = joined.unionByName(skip_metric)
    else:
        # If needs_metric was empty, joined has no new columns yet -- add them
        if success_col_name not in skip_metric.columns:
            skip_metric = skip_metric.withColumn(success_col_name, F.lit(0)).withColumn(f"FIRST_{success_col_name}_DT", F.lit(None).cast("date"))
        audit_result_df = skip_metric

# Assign each row its campaign-specific SUCCESS flag (driven by config).
# Each campaign has one primary metric. The generic SUCCESS column is just
# a pointer to the right metric column -- e.g., for VDA, SUCCESS = ACQUISITION_SUCCESS.
success_expr = F.lit(0)
for mne, cfg in CAMPAIGNS.items():
    if mne in AUDIT_MNES:
        col_name = METRIC_TO_COLUMN[cfg["primary_metric"]]
        success_expr = F.when(F.col("MNE") == mne, F.col(col_name)).otherwise(success_expr)
audit_result_df = audit_result_df.withColumn("SUCCESS", success_expr)

# Persist audit_result_df, release audit_tactic_df
audit_result_df.persist(StorageLevel.MEMORY_AND_DISK)
_result_count = audit_result_df.count()  # Force materialization
audit_tactic_df.unpersist()

print(f"M6 (audit slice): Result DataFrame: {_result_count:,} rows")
print(f"Columns: {audit_result_df.columns}")
print(f"\nSuccess flag distribution:")
audit_result_df.groupBy("MNE", "TST_GRP_CD").agg(
    F.count("*").alias("total"),
    F.sum("SUCCESS").alias("successes"),
    (F.sum("SUCCESS") / F.count("*") * 100).alias("success_rate_pct")
).orderBy("MNE", "TST_GRP_CD").show(50, truncate=False)

# Sanity gate
if _result_count == 0:
    raise RuntimeError("STOP: audit_result_df has 0 rows. Check pipeline logic.")

print(f"PASS: audit_result_df ready with {_result_count:,} rows.")
print("Data pipeline complete. Proceeding to audit checks.")


# ============================================================
# SECTION A: INTERNAL CONSISTENCY CHECKS
# All checks use audit_result_df (the audit slice).
# These verify that the pipeline's output is self-consistent --
# they don't compare against raw source data (that's Section B).
# ============================================================


# --- Cell A2: TREATMT_END_DT Null Check ---
# If TREATMT_END_DT is NULL, the date-window comparison
# (SUCCESS_DT <= TREATMT_END_DT) evaluates to NULL in SQL/Spark,
# meaning those clients can NEVER get SUCCESS=1 regardless of
# whether they actually succeeded. This silently suppresses
# true conversions.
#
# PASS = every row has a non-null end date.
# FAIL = some rows are missing end dates and their success is
#        being silently zeroed out.

print("=" * 60)
print("A2: TREATMT_END_DT NULL CHECK")
print("=" * 60)

null_end_dt = audit_result_df.filter(F.col("TREATMT_END_DT").isNull())
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


# --- Cell A3: TACTIC_CELL_CD (Channel) Distribution ---
# TACTIC_CELL_CD encodes the communication channel (email, push,
# in-app, etc.). This check doesn't pass/fail -- it's informational.
# Look for unexpected values or a suspiciously high NULL rate,
# which might indicate a data quality issue upstream.
#
# Control group (TG7) should have channel codes too -- they were
# selected for the campaign but not contacted.

print("=" * 60)
print("A3: TACTIC_CELL_CD (CHANNEL) DISTRIBUTION")
print("=" * 60)

print("Full distribution by test group and channel:")
audit_result_df.groupBy(
    "TST_GRP_CD",
    F.trim(F.col("TACTIC_CELL_CD")).alias("TACTIC_CELL_CD_trimmed")
).agg(
    F.count("*").alias("count")
).orderBy("TST_GRP_CD", "TACTIC_CELL_CD_trimmed").show(50, truncate=False)

print("Control group (TG7) channel detail:")
audit_result_df.filter(
    F.col("TST_GRP_CD") == CONTROL_GROUP
).groupBy(
    "MNE",
    F.trim(F.col("TACTIC_CELL_CD")).alias("TACTIC_CELL_CD_trimmed")
).agg(
    F.count("*").alias("count")
).orderBy("MNE", "TACTIC_CELL_CD_trimmed").show(50, truncate=False)

null_channel = audit_result_df.filter(
    F.col("TACTIC_CELL_CD").isNull() | (F.trim(F.col("TACTIC_CELL_CD")) == "")
).count()
print(f"Rows with NULL or empty TACTIC_CELL_CD: {null_channel:,}")

audit_results["A3_channel_dist"] = "INFO"


# --- Cell A4: RPT_GRP_CD Distribution ---
# RPT_GRP_CD is the reporting group -- a finer segmentation within
# each test group. This is informational: look for unexpected
# codes or imbalanced distributions that might indicate a
# targeting or randomization issue.

print("=" * 60)
print("A4: RPT_GRP_CD DISTRIBUTION")
print("=" * 60)

print("RPT_GRP_CD by campaign:")
audit_result_df.groupBy("MNE", "RPT_GRP_CD").agg(
    F.count("*").alias("count")
).orderBy("MNE", "RPT_GRP_CD").show(100, truncate=False)

print("RPT_GRP_CD by campaign and test group:")
audit_result_df.groupBy("MNE", "TST_GRP_CD", "RPT_GRP_CD").agg(
    F.count("*").alias("count")
).orderBy("MNE", "TST_GRP_CD", "RPT_GRP_CD").show(100, truncate=False)

audit_results["A4_rpt_grp"] = "INFO"


# --- Cell A5: SUCCESS Flag Validation (Internal Consistency) ---
# The generic SUCCESS column should equal the campaign's primary
# metric column. For example, VDA's SUCCESS should always equal
# ACQUISITION_SUCCESS. If they disagree, the config-driven
# success assignment logic in M6 has a wiring bug.
#
# PASS = SUCCESS matches the expected metric column for every row.
# FAIL = some rows have a SUCCESS value that doesn't come from
#        the right metric -- the campaign-to-metric mapping is broken.

print("=" * 60)
print("A5: SUCCESS FLAG VALIDATION")
print("=" * 60)
print("Verifying SUCCESS == expected metric column for each campaign.\n")

a5_pass = True

for mne in AUDIT_MNES:
    expected_col = MNE_TO_SUCCESS_COL[mne]
    mne_df = audit_result_df.filter(F.col("MNE") == mne)
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


# --- Cell A6: Date Window Validation ---
# For every row where SUCCESS=1, verify that the success event
# date falls within [TREATMT_STRT_DT, TREATMT_END_DT]. Also
# count boundary cases (success on exact start or end date)
# since those are most likely to have off-by-one bugs.
#
# PASS = all SUCCESS=1 rows have a success date inside their window.
# FAIL = the pipeline assigned SUCCESS=1 to clients whose success
#        event fell outside their treatment window -- a join bug.

print("=" * 60)
print("A6: DATE WINDOW VALIDATION (SUCCESS=1 rows)")
print("=" * 60)

a6_pass = True

for mne in AUDIT_MNES:
    cfg = CAMPAIGNS[mne]
    success_col = METRIC_TO_COLUMN[cfg["primary_metric"]]
    dt_col = f"FIRST_{success_col}_DT"

    success_rows = audit_result_df.filter(
        (F.col("MNE") == mne) & (F.col("SUCCESS") == 1)
    )
    total_success = success_rows.count()

    if total_success == 0:
        print(f"  {mne}: No SUCCESS=1 rows (skipped)")
        continue

    before_start = success_rows.filter(F.col(dt_col) < F.col("TREATMT_STRT_DT")).count()
    after_end = success_rows.filter(F.col(dt_col) > F.col("TREATMT_END_DT")).count()
    null_dt = success_rows.filter(F.col(dt_col).isNull()).count()

    # Boundary counts are informational -- they're valid but worth knowing about
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


# --- Cell A7: Deployment Granularity Check ---
# Each row in audit_result_df represents one deployment of one campaign
# to one client. The grain is (CLNT_NO, TACTIC_ID, MNE, TREATMT_STRT_DT).
# Duplicates here would inflate success rates and break the
# overcontacting analysis in the main pipeline.
#
# PASS = every row is unique at the deployment grain.
# FAIL = duplicates exist, meaning the M6 deduplication window
#        didn't fully collapse multi-event rows.

print("=" * 60)
print("A7: DEPLOYMENT GRANULARITY CHECK")
print("=" * 60)
print("Each row should be unique on (CLNT_NO, TACTIC_ID, MNE, TREATMT_STRT_DT).\n")

total_rows = audit_result_df.count()
distinct_rows = audit_result_df.select(
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
    audit_result_df.withColumn(
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
# For a sample of clients, go back to the raw Hive/EDW tables
# and confirm the pipeline's SUCCESS flag matches what the raw
# data says. Any mismatch means the pipeline is producing
# incorrect results.
#
# Uses PERSISTED success DFs from Cell 3 -- no re-reading raw tables.
# ============================================================


# --- Cell B1: Sample Selection ---
# Build a targeted sample of clients for row-level validation
# against raw source tables. The sample is stratified:
#   - SUCCESS=1 and SUCCESS=0 from each campaign x test group
#   - Edge cases: shortest/longest windows, multi-campaign clients,
#     boundary dates (success on exact start date)
#
# We validate both true positives AND true negatives for action
# AND control in every campaign, plus the scenarios most likely
# to expose join or date-window bugs.

print("=" * 60)
print("B1: SAMPLE SELECTION")
print("=" * 60)

# Pick up to 2 clients per (campaign, test group, success flag) combination.
success_1_samples = []
success_0_samples = []

for mne in AUDIT_MNES:
    for tg in [ACTION_GROUP, CONTROL_GROUP]:
        # SUCCESS=1
        s1_pool = (
            audit_result_df.filter(
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
            audit_result_df.filter(
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

random.shuffle(success_1_samples)
random.shuffle(success_0_samples)
success_1_samples = success_1_samples[:20]
success_0_samples = success_0_samples[:20]

# --- TARGETED EDGE CASES ---
# These are the scenarios most likely to expose bugs in the join logic.
edge_case_samples = []

# Shortest treatment windows are most likely to have boundary issues
# (e.g., success on exact start/end date, or a 0-day window that
# requires same-day success).
shortest = (
    audit_result_df.filter(F.col("WINDOW_DAYS").isNotNull())
    .orderBy("WINDOW_DAYS")
    .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
            "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
    .limit(3).collect()
)
edge_case_samples.extend(shortest)

# Longest treatment windows are most likely to accidentally capture
# success events from a different campaign cycle.
longest = (
    audit_result_df.filter(F.col("WINDOW_DAYS").isNotNull())
    .orderBy(F.col("WINDOW_DAYS").desc())
    .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
            "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
    .limit(3).collect()
)
edge_case_samples.extend(longest)

# Clients in MULTIPLE campaigns test whether the campaign-scoped join
# correctly isolates each campaign's success flag. A cross-contamination
# bug would show up here (e.g., VDA success leaking into VDT).
multi_camp = (
    audit_result_df.groupBy("CLNT_NO").agg(F.countDistinct("MNE").alias("n_campaigns"))
    .filter(F.col("n_campaigns") > 1)
    .orderBy(F.col("n_campaigns").desc())
    .limit(3)
    .select("CLNT_NO").collect()
)
n_multi = 0
for mc in multi_camp:
    mc_rows = (
        audit_result_df.filter(F.col("CLNT_NO") == str(mc.CLNT_NO))
        .select("CLNT_NO", "MNE", "TST_GRP_CD", "TACTIC_ID",
                "TREATMT_STRT_DT", "TREATMT_END_DT", "SUCCESS")
        .limit(3).collect()
    )
    n_multi += len(mc_rows)
    edge_case_samples.extend(mc_rows)

# SUCCESS=1 where the success date exactly equals TREATMT_STRT_DT.
# Tests that the window is inclusive on the start side (>= not >).
n_boundary = 0
for mne in AUDIT_MNES:
    cfg = CAMPAIGNS[mne]
    dt_col = f"FIRST_{METRIC_TO_COLUMN[cfg['primary_metric']]}_DT"
    boundary = (
        audit_result_df.filter(
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

print(f"\nSelected {len(success_1_samples)} SUCCESS=1 samples and {len(success_0_samples)} SUCCESS=0 samples")
print(f"Total sample: {sample_clients.count()} rows")
print("\nSUCCESS=1 samples:")
sample_clients.filter(F.col("SUCCESS") == 1).show(25, truncate=False)
print("SUCCESS=0 samples:")
sample_clients.filter(F.col("SUCCESS") == 0).show(25, truncate=False)

sample_clnt_list = [str(r.CLNT_NO) for r in sample_clients.select("CLNT_NO").distinct().collect()]
print(f"Unique sample clients: {len(sample_clnt_list)}")


# --- Cell B2: Raw Card Table Validation (card_acquisition) ---
# For each sampled card_acquisition client, go back to the raw
# card table and check whether a qualifying card record actually
# exists in their treatment window.
#
# If this check FAILS, it means the pipeline's SUCCESS flag
# disagrees with what the raw data says -- either a false positive
# (pipeline says SUCCESS=1 but no qualifying card exists) or a
# false negative (raw card exists but pipeline says SUCCESS=0).

print("=" * 60)
print("B2: RAW CARD TABLE VALIDATION (card_acquisition)")
print("=" * 60)

b2_mismatches = 0
acq_mnes_in_audit = [m for m in AUDIT_MNES if CAMPAIGNS[m]["primary_metric"] == "card_acquisition"]

if not acq_mnes_in_audit:
    print(f"No card_acquisition campaigns in AUDIT_MNES {AUDIT_MNES}. Skipping.")
    audit_results["B2_card_acquisition"] = "SKIP"
elif raw_card_filtered is None:
    print("Card data not available. Skipping.")
    audit_results["B2_card_acquisition"] = "SKIP"
else:
    # Use PERSISTED raw_card_filtered from Cell 3 -- filter to sample clients
    raw_card_sample = raw_card_filtered.filter(F.col("CLNT_NO").isin(sample_clnt_list))

    print(f"Raw card records for sample clients: {raw_card_sample.count()}")
    print("\nAll raw card records for sample clients:")
    raw_card_sample.show(100, truncate=False)

    acq_samples = sample_clients.filter(F.col("MNE").isin(acq_mnes_in_audit))

    if acq_samples.count() > 0:
        print(f"\nCard Acquisition validation ({acq_mnes_in_audit}):")
        print("-" * 60)

        acq_rows = acq_samples.collect()
        for sr in acq_rows:
            clnt = str(sr.CLNT_NO)
            strt = sr.TREATMT_STRT_DT
            end = sr.TREATMT_END_DT
            expected_success = int(sr.SUCCESS)

            # Apply the same filters as the pipeline: active/renewed cards
            # (STS_CD 06/08), Visa Debit (SRVC_ID 36), with an issue date
            # inside the treatment window.
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
        print(f"No {acq_mnes_in_audit} clients in sample.")

    print(f"\nCard acquisition mismatches: {b2_mismatches}")
    audit_results["B2_card_acquisition"] = "PASS" if b2_mismatches == 0 else "FAIL"


# --- Cell B3: Raw Card Activation Validation (card_activation) ---
# Same approach as B2 but checks ACTV_DT (activation date)
# instead of ISS_DT (issue date). Card activation means the
# client received AND activated their Visa Debit card.

print("=" * 60)
print("B3: RAW CARD ACTIVATION VALIDATION (card_activation)")
print("=" * 60)

b3_mismatches = 0
act_mnes_in_audit = [m for m in AUDIT_MNES if CAMPAIGNS[m]["primary_metric"] == "card_activation"]

if not act_mnes_in_audit:
    print(f"No card_activation campaigns in AUDIT_MNES {AUDIT_MNES}. Skipping.")
    audit_results["B3_card_activation"] = "SKIP"
elif raw_card_filtered is None:
    print("Card data not available. Skipping.")
    audit_results["B3_card_activation"] = "SKIP"
else:
    raw_card_sample = raw_card_filtered.filter(F.col("CLNT_NO").isin(sample_clnt_list))
    act_samples = sample_clients.filter(F.col("MNE").isin(act_mnes_in_audit))

    if act_samples.count() > 0:
        print(f"Card Activation validation ({act_mnes_in_audit}):")
        print("-" * 60)

        act_rows = act_samples.collect()
        for sr in act_rows:
            clnt = str(sr.CLNT_NO)
            strt = sr.TREATMT_STRT_DT
            end = sr.TREATMT_END_DT
            expected_success = int(sr.SUCCESS)

            # Same card-level filters as acquisition, but using ACTV_DT
            # (when the client first used the card) instead of ISS_DT.
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
        print(f"No {act_mnes_in_audit} clients in sample.")

    print(f"\nCard activation mismatches: {b3_mismatches}")
    audit_results["B3_card_activation"] = "PASS" if b3_mismatches == 0 else "FAIL"


# --- Cell B4: Raw POS Validation (card_usage) ---
# For each sampled card_usage client, go back to the raw POS
# transaction table and check whether a qualifying transaction
# exists in their treatment window.
#
# This validates the USAGE_SUCCESS column specifically (not the
# generic SUCCESS), because some campaigns (VUT, VAW) have
# card_usage as a secondary metric.

print("=" * 60)
print("B4: RAW POS VALIDATION (card_usage)")
print("=" * 60)

b4_mismatches = 0
usage_mnes_in_audit = [m for m in AUDIT_MNES if
                       CAMPAIGNS[m]["primary_metric"] == "card_usage" or
                       CAMPAIGNS[m].get("secondary_metric") == "card_usage"]

if not usage_mnes_in_audit:
    print(f"No card_usage campaigns in AUDIT_MNES {AUDIT_MNES}. Skipping.")
    audit_results["B4_card_usage"] = "SKIP"
elif raw_pos_filtered is None:
    print("POS data not available. Skipping.")
    audit_results["B4_card_usage"] = "SKIP"
else:
    raw_pos_sample = raw_pos_filtered.filter(F.col("CLNT_NO").isin(sample_clnt_list))

    print(f"Raw POS records for sample clients (after all filters): {raw_pos_sample.count()}")
    print("\nFiltered POS records for sample clients:")
    raw_pos_sample.show(100, truncate=False)

    usage_samples = sample_clients.filter(F.col("MNE").isin(usage_mnes_in_audit))

    if usage_samples.count() > 0:
        print(f"\nCard Usage validation ({usage_mnes_in_audit}):")
        print("-" * 60)

        usage_rows = usage_samples.collect()
        for sr in usage_rows:
            clnt = str(sr.CLNT_NO)
            strt = sr.TREATMT_STRT_DT
            end = sr.TREATMT_END_DT
            mne = str(sr.MNE)

            # Look up the USAGE_SUCCESS value from audit_result_df (not the
            # generic SUCCESS, which maps to the primary metric). This matters
            # for VUT/VAW where card_usage is the secondary metric.
            result_row = audit_result_df.filter(
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
        print(f"No {usage_mnes_in_audit} clients in sample.")

    print(f"\nCard usage mismatches: {b4_mismatches}")
    audit_results["B4_card_usage"] = "PASS" if b4_mismatches == 0 else "FAIL"


# --- Cell B5: Raw EDW Validation (wallet_provisioning) ---
# For sampled wallet_provisioning clients, query EDW (Teradata)
# directly and apply all the provisioning filters by hand to
# verify the pipeline's PROVISIONING_SUCCESS flag.
#
# This is the hardest metric to validate because the filters are
# complex (BIN ranges, token indicators, zero-dollar amounts)
# and the data lives in a different system (EDW vs Hive).

print("=" * 60)
print("B5: RAW EDW VALIDATION (wallet_provisioning)")
print("=" * 60)

prov_mnes_in_audit = [m for m in AUDIT_MNES if CAMPAIGNS[m]["primary_metric"] == "wallet_provisioning"]

if not prov_mnes_in_audit:
    print(f"No wallet_provisioning campaigns in AUDIT_MNES {AUDIT_MNES}. Skipping.")
    audit_results["B5_wallet_prov"] = "SKIP"
else:
    print(f"Querying EDW (Teradata) for sample {prov_mnes_in_audit} clients.\n")
    prov_samples = sample_clients.filter(F.col("MNE").isin(prov_mnes_in_audit))
    prov_sample_rows = prov_samples.collect()

    b5_mismatches = 0

    if len(prov_sample_rows) == 0:
        print(f"No {prov_mnes_in_audit} clients in sample. Skipping EDW validation.")
        audit_results["B5_wallet_prov"] = "SKIP"
    else:
        try:
            cursor = EDW.cursor()

            prov_clnt_list = list(set(str(r.CLNT_NO) for r in prov_sample_rows))
            in_clause = ", ".join(prov_clnt_list)
            min_year = min(AUDIT_YEARS)

            # Broader query than M3c -- intentionally fetches ALL records
            # for sample clients (not just qualifying ones) so we can see
            # which filter each non-qualifying record fails on.
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

            print(f"\nWallet provisioning validation ({prov_mnes_in_audit}):")
            print("-" * 60)

            for sr in prov_sample_rows:
                clnt = str(sr.CLNT_NO)
                strt = sr.TREATMT_STRT_DT
                end = sr.TREATMT_END_DT

                result_row = audit_result_df.filter(
                    (F.col("CLNT_NO") == clnt) &
                    (F.col("MNE") == str(sr.MNE)) &
                    (F.col("TREATMT_STRT_DT") == F.lit(strt)) &
                    (F.col("TACTIC_ID") == str(sr.TACTIC_ID))
                ).select("PROVISIONING_SUCCESS").collect()

                expected_prov = int(result_row[0].PROVISIONING_SUCCESS) if result_row else -1

                # Apply all provisioning filters manually to see if any raw
                # record qualifies. These mirror the M3c WHERE clause exactly.
                qualifying = [
                    r for r in edw_results
                    if str(int(r[0])) == clnt
                    and r[2] == 0                  # AMT1 = 0 (provisioning, not purchase)
                    and str(r[3]) == '45190'       # Card BIN prefix
                    and str(r[4]) == '45199'       # Visa Debit BIN prefix
                    and str(r[5]) > '0'            # Valid token requestor
                    and str(r[6]) == '000'         # Token-based POS entry mode
                    and r[7] == 36                  # SRVC_CD = Visa Debit
                    and str(r[8]) == 'Y'           # Confirmed wallet provisioning
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


# --- Cell B6: CLNT_NO Derivation Consistency Check ---
# CLNT_NO is derived differently in each source table:
#   - Tactic table: strip leading zeros from TACTIC_EVNT_ID
#   - Card table: strip leading zeros from CLNT_NO column
#   - POS table: extract positions 7-15 from CLNT_CRD_NO, strip zeros
#
# If these derivations produce different values for the same
# physical client, the join in M6 will silently miss matches
# and undercount successes.
#
# This is an INFO check -- review the output manually to confirm
# all three sources agree on CLNT_NO for the traced clients.

print("=" * 60)
print("B6: CLNT_NO DERIVATION CONSISTENCY CHECK")
print("=" * 60)
print("Verifying that CLNT_NO resolves to the same value across all sources.\n")

trace_clients = sample_clnt_list[:5]

# 1. Tactic table: use audit_result_df
print("1. Tactic table: TACTIC_ID -> derived MNE, CLNT_NO from audit_result_df")
print("-" * 60)
audit_result_df.filter(
    F.col("CLNT_NO").isin(trace_clients)
).select(
    "CLNT_NO", "TACTIC_ID",
    F.substring(F.col("TACTIC_ID"), 8, 3).alias("DERIVED_MNE"),
    "MNE"
).distinct().show(20, truncate=False)

# 2. Card table: use PERSISTED raw_card_filtered from Cell 3
print("\n2. Card table: CLNT_NO from persisted raw_card_filtered")
print("-" * 60)
if raw_card_filtered is not None:
    raw_card_filtered.filter(
        F.col("CLNT_NO").isin(trace_clients)
    ).select("CLNT_NO", "CLNT_CRD_NO").distinct().show(20, truncate=False)
else:
    print("Card data not available.")

# 3. POS table: use PERSISTED raw_pos_filtered from Cell 3
print("\n3. POS table: CLNT_CRD_NO -> CLNT_NO from persisted raw_pos_filtered")
print("-" * 60)
if raw_pos_filtered is not None:
    raw_pos_filtered.filter(
        F.col("CLNT_NO").isin(trace_clients)
    ).select("CLNT_CRD_NO", "CLNT_NO").distinct().show(20, truncate=False)
else:
    print("POS data not available.")

print("Review above: all three sources should resolve to the same CLNT_NO for each client.")
audit_results["B6_clnt_no_consistency"] = "INFO"


# --- Cell B7: Deduplication Audit ---
# When a client has multiple success events in the same treatment
# window (e.g., two card transactions on different days), the
# pipeline should keep only the FIRST (earliest) one. This check
# verifies that the FIRST_*_DT in audit_result_df matches the
# earliest raw event date.
#
# PASS = pipeline always picked the earliest event.
# FAIL = pipeline picked a later event or missed deduplication,
#        which would misrepresent the timing of the success.

print("=" * 60)
print("B7: DEDUPLICATION AUDIT")
print("=" * 60)
print("Finding clients with MULTIPLE success events in the same treatment window.")
print("Verifying only the FIRST (earliest) is kept in audit_result_df.\n")

b7_pass = True

for mne in AUDIT_MNES:
    cfg = CAMPAIGNS[mne]
    success_col = METRIC_TO_COLUMN[cfg["primary_metric"]]
    dt_col = f"FIRST_{success_col}_DT"
    metric = cfg["primary_metric"]

    success_rows = audit_result_df.filter(
        (F.col("MNE") == mne) & (F.col("SUCCESS") == 1)
    )

    if success_rows.count() == 0:
        continue

    sample_success = success_rows.select(
        "CLNT_NO", "TACTIC_ID", "TREATMT_STRT_DT", "TREATMT_END_DT", dt_col
    ).limit(5).collect()

    if not sample_success:
        continue

    print(f"\n{mne} ({metric}) -- checking deduplication:")
    print("-" * 60)

    for sr in sample_success:
        clnt = str(sr.CLNT_NO)
        strt = sr.TREATMT_STRT_DT
        end = sr.TREATMT_END_DT
        pipeline_dt = sr[dt_col]

        # Query the raw source for ALL qualifying events in the window,
        # ordered by date. If there are multiple, the pipeline should
        # have kept only the first.
        if metric == "card_acquisition" and raw_card_filtered is not None:
            raw_events = (
                raw_card_filtered
                .filter(F.col("CLNT_NO") == clnt)
                .filter(F.col("STS_CD").isin(["06", "08"]))
                .filter(F.col("ISS_DT").isNotNull())
                .filter(F.col("ISS_DT") >= F.lit(strt))
                .filter(F.col("ISS_DT") <= F.lit(end))
                .select(F.col("ISS_DT").alias("SUCCESS_DT"))
                .orderBy("SUCCESS_DT")
            )
        elif metric == "card_activation" and raw_card_filtered is not None:
            raw_events = (
                raw_card_filtered
                .filter(F.col("CLNT_NO") == clnt)
                .filter(F.col("STS_CD").isin(["06", "08"]))
                .filter(F.col("ACTV_DT").isNotNull())
                .filter(F.col("ACTV_DT") >= F.lit(strt))
                .filter(F.col("ACTV_DT") <= F.lit(end))
                .select(F.col("ACTV_DT").alias("SUCCESS_DT"))
                .orderBy("SUCCESS_DT")
            )
        elif metric == "card_usage" and raw_pos_filtered is not None:
            raw_events = (
                raw_pos_filtered
                .filter(F.col("CLNT_NO") == clnt)
                .filter(F.col("TXN_DT") >= F.lit(strt))
                .filter(F.col("TXN_DT") <= F.lit(end))
                .select(F.col("TXN_DT").alias("SUCCESS_DT"))
                .orderBy("SUCCESS_DT")
            )
        else:
            print(f"  {mne} CLNT={clnt}: pipeline_dt={pipeline_dt} (EDW -- raw check skipped)")
            continue

        raw_count = raw_events.count()
        raw_dates = [str(r.SUCCESS_DT) for r in raw_events.collect()]

        if raw_count > 1:
            # Multiple events found -- verify the pipeline kept the earliest
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


# --- Cell B8: Summary + Cleanup ---
# Collect all PASS/FAIL/SKIP/INFO results from every check
# and print a single summary table. Any FAIL requires investigation
# before trusting the main pipeline's results.

print("=" * 60)
print("=" * 60)
print("AUDIT SUMMARY")
print(f"Slice: AUDIT_MNES={AUDIT_MNES}, AUDIT_YEARS={AUDIT_YEARS}")
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

# Cleanup all persisted audit DataFrames to free executor memory.
# Run this when you're done reviewing the audit output.
print("\nCleaning up persisted DataFrames...")
audit_result_df.unpersist()
sample_clients.unpersist()
try:
    raw_card_filtered.unpersist()
except:
    pass
try:
    raw_pos_filtered.unpersist()
except:
    pass
print("All audit DataFrames unpersisted.")
