#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================================================================
# VVD v2 — Deep Analysis (Post-Success Consumption & Diagnostics)
#
# Separate from the main pipeline. Reads persisted result_df + raw POS.
# Each campaign has its own treatment window — no hardcoded 90 days.
#
# Campaigns and what they're trying to achieve:
#   VCN — Contextual Notification: acquire new VVD cardholders (trigger)
#   VDA — Black Friday: acquire new VVD cardholders (seasonal batch)
#   VDT — Activation Trigger: get issued-but-inactive cards activated (5% need manual activation)
#   VUI — Usage Trigger: get active cardholders to transact (they have the card, not using it)
#   VUT — Tokenization Usage: get cardholders to add card to digital wallet (trigger)
#   VAW — Add To Wallet: same goal as VUT, different channel/timing
#
# Control groups are ORGANIC — they were NOT communicated to.
# Any success in Control happened without campaign influence.
# ====================================================================


# ============================================================
# CELL 1: SETUP
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark import StorageLevel
import base64

def download_excel(sheets_dict, filename):
    """Browser download link for multi-sheet Excel. Falls back to CSVs if openpyxl unavailable."""
    import pandas as pd
    from io import BytesIO
    from IPython.display import display, HTML
    try:
        import openpyxl  # noqa: F401
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            for sheet_name, df in sheets_dict.items():
                pdf = df.toPandas() if hasattr(df, 'toPandas') else df
                pdf.to_excel(writer, sheet_name=sheet_name, index=False)
        b64 = base64.b64encode(buf.getvalue()).decode()
        size_mb = buf.tell() / 1_048_576
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        display(HTML(f'<a href="data:{mime};base64,{b64}" download="{filename}" '
                     f'style="font-size:16px;padding:8px 16px;background:#1a73e8;color:white;'
                     f'text-decoration:none;border-radius:4px">Download {filename}</a> '
                     f'({size_mb:.2f} MB)'))
    except ImportError:
        print("openpyxl not available — falling back to individual CSVs")
        csv_name = filename.replace(".xlsx", "")
        for sheet_name, df in sheets_dict.items():
            safe = sheet_name.replace(" ", "_").lower()
            download_csv(df, f"{csv_name}_{safe}.csv")

def download_csv(df, filename):
    """Browser download link for single CSV."""
    pdf = df.toPandas() if hasattr(df, 'toPandas') else df
    csv_data = pdf.to_csv(index=False)
    b64 = base64.b64encode(csv_data.encode()).decode()
    size_mb = len(csv_data) / 1_048_576
    from IPython.display import display, HTML
    display(HTML(f'<a href="data:text/csv;base64,{b64}" download="{filename}" '
                 f'style="font-size:16px;padding:8px 16px;background:#1a73e8;color:white;'
                 f'text-decoration:none;border-radius:4px">Download {filename}</a> '
                 f'({size_mb:.2f} MB)'))

# Load from HDFS if result_df not in memory
try:
    result_df.count()
    print("result_df already in memory")
except NameError:
    HDFS_PATH = "/user/427966379/vvd_v2_result"
    result_df = spark.read.parquet(HDFS_PATH)
    print(f"Loaded result_df from HDFS: {result_df.count():,} rows")

# Raw POS transactions with amounts
POS_TXN_PATH = "/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT={year}*"
YEARS = [2024, 2025, 2026]

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

# Pre-filter to experiment clients
experiment_clients = result_df.select("CLNT_NO").distinct()
pos_df = pos_df.join(experiment_clients, "CLNT_NO", "left_semi")
pos_df.persist(StorageLevel.MEMORY_AND_DISK)
print(f"POS transactions (experiment clients): {pos_df.count():,}")


# ============================================================
# CELL 2: POST-ACQUISITION SPENDING (VCN, VDA)
# Clients who acquired a card — how much did they spend within
# their treatment window? Action vs Control.
# VCN = contextual trigger (ongoing), VDA = seasonal batch (Black Friday)
# ============================================================

print("=" * 60)
print("POST-ACQUISITION SPENDING (VCN, VDA)")
print("=" * 60)

# Clients who acquired, with their actual treatment window
acq_clients = (
    result_df
    .filter(F.col("MNE").isin(["VCN", "VDA"]))
    .filter(F.col("ACQUISITION_SUCCESS") == 1)
    .select("CLNT_NO", "MNE", "TST_GRP_CD",
            "FIRST_ACQUISITION_SUCCESS_DT", "TREATMT_STRT_DT", "TREATMT_END_DT")
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .dropDuplicates(["CLNT_NO", "MNE"])
)

# Spending after acquisition, bounded by treatment end date
acq_spending = (
    acq_clients
    .join(pos_df, "CLNT_NO", "inner")
    .filter(F.col("TXN_DT") >= F.col("FIRST_ACQUISITION_SUCCESS_DT"))
    .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
    .withColumn("DAYS_SINCE_ACQ", F.datediff(F.col("TXN_DT"), F.col("FIRST_ACQUISITION_SUCCESS_DT")))
)

_acq_agg_cols = [
    F.countDistinct("CLNT_NO").alias("clients_with_spend"),
    F.count("*").alias("txn_count"),
    F.sum("TXN_AMT").alias("total_spend"),
    F.avg("TXN_AMT").alias("avg_txn_amt"),
    (F.count("*") / F.countDistinct("CLNT_NO")).alias("avg_txn_per_client"),
    (F.sum("TXN_AMT") / F.countDistinct("CLNT_NO")).alias("avg_spend_per_client"),
]

# --- Per-cohort ---
acq_summary_cohort = (
    acq_spending
    .groupBy("MNE", "COHORT", "TST_GRP_CD")
    .agg(*_acq_agg_cols)
    .orderBy("MNE", "COHORT", "TST_GRP_CD")
)

acq_total_cohort = acq_clients.groupBy("MNE", "COHORT", "TST_GRP_CD").agg(F.countDistinct("CLNT_NO").alias("total_acquirers"))

acq_result_cohort = (
    acq_total_cohort.join(acq_summary_cohort, ["MNE", "COHORT", "TST_GRP_CD"], "left")
    .fillna(0, subset=["clients_with_spend", "txn_count", "total_spend",
                        "avg_txn_amt", "avg_txn_per_client", "avg_spend_per_client"])
    .withColumn("pct_with_spend", F.col("clients_with_spend") / F.col("total_acquirers") * 100)
    .orderBy("MNE", "COHORT", "TST_GRP_CD")
)

print("\nPost-acquisition spending BY COHORT (within treatment window):")
acq_result_cohort.show(100, truncate=False)

# --- 6-month average (last 6 cohorts per MNE x TST_GRP_CD) ---
# Rank cohorts descending within each MNE, keep last 6
_cohort_window = Window.partitionBy("MNE", "TST_GRP_CD").orderBy(F.col("COHORT").desc())
acq_cohort_ranked = acq_result_cohort.withColumn("_rank", F.row_number().over(_cohort_window))

acq_last6 = acq_cohort_ranked.filter(F.col("_rank") <= 6).drop("_rank")

acq_avg_6m = (
    acq_last6
    .groupBy("MNE", "TST_GRP_CD")
    .agg(
        F.sum("total_acquirers").alias("total_acquirers"),
        F.sum("clients_with_spend").alias("clients_with_spend"),
        F.sum("txn_count").alias("txn_count"),
        F.sum("total_spend").alias("total_spend"),
        F.avg("avg_txn_amt").alias("avg_txn_amt"),
    )
    .withColumn("avg_txn_per_client",
                F.when(F.col("clients_with_spend") > 0,
                       F.col("txn_count") / F.col("clients_with_spend")).otherwise(0))
    .withColumn("avg_spend_per_client",
                F.when(F.col("clients_with_spend") > 0,
                       F.col("total_spend") / F.col("clients_with_spend")).otherwise(0))
    .withColumn("pct_with_spend", F.col("clients_with_spend") / F.col("total_acquirers") * 100)
    .withColumn("COHORT", F.lit("AVG_6M"))
    .select(acq_result_cohort.columns)
)

# Append the 6-month average row to the cohort output
acq_result_cohort = acq_result_cohort.unionByName(acq_avg_6m).orderBy("MNE", "COHORT", "TST_GRP_CD")

print("\n6-month average appended (COHORT=AVG_6M):")
acq_avg_6m.show(20, truncate=False)

# --- Overall (aggregate across cohorts) ---
acq_summary = (
    acq_spending
    .groupBy("MNE", "TST_GRP_CD")
    .agg(*_acq_agg_cols)
    .orderBy("MNE", "TST_GRP_CD")
)

acq_total = acq_clients.groupBy("MNE", "TST_GRP_CD").agg(F.countDistinct("CLNT_NO").alias("total_acquirers"))

acq_result = (
    acq_total.join(acq_summary, ["MNE", "TST_GRP_CD"], "left")
    .fillna(0, subset=["clients_with_spend", "txn_count", "total_spend",
                        "avg_txn_amt", "avg_txn_per_client", "avg_spend_per_client"])
    .withColumn("pct_with_spend", F.col("clients_with_spend") / F.col("total_acquirers") * 100)
    .orderBy("MNE", "TST_GRP_CD")
)

print("\nPost-acquisition spending OVERALL (within treatment window):")
acq_result.show(20, truncate=False)



# ============================================================
# CELL 3: POST-ACTIVATION SPENDING (VDT)
# Only ~5% of cards need manual activation (rest are digital).
# For those who activated, what's their spending pattern?
# ============================================================

print("=" * 60)
print("POST-ACTIVATION SPENDING (VDT)")
print("=" * 60)

act_clients = (
    result_df
    .filter(F.col("MNE") == "VDT")
    .filter(F.col("ACTIVATION_SUCCESS") == 1)
    .select("CLNT_NO", "MNE", "TST_GRP_CD",
            "FIRST_ACTIVATION_SUCCESS_DT", "TREATMT_STRT_DT", "TREATMT_END_DT")
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .dropDuplicates(["CLNT_NO", "MNE"])
)

act_spending = (
    act_clients
    .join(pos_df, "CLNT_NO", "inner")
    .filter(F.col("TXN_DT") >= F.col("FIRST_ACTIVATION_SUCCESS_DT"))
    .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
)

_act_agg_cols = [
    F.countDistinct("CLNT_NO").alias("clients_with_spend"),
    F.count("*").alias("txn_count"),
    F.sum("TXN_AMT").alias("total_spend"),
    F.avg("TXN_AMT").alias("avg_txn_amt"),
    (F.count("*") / F.countDistinct("CLNT_NO")).alias("avg_txn_per_client"),
    (F.sum("TXN_AMT") / F.countDistinct("CLNT_NO")).alias("avg_spend_per_client"),
]

# --- Per-cohort ---
act_summary_cohort = (
    act_spending
    .groupBy("MNE", "COHORT", "TST_GRP_CD")
    .agg(*_act_agg_cols)
)

act_total_cohort = act_clients.groupBy("MNE", "COHORT", "TST_GRP_CD").agg(F.countDistinct("CLNT_NO").alias("total_activators"))

act_result_cohort = (
    act_total_cohort.join(act_summary_cohort, ["MNE", "COHORT", "TST_GRP_CD"], "left")
    .withColumn("pct_with_spend", F.col("clients_with_spend") / F.col("total_activators") * 100)
    .orderBy("MNE", "COHORT", "TST_GRP_CD")
)

print("\nPost-activation spending BY COHORT (within treatment window):")
act_result_cohort.show(100, truncate=False)

# --- Overall ---
act_summary = (
    act_spending
    .groupBy("MNE", "TST_GRP_CD")
    .agg(*_act_agg_cols)
)

act_total = act_clients.groupBy("MNE", "TST_GRP_CD").agg(F.countDistinct("CLNT_NO").alias("total_activators"))

act_result = (
    act_total.join(act_summary, ["MNE", "TST_GRP_CD"], "left")
    .withColumn("pct_with_spend", F.col("clients_with_spend") / F.col("total_activators") * 100)
    .orderBy("TST_GRP_CD")
)

print("\nPost-activation spending OVERALL (within treatment window):")
act_result.show(20, truncate=False)



# ============================================================
# CELL 4: POST-PROVISIONING — BEFORE vs AFTER + DiD (VUT, VAW)
# These clients were ALREADY active cardholders.
# Question: did adding to wallet change their spending?
# Use each client's own treatment window, not hardcoded days.
# DiD = (Action_post - Action_pre) - (Control_post - Control_pre)
# ============================================================

print("=" * 60)
print("POST-PROVISIONING SPENDING — BEFORE vs AFTER (VUT, VAW)")
print("=" * 60)

prov_clients = (
    result_df
    .filter(F.col("MNE").isin(["VUT", "VAW"]))
    .filter(F.col("PROVISIONING_SUCCESS") == 1)
    .select("CLNT_NO", "MNE", "TST_GRP_CD",
            "FIRST_PROVISIONING_SUCCESS_DT", "TREATMT_STRT_DT", "TREATMT_END_DT")
    .withColumn("WINDOW_DAYS", F.datediff(F.col("TREATMT_END_DT"), F.col("TREATMT_STRT_DT")))
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .dropDuplicates(["CLNT_NO", "MNE"])
)

prov_txn = prov_clients.join(pos_df, "CLNT_NO", "inner")

# Pre: same duration before provisioning as treatment window
# F.date_sub doesn't support Column for days arg on CDH Spark — use expr
pre_prov = (
    prov_txn
    .filter(F.col("TXN_DT") < F.col("FIRST_PROVISIONING_SUCCESS_DT"))
    .filter(F.col("TXN_DT") >= F.expr("date_sub(FIRST_PROVISIONING_SUCCESS_DT, WINDOW_DAYS)"))
    .withColumn("PERIOD", F.lit("PRE"))
)

# Post: from provisioning to end of treatment window
post_prov = (
    prov_txn
    .filter(F.col("TXN_DT") >= F.col("FIRST_PROVISIONING_SUCCESS_DT"))
    .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
    .withColumn("PERIOD", F.lit("POST"))
)

prov_combined = pre_prov.unionByName(post_prov)

_prov_agg_cols = [
    F.countDistinct("CLNT_NO").alias("clients"),
    F.count("*").alias("txn_count"),
    F.sum("TXN_AMT").alias("total_spend"),
    F.avg("TXN_AMT").alias("avg_txn_amt"),
    (F.count("*") / F.countDistinct("CLNT_NO")).alias("avg_txn_per_client"),
    (F.sum("TXN_AMT") / F.countDistinct("CLNT_NO")).alias("avg_spend_per_client"),
]

# --- Per-cohort ---
prov_summary_cohort = (
    prov_combined
    .groupBy("MNE", "COHORT", "TST_GRP_CD", "PERIOD")
    .agg(*_prov_agg_cols)
    .orderBy("MNE", "COHORT", "TST_GRP_CD", "PERIOD")
)

print("\nPre vs Post provisioning spending BY COHORT:")
prov_summary_cohort.show(100, truncate=False)

# --- Overall ---
prov_summary = (
    prov_combined
    .groupBy("MNE", "TST_GRP_CD", "PERIOD")
    .agg(*_prov_agg_cols)
    .orderBy("MNE", "TST_GRP_CD", "PERIOD")
)

print("\nPre vs Post provisioning spending OVERALL:")
prov_summary.show(20, truncate=False)

# --- Difference-in-Differences (overall) ---
prov_did = (
    prov_summary
    .groupBy("MNE", "TST_GRP_CD")
    .pivot("PERIOD", ["PRE", "POST"])
    .agg(
        F.first("avg_spend_per_client").alias("spend"),
        F.first("avg_txn_per_client").alias("txn"),
    )
    .withColumn("spend_change", F.col("POST_spend") - F.col("PRE_spend"))
    .withColumn("txn_change", F.col("POST_txn") - F.col("PRE_txn"))
    .orderBy("MNE", "TST_GRP_CD")
)

print("Pre vs Post change by test group (OVERALL):")
prov_did.show(20, truncate=False)

# --- DiD per cohort ---
prov_did_cohort = (
    prov_summary_cohort
    .groupBy("MNE", "COHORT", "TST_GRP_CD")
    .pivot("PERIOD", ["PRE", "POST"])
    .agg(
        F.first("avg_spend_per_client").alias("spend"),
        F.first("avg_txn_per_client").alias("txn"),
    )
    .withColumn("spend_change", F.col("POST_spend") - F.col("PRE_spend"))
    .withColumn("txn_change", F.col("POST_txn") - F.col("PRE_txn"))
    .orderBy("MNE", "COHORT", "TST_GRP_CD")
)

print("Pre vs Post change by test group BY COHORT:")
prov_did_cohort.show(100, truncate=False)

# Compute DiD: Action change minus Control change (overall)
for mne in ["VUT", "VAW"]:
    mne_data = prov_did.filter(F.col("MNE") == mne).collect()
    action = [r for r in mne_data if r.TST_GRP_CD.strip() == "TG4"]
    control = [r for r in mne_data if r.TST_GRP_CD.strip() == "TG7"]
    if action and control:
        a, c = action[0], control[0]
        did_spend = float(a.spend_change or 0) - float(c.spend_change or 0)
        did_txn = float(a.txn_change or 0) - float(c.txn_change or 0)
        print(f"\n{mne} Difference-in-Differences (OVERALL):")
        print(f"  Spend DiD: ${did_spend:,.2f} per client (causal campaign effect)")
        print(f"  Txn DiD:   {did_txn:,.2f} txns per client")
        print(f"  Action pre->post: ${float(a.PRE_spend or 0):,.2f} -> ${float(a.POST_spend or 0):,.2f}")
        print(f"  Control pre->post: ${float(c.PRE_spend or 0):,.2f} -> ${float(c.POST_spend or 0):,.2f}")

# Compute DiD per cohort
for mne in ["VUT", "VAW"]:
    cohort_data = prov_did_cohort.filter(F.col("MNE") == mne).collect()
    cohorts = sorted(set(r.COHORT for r in cohort_data))
    print(f"\n{mne} Difference-in-Differences BY COHORT:")
    for cohort in cohorts:
        action = [r for r in cohort_data if r.COHORT == cohort and r.TST_GRP_CD.strip() == "TG4"]
        control = [r for r in cohort_data if r.COHORT == cohort and r.TST_GRP_CD.strip() == "TG7"]
        if action and control:
            a, c = action[0], control[0]
            did_spend = float(a.spend_change or 0) - float(c.spend_change or 0)
            print(f"  {cohort}: Spend DiD = ${did_spend:,.2f}/client")



# ============================================================
# CELL 5: WALLET PROVISIONING — USAGE COMPARISON
# Among VUT/VAW clients: those who provisioned vs those who didn't.
# Did adding to wallet increase usage? Action vs Control.
# ============================================================

print("\n" + "=" * 60)
print("WALLET PROVISIONING — USAGE LIFT (VUT, VAW)")
print("=" * 60)

wallet_all = (
    result_df
    .filter(F.col("MNE").isin(["VUT", "VAW"]))
    .select("CLNT_NO", "MNE", "TST_GRP_CD",
            "PROVISIONING_SUCCESS", "USAGE_SUCCESS",
            "TREATMT_STRT_DT", "TREATMT_END_DT")
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .dropDuplicates(["CLNT_NO", "MNE"])
)

_wallet_usage_agg = [
    F.count("*").alias("clients"),
    F.sum("USAGE_SUCCESS").alias("usage_successes"),
    (F.sum("USAGE_SUCCESS") / F.count("*") * 100).alias("usage_rate_pct"),
]

# --- Per-cohort ---
wallet_usage_cohort = (
    wallet_all
    .groupBy("MNE", "COHORT", "TST_GRP_CD", "PROVISIONING_SUCCESS")
    .agg(*_wallet_usage_agg)
    .orderBy("MNE", "COHORT", "TST_GRP_CD", "PROVISIONING_SUCCESS")
)

print("\nUsage rate by provisioning status BY COHORT:")
wallet_usage_cohort.show(100, truncate=False)

# --- Overall ---
wallet_usage = (
    wallet_all
    .groupBy("MNE", "TST_GRP_CD", "PROVISIONING_SUCCESS")
    .agg(*_wallet_usage_agg)
    .orderBy("MNE", "TST_GRP_CD", "PROVISIONING_SUCCESS")
)

print("\nUsage rate by provisioning status OVERALL:")
wallet_usage.show(20, truncate=False)

# Spending comparison: provisioned vs not provisioned
wallet_spend = wallet_all.join(pos_df, "CLNT_NO", "inner")
wallet_spend = (
    wallet_spend
    .filter(F.col("TXN_DT") >= F.col("TREATMT_STRT_DT"))
    .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
)

_wallet_spend_agg = [
    F.countDistinct("CLNT_NO").alias("clients_with_spend"),
    F.sum("TXN_AMT").alias("total_spend"),
    (F.sum("TXN_AMT") / F.countDistinct("CLNT_NO")).alias("avg_spend_per_client"),
    (F.count("*") / F.countDistinct("CLNT_NO")).alias("avg_txn_per_client"),
]

# --- Per-cohort ---
wallet_spend_summary_cohort = (
    wallet_spend
    .groupBy("MNE", "COHORT", "TST_GRP_CD", "PROVISIONING_SUCCESS")
    .agg(*_wallet_spend_agg)
    .orderBy("MNE", "COHORT", "TST_GRP_CD", "PROVISIONING_SUCCESS")
)

print("\nSpending by provisioning status BY COHORT (within treatment window):")
wallet_spend_summary_cohort.show(100, truncate=False)

# --- Overall ---
wallet_spend_summary = (
    wallet_spend
    .groupBy("MNE", "TST_GRP_CD", "PROVISIONING_SUCCESS")
    .agg(*_wallet_spend_agg)
    .orderBy("MNE", "TST_GRP_CD", "PROVISIONING_SUCCESS")
)

print("\nSpending by provisioning status OVERALL (within treatment window):")
wallet_spend_summary.show(20, truncate=False)



# ============================================================
# CELL 5b: PER-COHORT SUCCESS SUMMARY WITH SIGNIFICANCE
# Final success rate per MNE x COHORT — the "end of curve" result.
# Uses proportions z-test (scipy if available, manual fallback).
# ============================================================

print("=" * 60)
print("PER-COHORT SUCCESS SUMMARY WITH SIGNIFICANCE")
print("=" * 60)

# Campaign -> primary success column mapping
CAMPAIGN_SUCCESS_FLAG = {
    "VCN": "ACQUISITION_SUCCESS",
    "VDA": "ACQUISITION_SUCCESS",
    "VDT": "ACTIVATION_SUCCESS",
    "VUI": "USAGE_SUCCESS",
    "VUT": "PROVISIONING_SUCCESS",
    "VAW": "PROVISIONING_SUCCESS",
}

# Build per-cohort success rates
cohort_rates = (
    result_df
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .select("CLNT_NO", "MNE", "TST_GRP_CD", "COHORT", "SUCCESS")
    .groupBy("MNE", "COHORT", "TST_GRP_CD")
    .agg(
        F.count("*").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
        (F.sum("SUCCESS") / F.count("*")).alias("success_rate"),
    )
    .orderBy("MNE", "COHORT", "TST_GRP_CD")
)

cohort_rates_collected = cohort_rates.collect()

# Significance testing
import math

try:
    from scipy.stats import proportions_ztest
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False

print(f"scipy available: {_HAS_SCIPY}")

# Group by MNE + COHORT, compute lift + p-value
cohort_summary_rows = []
mne_cohort_pairs = sorted(set((r.MNE, r.COHORT) for r in cohort_rates_collected))

for mne, cohort in mne_cohort_pairs:
    action_rows = [r for r in cohort_rates_collected
                   if r.MNE == mne and r.COHORT == cohort and r.TST_GRP_CD.strip() == "TG4"]
    control_rows = [r for r in cohort_rates_collected
                    if r.MNE == mne and r.COHORT == cohort and r.TST_GRP_CD.strip() == "TG7"]

    if not action_rows or not control_rows:
        continue

    a = action_rows[0]
    c = control_rows[0]

    a_n, a_s = int(a.clients), int(a.successes)
    c_n, c_s = int(c.clients), int(c.successes)
    a_rate = a_s / a_n if a_n > 0 else 0.0
    c_rate = c_s / c_n if c_n > 0 else 0.0
    abs_lift = a_rate - c_rate

    # Significance test
    if _HAS_SCIPY and a_n > 0 and c_n > 0:
        _, p_val = proportions_ztest([a_s, c_s], [a_n, c_n], alternative='two-sided')
    elif a_n > 0 and c_n > 0:
        # Manual z-test fallback
        p_pool = (a_s + c_s) / (a_n + c_n)
        if p_pool > 0 and p_pool < 1:
            se = math.sqrt(p_pool * (1 - p_pool) * (1.0 / a_n + 1.0 / c_n))
            z = abs_lift / se if se > 0 else 0.0
            # Two-tailed p-value approximation
            p_val = 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))
        else:
            p_val = 1.0
    else:
        p_val = 1.0

    sig_flag = "***" if p_val < 0.001 else "**" if p_val < 0.01 else "*" if p_val < 0.05 else ""
    incremental = int(abs_lift * a_n) if abs_lift > 0 else 0

    cohort_summary_rows.append((
        mne, cohort,
        float(a_rate), float(c_rate), float(abs_lift),
        float(p_val), sig_flag,
        a_n, c_n, incremental
    ))

# Build summary DF
if cohort_summary_rows:
    cohort_summary_df = spark.createDataFrame(
        cohort_summary_rows,
        ["MNE", "COHORT", "action_rate", "control_rate", "abs_lift",
         "p_value", "significance", "action_n", "control_n", "incremental_clients"]
    )

    print("\nPer-cohort success summary:")
    cohort_summary_df.orderBy("MNE", "COHORT").show(200, truncate=False)

    # Overall summary (aggregate across cohorts) for comparison
    print("\nOverall success rates (for reference):")
    overall_rates = (
        result_df
        .groupBy("MNE", "TST_GRP_CD")
        .agg(
            F.count("*").alias("clients"),
            F.sum("SUCCESS").alias("successes"),
            (F.sum("SUCCESS") / F.count("*") * 100).alias("success_rate_pct"),
        )
        .orderBy("MNE", "TST_GRP_CD")
    )
    overall_rates.show(20, truncate=False)

else:
    print("No cohort data to summarize.")


# ============================================================
# CELL 6: SPEND VELOCITY CURVES
# Cumulative average spend over days since success.
# Like vintage curves but for dollars, not success rate.
# One curve per campaign x cohort x test group.
# ============================================================

print("=" * 60)
print("SPEND VELOCITY CURVES")
print("=" * 60)

# Build per-campaign: days since success -> cumulative avg spend
CAMPAIGN_SUCCESS_COL = {
    "VCN": "FIRST_ACQUISITION_SUCCESS_DT",
    "VDA": "FIRST_ACQUISITION_SUCCESS_DT",
    "VDT": "FIRST_ACTIVATION_SUCCESS_DT",
    "VUI": "FIRST_USAGE_SUCCESS_DT",
    "VUT": "FIRST_PROVISIONING_SUCCESS_DT",
    "VAW": "FIRST_PROVISIONING_SUCCESS_DT",
}

velocity_parts = []

for mne, date_col in CAMPAIGN_SUCCESS_COL.items():
    # Only clients who succeeded
    mne_clients = (
        result_df
        .filter((F.col("MNE") == mne) & (F.col("SUCCESS") == 1))
        .select("CLNT_NO", "MNE", "TST_GRP_CD", F.col(date_col).alias("SUCCESS_DT"),
                "TREATMT_STRT_DT", "TREATMT_END_DT")
        .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
        .dropDuplicates(["CLNT_NO", "MNE"])
    )

    mne_spend = (
        mne_clients.join(pos_df, "CLNT_NO", "inner")
        .filter(F.col("TXN_DT") >= F.col("SUCCESS_DT"))
        .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
        .withColumn("DAY", F.datediff(F.col("TXN_DT"), F.col("SUCCESS_DT")))
    )

    # Cumulative spend per client per day
    client_daily = (
        mne_spend
        .groupBy("MNE", "COHORT", "TST_GRP_CD", "CLNT_NO", "DAY")
        .agg(F.sum("TXN_AMT").alias("daily_spend"))
    )

    # Total clients per group (denominator — includes those with zero spend)
    total_clients = mne_clients.groupBy("MNE", "COHORT", "TST_GRP_CD").agg(F.countDistinct("CLNT_NO").alias("total_clients"))

    # Cumulative: sum all spend up to each day, divide by total clients
    day_agg = (
        client_daily
        .groupBy("MNE", "COHORT", "TST_GRP_CD", "DAY")
        .agg(F.sum("daily_spend").alias("day_total_spend"))
        .orderBy("DAY")
    )

    # Running cumulative sum — partitioned by cohort too
    w = Window.partitionBy("MNE", "COHORT", "TST_GRP_CD").orderBy("DAY").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    day_cum = day_agg.withColumn("cum_spend", F.sum("day_total_spend").over(w))

    # Join total_clients for per-client average
    day_cum = (
        day_cum.join(total_clients, ["MNE", "COHORT", "TST_GRP_CD"])
        .withColumn("avg_cum_spend_per_client", F.col("cum_spend") / F.col("total_clients"))
        .select("MNE", "COHORT", "TST_GRP_CD", "DAY", "avg_cum_spend_per_client", "total_clients")
    )

    velocity_parts.append(day_cum)

# Union all
velocity_df = velocity_parts[0]
for part in velocity_parts[1:]:
    velocity_df = velocity_df.unionByName(part)

# Show summary at key day milestones
print("\nCumulative avg spend per client at day milestones:")
for milestone in [7, 14, 30, 60, 90]:
    print(f"\n--- Day {milestone} ---")
    (
        velocity_df
        .filter(F.col("DAY") == milestone)
        .orderBy("MNE", "COHORT", "TST_GRP_CD")
        .show(100, truncate=False)
    )

# Save velocity curves for charting
velocity_pd = velocity_df.toPandas()
print(f"\nVelocity curve data: {len(velocity_pd):,} rows")
print("Columns: MNE, COHORT, TST_GRP_CD, DAY, avg_cum_spend_per_client, total_clients")
print("Use this for Plotly/matplotlib line charts (Action vs Control per campaign per cohort)")

# ============================================================
# CELL 6b: VUI PRE/POST SPENDING (NEW — required for Mega Deep Dive)
# VUI nudges existing cardholders to transact more.
# No "success event" — the population IS the denominator.
# PRE: 30 days before treatment start
# POST: treatment start to treatment end
# ============================================================

print("=" * 60)
print("VUI PRE/POST SPENDING")
print("=" * 60)

vui_clients = (
    result_df
    .filter(F.col("MNE") == "VUI")
    .select("CLNT_NO", "MNE", "TST_GRP_CD", "TREATMT_STRT_DT", "TREATMT_END_DT")
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .dropDuplicates(["CLNT_NO", "MNE"])
)

vui_txn = vui_clients.join(pos_df, "CLNT_NO", "inner")

vui_pre = (
    vui_txn
    .filter(F.col("TXN_DT") < F.col("TREATMT_STRT_DT"))
    .filter(F.col("TXN_DT") >= F.expr("date_sub(TREATMT_STRT_DT, 30)"))
    .withColumn("PERIOD", F.lit("PRE"))
)

vui_post = (
    vui_txn
    .filter(F.col("TXN_DT") >= F.col("TREATMT_STRT_DT"))
    .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
    .withColumn("PERIOD", F.lit("POST"))
)

vui_combined = vui_pre.unionByName(vui_post)

_vui_agg_cols = [
    F.countDistinct("CLNT_NO").alias("clients_with_spend"),
    F.count("*").alias("txn_count"),
    F.sum("TXN_AMT").alias("total_spend"),
    (F.count("*") / F.countDistinct("CLNT_NO")).alias("avg_txn_per_client"),
    (F.sum("TXN_AMT") / F.countDistinct("CLNT_NO")).alias("avg_spend_per_client"),
]

vui_summary_cohort = (
    vui_combined
    .groupBy("MNE", "COHORT", "TST_GRP_CD", "PERIOD")
    .agg(*_vui_agg_cols)
    .orderBy("MNE", "COHORT", "TST_GRP_CD", "PERIOD")
)

vui_total_cohort = (
    vui_clients
    .groupBy("MNE", "COHORT", "TST_GRP_CD")
    .agg(F.countDistinct("CLNT_NO").alias("total_clients"))
)

print("\nVUI pre/post spending BY COHORT:")
vui_summary_cohort.show(100, truncate=False)
print("\nVUI total clients BY COHORT:")
vui_total_cohort.show(50, truncate=False)


# ============================================================
# CELL 6c: MEGA DEEP DIVE OUTPUT
# Unified DataFrame: ALL 6 campaigns, ALL cohorts, PRE/POST.
# One row per MNE x COHORT x TST_GRP_CD x PERIOD.
# ============================================================

print("=" * 60)
print("MEGA DEEP DIVE — UNIFIED OUTPUT")
print("=" * 60)

# --- VCN (POST only, from acq_result_cohort) ---
# acq_result_cohort has AVG_6M rows already — exclude them, we'll recompute
vcn_mega = (
    acq_result_cohort
    .filter((F.col("MNE") == "VCN") & (F.col("COHORT") != "AVG_6M"))
    .select(
        F.col("MNE"),
        F.col("COHORT"),
        F.col("TST_GRP_CD"),
        F.lit("POST").alias("PERIOD"),
        F.col("total_acquirers").alias("success_count"),
        F.col("clients_with_spend"),
        F.col("pct_with_spend"),
        F.col("txn_count"),
        F.col("total_spend"),
        F.col("avg_spend_per_client"),
        F.col("avg_txn_per_client"),
    )
)

# --- VDA (POST only, from acq_result_cohort) ---
vda_mega = (
    acq_result_cohort
    .filter((F.col("MNE") == "VDA") & (F.col("COHORT") != "AVG_6M"))
    .select(
        F.col("MNE"),
        F.col("COHORT"),
        F.col("TST_GRP_CD"),
        F.lit("POST").alias("PERIOD"),
        F.col("total_acquirers").alias("success_count"),
        F.col("clients_with_spend"),
        F.col("pct_with_spend"),
        F.col("txn_count"),
        F.col("total_spend"),
        F.col("avg_spend_per_client"),
        F.col("avg_txn_per_client"),
    )
)

# --- VDT (POST only, from act_result_cohort) ---
vdt_mega = (
    act_result_cohort
    .select(
        F.col("MNE"),
        F.col("COHORT"),
        F.col("TST_GRP_CD"),
        F.lit("POST").alias("PERIOD"),
        F.col("total_activators").alias("success_count"),
        F.col("clients_with_spend"),
        F.col("pct_with_spend"),
        F.col("txn_count"),
        F.col("total_spend"),
        F.col("avg_spend_per_client"),
        F.col("avg_txn_per_client"),
    )
)

# --- VUI (PRE and POST, from vui_summary_cohort + vui_total_cohort) ---
# VUI success_count = total experiment clients (not a success event)
vui_mega = (
    vui_summary_cohort
    .join(vui_total_cohort, ["MNE", "COHORT", "TST_GRP_CD"], "left")
    .select(
        F.col("MNE"),
        F.col("COHORT"),
        F.col("TST_GRP_CD"),
        F.col("PERIOD"),
        F.col("total_clients").alias("success_count"),
        F.col("clients_with_spend"),
        (F.col("clients_with_spend") / F.col("total_clients") * 100).alias("pct_with_spend"),
        F.col("txn_count"),
        F.col("total_spend"),
        F.col("avg_spend_per_client"),
        F.col("avg_txn_per_client"),
    )
)

# --- VUT (PRE and POST, from prov_summary_cohort + prov_clients count) ---
# prov_summary_cohort has "clients" = clients with spend, need provisioned_clients total
prov_total_cohort = (
    prov_clients
    .groupBy("MNE", "COHORT", "TST_GRP_CD")
    .agg(F.countDistinct("CLNT_NO").alias("provisioned_clients"))
)

vut_mega = (
    prov_summary_cohort
    .filter(F.col("MNE") == "VUT")
    .join(prov_total_cohort.filter(F.col("MNE") == "VUT"), ["MNE", "COHORT", "TST_GRP_CD"], "left")
    .select(
        F.col("MNE"),
        F.col("COHORT"),
        F.col("TST_GRP_CD"),
        F.col("PERIOD"),
        F.col("provisioned_clients").alias("success_count"),
        F.col("clients").alias("clients_with_spend"),
        (F.col("clients") / F.col("provisioned_clients") * 100).alias("pct_with_spend"),
        F.col("txn_count"),
        F.col("total_spend"),
        F.col("avg_spend_per_client"),
        F.col("avg_txn_per_client"),
    )
)

# --- VAW (PRE and POST, from prov_summary_cohort + prov_clients count) ---
vaw_mega = (
    prov_summary_cohort
    .filter(F.col("MNE") == "VAW")
    .join(prov_total_cohort.filter(F.col("MNE") == "VAW"), ["MNE", "COHORT", "TST_GRP_CD"], "left")
    .select(
        F.col("MNE"),
        F.col("COHORT"),
        F.col("TST_GRP_CD"),
        F.col("PERIOD"),
        F.col("provisioned_clients").alias("success_count"),
        F.col("clients").alias("clients_with_spend"),
        (F.col("clients") / F.col("provisioned_clients") * 100).alias("pct_with_spend"),
        F.col("txn_count"),
        F.col("total_spend"),
        F.col("avg_spend_per_client"),
        F.col("avg_txn_per_client"),
    )
)

# --- UNION ALL ---
mega_deep = (
    vcn_mega
    .unionByName(vda_mega)
    .unionByName(vdt_mega)
    .unionByName(vui_mega)
    .unionByName(vut_mega)
    .unionByName(vaw_mega)
)

# --- AVG_6M summary rows ---
# For each MNE x TST_GRP_CD x PERIOD, rank cohorts descending, take last 6, weighted avg
_mega_window = Window.partitionBy("MNE", "TST_GRP_CD", "PERIOD").orderBy(F.col("COHORT").desc())
mega_ranked = mega_deep.withColumn("_rank", F.row_number().over(_mega_window))
mega_last6 = mega_ranked.filter(F.col("_rank") <= 6).drop("_rank")

mega_avg_6m = (
    mega_last6
    .groupBy("MNE", "TST_GRP_CD", "PERIOD")
    .agg(
        F.sum("success_count").alias("success_count"),
        F.sum("clients_with_spend").alias("clients_with_spend"),
        F.sum("txn_count").alias("txn_count"),
        F.sum("total_spend").alias("total_spend"),
    )
    .withColumn("pct_with_spend",
                F.when(F.col("success_count") > 0,
                       F.col("clients_with_spend") / F.col("success_count") * 100).otherwise(0))
    .withColumn("avg_spend_per_client",
                F.when(F.col("clients_with_spend") > 0,
                       F.col("total_spend") / F.col("clients_with_spend")).otherwise(0))
    .withColumn("avg_txn_per_client",
                F.when(F.col("clients_with_spend") > 0,
                       F.col("txn_count") / F.col("clients_with_spend")).otherwise(0))
    .withColumn("COHORT", F.lit("AVG_6M"))
    .select("MNE", "COHORT", "TST_GRP_CD", "PERIOD",
            "success_count", "clients_with_spend", "pct_with_spend",
            "txn_count", "total_spend", "avg_spend_per_client", "avg_txn_per_client")
)

# Append AVG_6M and sort
mega_deep = (
    mega_deep
    .unionByName(mega_avg_6m)
    .fillna(0, subset=["success_count", "clients_with_spend", "pct_with_spend",
                        "txn_count", "total_spend", "avg_spend_per_client", "avg_txn_per_client"])
    .orderBy("MNE", "PERIOD", "COHORT", "TST_GRP_CD")
)

# --- VERIFICATION ---
print("\nMega Deep Dive columns:")
print(mega_deep.columns)

print("\nCampaign x Period breakdown:")
(
    mega_deep
    .filter(F.col("COHORT") != "AVG_6M")
    .groupBy("MNE", "PERIOD")
    .agg(F.countDistinct("COHORT").alias("cohorts"), F.sum("success_count").alias("total_success"))
    .orderBy("MNE", "PERIOD")
    .show(20, truncate=False)
)

print("\nAVG_6M rows:")
mega_deep.filter(F.col("COHORT") == "AVG_6M").show(30, truncate=False)

print(f"\nMega Deep Dive total rows: {mega_deep.count()}")
mega_deep.show(200, truncate=False)

# --- EXPORT ---
download_csv(mega_deep, "vvd_v2_mega_deep_dive.csv")


# ── EXPORT ALL RESULTS ──
deep_analysis_sheets = {}

# Cell 2 — Post-acquisition
if 'acq_result' in dir(): deep_analysis_sheets["Post-Acq Spending"] = acq_result
if 'acq_result_cohort' in dir(): deep_analysis_sheets["Post-Acq Cohort"] = acq_result_cohort

# Cell 3 — Post-activation
if 'act_result' in dir(): deep_analysis_sheets["Post-Act Spending"] = act_result
if 'act_result_cohort' in dir(): deep_analysis_sheets["Post-Act Cohort"] = act_result_cohort

# Cell 4 — DiD
if 'prov_summary_cohort' in dir(): deep_analysis_sheets["DiD Cohort"] = prov_summary_cohort
if 'prov_did' in dir(): deep_analysis_sheets["DiD Summary"] = prov_did
if 'prov_did_cohort' in dir(): deep_analysis_sheets["DiD Pivoted"] = prov_did_cohort

# Cell 5 — Wallet usage
if 'wallet_usage' in dir(): deep_analysis_sheets["Wallet Usage"] = wallet_usage
if 'wallet_usage_cohort' in dir(): deep_analysis_sheets["Wallet Usage Cohort"] = wallet_usage_cohort
if 'wallet_spend_summary' in dir(): deep_analysis_sheets["Wallet Spend"] = wallet_spend_summary
if 'wallet_spend_summary_cohort' in dir(): deep_analysis_sheets["Wallet Spend Cohort"] = wallet_spend_summary_cohort

# Cell 5b — Cohort summary
if 'cohort_summary_df' in dir(): deep_analysis_sheets["Cohort Summary"] = cohort_summary_df
if 'overall_rates' in dir(): deep_analysis_sheets["Overall Rates"] = overall_rates

# Cell 6 — Velocity
if 'velocity_pd' in dir(): deep_analysis_sheets["Spend Velocity"] = velocity_pd

# Cell 6b — VUI
if 'vui_summary_cohort' in dir(): deep_analysis_sheets["VUI Pre-Post Cohort"] = vui_summary_cohort

# Cell 6c — Mega Deep Dive
if 'mega_deep' in dir(): deep_analysis_sheets["Mega Deep Dive"] = mega_deep

if deep_analysis_sheets:
    download_excel(deep_analysis_sheets, "vvd_v2_deep_analysis.xlsx")


# ============================================================
# CELL 7: CLEANUP
# ============================================================

pos_df.unpersist()
print("pos_df unpersisted.")
