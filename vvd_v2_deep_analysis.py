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
pos_df.persist()
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

# Total acquirers (including those with zero spend)
acq_total = acq_clients.groupBy("MNE", "TST_GRP_CD").agg(F.countDistinct("CLNT_NO").alias("total_acquirers"))

acq_result = (
    acq_total.join(acq_summary, ["MNE", "TST_GRP_CD"], "left")
    .withColumn("pct_with_spend", F.col("clients_with_spend") / F.col("total_acquirers") * 100)
    .orderBy("MNE", "TST_GRP_CD")
)

print("\nPost-acquisition spending (within treatment window):")
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
    .dropDuplicates(["CLNT_NO", "MNE"])
)

act_spending = (
    act_clients
    .join(pos_df, "CLNT_NO", "inner")
    .filter(F.col("TXN_DT") >= F.col("FIRST_ACTIVATION_SUCCESS_DT"))
    .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
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
)

act_total = act_clients.groupBy("MNE", "TST_GRP_CD").agg(F.countDistinct("CLNT_NO").alias("total_activators"))

act_result = (
    act_total.join(act_summary, ["MNE", "TST_GRP_CD"], "left")
    .withColumn("pct_with_spend", F.col("clients_with_spend") / F.col("total_activators") * 100)
    .orderBy("TST_GRP_CD")
)

print("\nPost-activation spending (within treatment window):")
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
    .dropDuplicates(["CLNT_NO", "MNE"])
)

prov_txn = prov_clients.join(pos_df, "CLNT_NO", "inner")

# Pre: same duration before provisioning as treatment window
pre_prov = (
    prov_txn
    .filter(F.col("TXN_DT") < F.col("FIRST_PROVISIONING_SUCCESS_DT"))
    .filter(F.col("TXN_DT") >= F.date_sub(F.col("FIRST_PROVISIONING_SUCCESS_DT"), F.col("WINDOW_DAYS")))
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

prov_summary = (
    prov_combined
    .groupBy("MNE", "TST_GRP_CD", "PERIOD")
    .agg(
        F.countDistinct("CLNT_NO").alias("clients"),
        F.count("*").alias("txn_count"),
        F.sum("TXN_AMT").alias("total_spend"),
        F.avg("TXN_AMT").alias("avg_txn_amt"),
        (F.count("*") / F.countDistinct("CLNT_NO")).alias("avg_txn_per_client"),
        (F.sum("TXN_AMT") / F.countDistinct("CLNT_NO")).alias("avg_spend_per_client"),
    )
    .orderBy("MNE", "TST_GRP_CD", "PERIOD")
)

print("\nPre vs Post provisioning spending:")
prov_summary.show(20, truncate=False)

# --- Difference-in-Differences ---
# Pivot to get PRE/POST side by side, then compute DiD
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

print("Pre vs Post change by test group:")
prov_did.show(20, truncate=False)

# Compute DiD: Action change minus Control change
for mne in ["VUT", "VAW"]:
    mne_data = prov_did.filter(F.col("MNE") == mne).collect()
    action = [r for r in mne_data if r.TST_GRP_CD.strip() == "TG4"]
    control = [r for r in mne_data if r.TST_GRP_CD.strip() == "TG7"]
    if action and control:
        a, c = action[0], control[0]
        did_spend = float(a.spend_change or 0) - float(c.spend_change or 0)
        did_txn = float(a.txn_change or 0) - float(c.txn_change or 0)
        print(f"\n{mne} Difference-in-Differences:")
        print(f"  Spend DiD: ${did_spend:,.2f} per client (causal campaign effect)")
        print(f"  Txn DiD:   {did_txn:,.2f} txns per client")
        print(f"  Action pre→post: ${float(a.PRE_spend or 0):,.2f} → ${float(a.POST_spend or 0):,.2f}")
        print(f"  Control pre→post: ${float(c.PRE_spend or 0):,.2f} → ${float(c.POST_spend or 0):,.2f}")


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
    .dropDuplicates(["CLNT_NO", "MNE"])
)

# Usage rate by provisioning status
wallet_usage = (
    wallet_all
    .groupBy("MNE", "TST_GRP_CD", "PROVISIONING_SUCCESS")
    .agg(
        F.count("*").alias("clients"),
        F.sum("USAGE_SUCCESS").alias("usage_successes"),
        (F.sum("USAGE_SUCCESS") / F.count("*") * 100).alias("usage_rate_pct"),
    )
    .orderBy("MNE", "TST_GRP_CD", "PROVISIONING_SUCCESS")
)

print("\nUsage rate by provisioning status:")
wallet_usage.show(20, truncate=False)

# Spending comparison: provisioned vs not provisioned
wallet_spend = wallet_all.join(pos_df, "CLNT_NO", "inner")
wallet_spend = (
    wallet_spend
    .filter(F.col("TXN_DT") >= F.col("TREATMT_STRT_DT"))
    .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
)

wallet_spend_summary = (
    wallet_spend
    .groupBy("MNE", "TST_GRP_CD", "PROVISIONING_SUCCESS")
    .agg(
        F.countDistinct("CLNT_NO").alias("clients_with_spend"),
        F.sum("TXN_AMT").alias("total_spend"),
        (F.sum("TXN_AMT") / F.countDistinct("CLNT_NO")).alias("avg_spend_per_client"),
        (F.count("*") / F.countDistinct("CLNT_NO")).alias("avg_txn_per_client"),
    )
    .orderBy("MNE", "TST_GRP_CD", "PROVISIONING_SUCCESS")
)

print("\nSpending by provisioning status (within treatment window):")
wallet_spend_summary.show(20, truncate=False)


# ============================================================
# CELL 6: SPEND VELOCITY CURVES
# Cumulative average spend over days since success.
# Like vintage curves but for dollars, not success rate.
# One curve per campaign x test group.
# ============================================================

print("=" * 60)
print("SPEND VELOCITY CURVES")
print("=" * 60)

# Build per-campaign: days since success → cumulative avg spend
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
        .select("CLNT_NO", "MNE", "TST_GRP_CD", F.col(date_col).alias("SUCCESS_DT"), "TREATMT_END_DT")
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
        .groupBy("MNE", "TST_GRP_CD", "CLNT_NO", "DAY")
        .agg(F.sum("TXN_AMT").alias("daily_spend"))
    )

    # Total clients per group (denominator — includes those with zero spend)
    total_clients = mne_clients.groupBy("MNE", "TST_GRP_CD").agg(F.countDistinct("CLNT_NO").alias("total_clients"))

    # Cumulative: sum all spend up to each day, divide by total clients
    day_agg = (
        client_daily
        .groupBy("MNE", "TST_GRP_CD", "DAY")
        .agg(F.sum("daily_spend").alias("day_total_spend"))
        .orderBy("DAY")
    )

    # Running cumulative sum
    w = Window.partitionBy("MNE", "TST_GRP_CD").orderBy("DAY").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    day_cum = day_agg.withColumn("cum_spend", F.sum("day_total_spend").over(w))

    # Join total_clients for per-client average
    day_cum = (
        day_cum.join(total_clients, ["MNE", "TST_GRP_CD"])
        .withColumn("avg_cum_spend_per_client", F.col("cum_spend") / F.col("total_clients"))
        .select("MNE", "TST_GRP_CD", "DAY", "avg_cum_spend_per_client", "total_clients")
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
        .orderBy("MNE", "TST_GRP_CD")
        .show(20, truncate=False)
    )

# Save velocity curves for charting
velocity_pd = velocity_df.toPandas()
print(f"\nVelocity curve data: {len(velocity_pd):,} rows")
print("Columns: MNE, TST_GRP_CD, DAY, avg_cum_spend_per_client, total_clients")
print("Use this for Plotly/matplotlib line charts (Action vs Control per campaign)")


# ============================================================
# CELL 7: CLEANUP
# ============================================================

pos_df.unpersist()
print("pos_df unpersisted.")
