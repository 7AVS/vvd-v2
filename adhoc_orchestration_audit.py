#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================================================================
# Orchestration Audit — Response Analysis
#
# Computes the three must-have analyses flagged by the audit:
# 1. Cumulative conversion distribution (settles VCN cap debate)
# 2. Already-succeeded waste (the uncomputed centerpiece)
# 3. Frequency bucket × campaign (proves 4-5 composition)
# 4. Transition stats excluding self-loops (reframes 0.9%)
#
# Reads result_df from HDFS. Each cell exports its own CSV.
# ====================================================================


# ============================================================
# CELL 1: SETUP
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import base64

try:
    from IPython.display import display, HTML
except ImportError:
    pass

HDFS_PATH = "/user/427966379/vvd_v2_result"
result_df = spark.read.parquet(HDFS_PATH)
result_df.persist()
print(f"Loaded result_df: {result_df.count():,} rows")

ACTION_GROUP = "TG4"
CONTROL_GROUP = "TG7"

NIBT_PER_CLIENT = 78.21

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


# ============================================================
# CELL 2: CUMULATIVE CONVERSION DISTRIBUTION
# For each campaign: what % of all conversions happen at each
# contact number? Settles: "Why cap VCN at N and not M?"
# ============================================================

action_df = result_df.filter(F.col("TST_GRP_CD") == ACTION_GROUP)

w_seq = Window.partitionBy("CLNT_NO", "MNE").orderBy("TREATMT_STRT_DT")
action_seq = action_df.withColumn("contact_seq", F.row_number().over(w_seq))

seq_stats = (
    action_seq
    .groupBy("MNE", "contact_seq")
    .agg(
        F.count("*").alias("deployments"),
        F.sum("SUCCESS").alias("successes"),
    )
    .withColumn("success_rate_pct",
        F.round(F.col("successes") / F.col("deployments") * 100, 4))
)

w_cum = (Window.partitionBy("MNE").orderBy("contact_seq")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))
w_total = (Window.partitionBy("MNE")
           .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

cum_dist = (
    seq_stats
    .withColumn("cum_successes", F.sum("successes").over(w_cum))
    .withColumn("total_successes", F.sum("successes").over(w_total))
    .withColumn("cum_conversion_pct",
        F.round(F.sum("successes").over(w_cum) / F.sum("successes").over(w_total) * 100, 2))
    .withColumn("cum_deployments", F.sum("deployments").over(w_cum))
    .withColumn("total_deployments", F.sum("deployments").over(w_total))
    .withColumn("cum_deployment_pct",
        F.round(F.sum("deployments").over(w_cum) / F.sum("deployments").over(w_total) * 100, 2))
    .select("MNE", "contact_seq", "deployments", "successes", "success_rate_pct",
            "cum_successes", "total_successes", "cum_conversion_pct",
            "cum_deployments", "total_deployments", "cum_deployment_pct")
    .orderBy("MNE", "contact_seq")
)

cum_dist_pd = cum_dist.toPandas()

# VCN detail (the key finding for the cap debate)
vcn = cum_dist_pd[cum_dist_pd["MNE"] == "VCN"]
print("=" * 75)
print("VCN CUMULATIVE CONVERSION DISTRIBUTION")
print("=" * 75)
print(f"{'Contact#':>8} {'Deploys':>10} {'Successes':>10} {'Rate%':>8} "
      f"{'Cum%Conv':>10} {'Cum%Deploy':>11}")
print("-" * 62)
for _, r in vcn.iterrows():
    print(f"{int(r.contact_seq):>8} {int(r.deployments):>10,} {int(r.successes):>10,} "
          f"{r.success_rate_pct:>7.2f}% {r.cum_conversion_pct:>9.1f}% "
          f"{r.cum_deployment_pct:>10.1f}%")

# Cap impact summary for all campaigns
print(f"\n{'='*75}")
print("CAP IMPACT SUMMARY — % of conversions captured at each cap level")
print("=" * 75)
print(f"{'Campaign':<8} {'Total':>8} {'@Cap=3':>8} {'@Cap=5':>8} "
      f"{'@Cap=7':>8} {'@Cap=10':>8}")
print("-" * 50)

for mne in sorted(cum_dist_pd["MNE"].unique()):
    mne_df = cum_dist_pd[cum_dist_pd["MNE"] == mne]
    total = int(mne_df["total_successes"].iloc[0])
    caps = {}
    for cap in [3, 5, 7, 10]:
        row = mne_df[mne_df["contact_seq"] == cap]
        if not row.empty:
            caps[cap] = f"{float(row['cum_conversion_pct'].iloc[0]):.1f}%"
        else:
            caps[cap] = "100%"
    print(f"{mne:<8} {total:>8,} {caps[3]:>8} {caps[5]:>8} "
          f"{caps[7]:>8} {caps[10]:>8}")

download_csv(cum_dist_pd, "vvd_v2_cumulative_conversions.csv")


# ============================================================
# CELL 3: ALREADY-SUCCEEDED WASTE
# Clients who already achieved their campaign's goal but keep
# getting contacted by the same campaign. Split by channel.
# ============================================================

w_first_success = (
    Window.partitionBy("CLNT_NO", "MNE")
    .orderBy("TREATMT_STRT_DT")
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

tagged = (
    action_df
    .withColumn("first_success_dt",
        F.min(F.when(F.col("SUCCESS") == 1, F.col("TREATMT_STRT_DT")))
         .over(w_first_success))
)

post_success = tagged.filter(
    (F.col("first_success_dt").isNotNull()) &
    (F.col("TREATMT_STRT_DT") > F.col("first_success_dt"))
)

# By campaign
waste_by_mne = (
    post_success.groupBy("MNE").agg(
        F.countDistinct("CLNT_NO").alias("clients_wasted"),
        F.count("*").alias("wasted_deployments"),
        F.round(F.count("*") / F.countDistinct("CLNT_NO"), 1).alias("avg_wasted_per_client")
    )
    .orderBy(F.desc("wasted_deployments"))
)

print("=" * 75)
print("ALREADY-SUCCEEDED WASTE — Deployments after client's first success")
print("=" * 75)
waste_by_mne.show(truncate=False)

# By campaign × channel (severity split)
waste_by_channel = (
    post_success.groupBy("MNE", "TACTIC_CELL_CD").agg(
        F.countDistinct("CLNT_NO").alias("clients_wasted"),
        F.count("*").alias("wasted_deployments"),
    )
    .orderBy("MNE", F.desc("wasted_deployments"))
)

print("WASTE BY CAMPAIGN × CHANNEL (severity context)")
print("-" * 55)
waste_by_channel.show(50, truncate=False)

# Total successful clients per campaign (denominator context)
success_clients = (
    action_df
    .filter(F.col("SUCCESS") == 1)
    .groupBy("MNE")
    .agg(F.countDistinct("CLNT_NO").alias("total_success_clients"))
)

# Join to show % of successful clients who got wasted contacts
waste_context = (
    waste_by_mne
    .join(success_clients, "MNE")
    .withColumn("pct_success_clients_wasted",
        F.round(F.col("clients_wasted") / F.col("total_success_clients") * 100, 1))
    .orderBy(F.desc("wasted_deployments"))
)

print("WASTE IN CONTEXT — What % of successful clients got post-success contacts?")
print("-" * 70)
waste_context.show(truncate=False)

# Combine into one export
waste_detail_pd = (
    post_success
    .groupBy("MNE", "TACTIC_CELL_CD", "COHORT")
    .agg(
        F.countDistinct("CLNT_NO").alias("clients_wasted"),
        F.count("*").alias("wasted_deployments"),
    )
    .orderBy("MNE", "TACTIC_CELL_CD", "COHORT")
    .toPandas()
)

download_csv(waste_detail_pd, "vvd_v2_post_success_waste.csv")

# Summary for the brief — channel-weighted severity
waste_summary_pd = waste_by_channel.toPandas()
em_waste = waste_summary_pd[
    waste_summary_pd["TACTIC_CELL_CD"].str.contains("EM", na=False)
]["wasted_deployments"].sum()
mb_waste = waste_summary_pd[
    waste_summary_pd["TACTIC_CELL_CD"].str.contains("MB", na=False)
]["wasted_deployments"].sum()
total_waste = waste_summary_pd["wasted_deployments"].sum()

print(f"\nSUMMARY:")
print(f"  Total post-success wasted deployments: {int(total_waste):,}")
print(f"  Email channel (HIGH severity):         {int(em_waste):,} ({em_waste/total_waste*100:.1f}%)")
print(f"  MB channel (LOW severity):             {int(mb_waste):,} ({mb_waste/total_waste*100:.1f}%)")


# ============================================================
# CELL 4: FREQUENCY BUCKET × CAMPAIGN COMPOSITION
# Proves the 4-5 contact bucket's high success rate is VDT
# composition, not a genuine "sweet spot."
# ============================================================

# Per client: total contacts across all campaigns + per-campaign contacts
client_total = (
    action_df
    .groupBy("CLNT_NO")
    .agg(
        F.count("*").alias("total_contacts"),
        F.max("SUCCESS").alias("any_success")
    )
    .withColumn("bucket",
        F.when(F.col("total_contacts") == 1, "01")
         .when(F.col("total_contacts") == 2, "02")
         .when(F.col("total_contacts") == 3, "03")
         .when(F.col("total_contacts").between(4, 5), "04-05")
         .when(F.col("total_contacts").between(6, 10), "06-10")
         .otherwise("11+")
    )
)

# Which campaigns are these clients in?
client_campaigns = (
    action_df
    .select("CLNT_NO", "MNE", "SUCCESS")
    .groupBy("CLNT_NO", "MNE")
    .agg(
        F.count("*").alias("campaign_contacts"),
        F.max("SUCCESS").alias("campaign_success")
    )
)

# Join: for each client in each bucket, which campaigns do they belong to?
bucket_campaign = (
    client_total.select("CLNT_NO", "bucket")
    .join(client_campaigns, "CLNT_NO")
)

# Composition: bucket × campaign → client count and success rate
composition = (
    bucket_campaign
    .groupBy("bucket", "MNE")
    .agg(
        F.countDistinct("CLNT_NO").alias("clients"),
        F.sum("campaign_success").alias("successes"),
    )
    .withColumn("success_rate_pct",
        F.round(F.col("successes") / F.col("clients") * 100, 2))
    .orderBy("bucket", F.desc("clients"))
)

composition_pd = composition.toPandas()

print("=" * 75)
print("FREQUENCY BUCKET × CAMPAIGN COMPOSITION")
print("=" * 75)

for bucket in sorted(composition_pd["bucket"].unique()):
    b_df = composition_pd[composition_pd["bucket"] == bucket]
    total_in_bucket = b_df["clients"].sum()
    print(f"\nBucket: {bucket} ({total_in_bucket:,} clients)")
    print(f"  {'Campaign':<8} {'Clients':>10} {'% of Bucket':>12} {'Success%':>10}")
    print(f"  {'-'*44}")
    for _, r in b_df.iterrows():
        pct = r["clients"] / total_in_bucket * 100
        print(f"  {r['MNE']:<8} {int(r['clients']):>10,} {pct:>11.1f}% "
              f"{r['success_rate_pct']:>9.2f}%")

download_csv(composition_pd, "vvd_v2_frequency_by_campaign.csv")


# ============================================================
# CELL 5: TRANSITION STATS — WITH AND WITHOUT SELF-LOOPS
# Reframes the "0.9% cross-stage" stat honestly.
# ============================================================

FUNNEL_STAGE = {
    "VCN": 1, "VDA": 1,  # Acquisition
    "VDT": 2,             # Activation
    "VUI": 3,             # Usage
    "VUT": 4, "VAW": 4,   # Provisioning
}

w_all = Window.partitionBy("CLNT_NO").orderBy("TREATMT_STRT_DT")
transitions = (
    action_df
    .withColumn("prev_mne", F.lag("MNE").over(w_all))
    .filter(F.col("prev_mne").isNotNull())
)

# Classify each transition
transitions_classified = (
    transitions
    .withColumn("from_stage", F.col("prev_mne"))
    .withColumn("to_stage", F.col("MNE"))
)

# Collect and classify in Python for flexibility
trans_raw = (
    transitions
    .groupBy("prev_mne", "MNE")
    .agg(
        F.count("*").alias("transitions"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.round(F.avg("gap_days") if "gap_days" in transitions.columns else F.lit(0), 1).alias("avg_gap")
    )
    .toPandas()
) if "gap_days" in transitions.columns else None

# Recompute with gap
transitions_with_gap = (
    transitions
    .withColumn("gap_days",
        F.datediff(F.col("TREATMT_STRT_DT"),
                   F.lag("TREATMT_STRT_DT").over(w_all)))
)

trans_raw = (
    transitions_with_gap
    .groupBy("prev_mne", "MNE")
    .agg(
        F.count("*").alias("transitions"),
        F.countDistinct("CLNT_NO").alias("clients"),
        F.round(F.avg("gap_days"), 1).alias("avg_gap_days")
    )
    .toPandas()
)

# Classify
def classify_transition(from_mne, to_mne):
    if from_mne == to_mne:
        return "Self-loop"
    from_s = FUNNEL_STAGE.get(from_mne, 0)
    to_s = FUNNEL_STAGE.get(to_mne, 0)
    if from_s == to_s:
        return "Lateral (same stage)"
    elif to_s > from_s:
        return "Forward"
    else:
        return "Backward"

trans_raw["transition_type"] = trans_raw.apply(
    lambda r: classify_transition(str(r["prev_mne"]), str(r["MNE"])), axis=1)
trans_raw["from_stage"] = trans_raw["prev_mne"].map(FUNNEL_STAGE)
trans_raw["to_stage"] = trans_raw["MNE"].map(FUNNEL_STAGE)

total_all = trans_raw["transitions"].sum()
total_no_self = trans_raw[trans_raw["transition_type"] != "Self-loop"]["transitions"].sum()

print("=" * 75)
print("TRANSITION CLASSIFICATION")
print("=" * 75)

type_summary = (
    trans_raw.groupby("transition_type")["transitions"]
    .sum().reset_index()
    .sort_values("transitions", ascending=False)
)

print(f"\n{'Type':<25} {'Transitions':>12} {'% of All':>9} {'% ex Self-Loop':>15}")
print("-" * 65)
for _, r in type_summary.iterrows():
    pct_all = r["transitions"] / total_all * 100
    pct_no_self = (r["transitions"] / total_no_self * 100
                   if r["transition_type"] != "Self-loop" else "---")
    pct_str = f"{pct_no_self:>14.1f}%" if isinstance(pct_no_self, float) else f"{pct_no_self:>15}"
    print(f"{r['transition_type']:<25} {int(r['transitions']):>12,} {pct_all:>8.1f}% {pct_str}")

print(f"\nTotal transitions:              {int(total_all):,}")
print(f"Total excluding self-loops:     {int(total_no_self):,}")

# Cross-stage detail
cross = trans_raw[trans_raw["transition_type"].isin(["Forward", "Backward"])]
cross = cross.sort_values("transitions", ascending=False)

print(f"\nCROSS-STAGE TRANSITIONS (Forward + Backward):")
print(f"{'From':<6} {'To':<6} {'Type':<10} {'Transitions':>12} {'Clients':>10}")
print("-" * 48)
for _, r in cross.iterrows():
    print(f"{r['prev_mne']:<6} {r['MNE']:<6} {r['transition_type']:<10} "
          f"{int(r['transitions']):>12,} {int(r['clients']):>10,}")

download_csv(trans_raw, "vvd_v2_transition_analysis.csv")


# ============================================================
# CELL 6: CLEANUP
# ============================================================

result_df.unpersist()
print("Done. Unpersisted result_df.")
