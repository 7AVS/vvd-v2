# ============================================================
# ADHOC: VUT↔VAW Attribution Overlap Analysis
# Run AFTER result_df is persisted (Cell 4 complete)
# Both campaigns share wallet_provisioning as primary metric —
# if same clients appear in both, successes are double-counted.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 70)
print("VUT <-> VAW Attribution Overlap Analysis")
print("=" * 70)

# ========== PART 1: Client-Level Overlap ==========
print("\nPART 1: Client-Level Overlap")
print("-" * 70)

vut_clients = result_df.filter(F.col("MNE") == "VUT").select("CLNT_NO").distinct()
vaw_clients = result_df.filter(F.col("MNE") == "VAW").select("CLNT_NO").distinct()

total_vut = vut_clients.count()
total_vaw = vaw_clients.count()
overlap_clients = vut_clients.join(vaw_clients, "CLNT_NO", "inner")
overlap_count = overlap_clients.count()

print(f"VUT unique clients: {total_vut:,}")
print(f"VAW unique clients: {total_vaw:,}")
print(f"Clients in BOTH:    {overlap_count:,} ({overlap_count/total_vut*100:.1f}% of VUT, {overlap_count/total_vaw*100:.1f}% of VAW)")

# ========== PART 2: Window Overlap ==========
print("\n\nPART 2: Treatment Window Overlap")
print("-" * 70)

# First deployment per client per campaign
w = Window.partitionBy("CLNT_NO", "MNE").orderBy("TREATMT_STRT_DT")

client_campaign = (
    result_df
    .filter(F.col("MNE").isin(["VUT", "VAW"]))
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

vut_df = (
    client_campaign.filter(F.col("MNE") == "VUT")
    .select(
        F.col("CLNT_NO"),
        F.col("TST_GRP_CD").alias("VUT_GROUP"),
        F.col("TREATMT_STRT_DT").alias("VUT_START"),
        F.col("TREATMT_END_DT").alias("VUT_END"),
    )
)

vaw_df = (
    client_campaign.filter(F.col("MNE") == "VAW")
    .select(
        F.col("CLNT_NO"),
        F.col("TST_GRP_CD").alias("VAW_GROUP"),
        F.col("TREATMT_STRT_DT").alias("VAW_START"),
        F.col("TREATMT_END_DT").alias("VAW_END"),
    )
)

both = vut_df.join(vaw_df, "CLNT_NO", "inner")

# Window overlap: VUT_START <= VAW_END AND VAW_START <= VUT_END
both = both.withColumn(
    "WINDOWS_OVERLAP",
    F.when(
        (F.col("VUT_START") <= F.col("VAW_END")) &
        (F.col("VAW_START") <= F.col("VUT_END")),
        F.lit(1)
    ).otherwise(F.lit(0))
).withColumn(
    "ORDER",
    F.when(F.col("VUT_START") <= F.col("VAW_START"), F.lit("VUT_first"))
     .otherwise(F.lit("VAW_first"))
).withColumn(
    "GAP_DAYS",
    F.when(
        F.col("ORDER") == "VUT_first",
        F.datediff(F.col("VAW_START"), F.col("VUT_END"))
    ).otherwise(
        F.datediff(F.col("VUT_START"), F.col("VAW_END"))
    )
)

window_overlap_count = both.filter(F.col("WINDOWS_OVERLAP") == 1).count()
both_count = both.count()
print(f"Clients with BOTH campaigns: {both_count:,}")
print(f"  With overlapping windows:  {window_overlap_count:,} ({window_overlap_count/both_count*100:.1f}%)")
print(f"  Non-overlapping:           {both_count - window_overlap_count:,}")

print("\nOrder distribution:")
both.groupBy("ORDER").agg(
    F.count("*").alias("clients"),
    F.avg("GAP_DAYS").alias("avg_gap_days"),
    F.expr("percentile_approx(GAP_DAYS, 0.5)").alias("median_gap_days"),
).show(truncate=False)

print("Gap distribution:")
both.withColumn(
    "GAP_BUCKET",
    F.when(F.col("WINDOWS_OVERLAP") == 1, F.lit("Overlapping"))
     .when(F.col("GAP_DAYS") <= 7, F.lit("1-7 days"))
     .when(F.col("GAP_DAYS") <= 30, F.lit("8-30 days"))
     .when(F.col("GAP_DAYS") <= 90, F.lit("31-90 days"))
     .otherwise(F.lit("90+ days"))
).groupBy("GAP_BUCKET").agg(
    F.count("*").alias("clients")
).orderBy("GAP_BUCKET").show(truncate=False)

# ========== PART 3: Success Attribution ==========
print("\n\nPART 3: Double-Counted Successes")
print("-" * 70)

# For overlap clients: check who has PROVISIONING_SUCCESS=1 in BOTH campaigns
overlap_ids = overlap_clients.select("CLNT_NO")

overlap_success = (
    result_df
    .join(overlap_ids, "CLNT_NO", "left_semi")
    .filter(F.col("MNE").isin(["VUT", "VAW"]))
    .groupBy("CLNT_NO")
    .agg(
        F.max(F.when(F.col("MNE") == "VUT", F.col("PROVISIONING_SUCCESS"))).alias("VUT_SUCCESS"),
        F.max(F.when(F.col("MNE") == "VAW", F.col("PROVISIONING_SUCCESS"))).alias("VAW_SUCCESS"),
    )
    .withColumn(
        "PATTERN",
        F.when((F.col("VUT_SUCCESS") == 1) & (F.col("VAW_SUCCESS") == 1), F.lit("BOTH_CLAIM"))
         .when(F.col("VUT_SUCCESS") == 1, F.lit("VUT_ONLY"))
         .when(F.col("VAW_SUCCESS") == 1, F.lit("VAW_ONLY"))
         .otherwise(F.lit("NEITHER"))
    )
)

print("Success attribution among overlap clients:")
overlap_success.groupBy("PATTERN").agg(
    F.count("*").alias("clients")
).orderBy(F.desc("clients")).show(truncate=False)

# ========== PART 4: Impact on Campaign Metrics ==========
print("\n\nPART 4: Metrics With vs Without Overlap Clients")
print("-" * 70)

# Current metrics (all clients)
print("CURRENT (all clients):")
(
    result_df
    .filter(F.col("MNE").isin(["VUT", "VAW"]))
    .groupBy("MNE", "TST_GRP_CD")
    .agg(
        F.count("*").alias("total"),
        F.sum("PROVISIONING_SUCCESS").alias("successes"),
        (F.sum("PROVISIONING_SUCCESS") / F.count("*") * 100).alias("rate_pct"),
    )
    .orderBy("MNE", "TST_GRP_CD")
    .show(truncate=False)
)

# Without overlap clients
non_overlap_ids = (
    result_df
    .filter(F.col("MNE").isin(["VUT", "VAW"]))
    .select("CLNT_NO")
    .distinct()
    .join(overlap_ids, "CLNT_NO", "left_anti")
)

print("WITHOUT overlap clients (exclusive audiences only):")
(
    result_df
    .join(non_overlap_ids, "CLNT_NO", "left_semi")
    .filter(F.col("MNE").isin(["VUT", "VAW"]))
    .groupBy("MNE", "TST_GRP_CD")
    .agg(
        F.count("*").alias("total"),
        F.sum("PROVISIONING_SUCCESS").alias("successes"),
        (F.sum("PROVISIONING_SUCCESS") / F.count("*") * 100).alias("rate_pct"),
    )
    .orderBy("MNE", "TST_GRP_CD")
    .show(truncate=False)
)

# ========== PART 5: Channel Check ==========
print("\n\nPART 5: Channel Confirmation")
print("-" * 70)

for mne in ["VUT", "VAW"]:
    channels = (
        result_df.filter(F.col("MNE") == mne)
        .groupBy("TACTIC_CELL_CD")
        .agg(F.countDistinct("CLNT_NO").alias("clients"))
        .orderBy(F.desc("clients"))
        .collect()
    )
    print(f"\n{mne} channels: {[(r.TACTIC_CELL_CD, r.clients) for r in channels]}")

print("\n" + "=" * 70)
print("Done. Key question: how many 'BOTH_CLAIM' clients exist?")
print("If high → double-counting inflates both campaigns' success counts.")
print("If low → VUT and VAW target different audiences despite shared metric.")
print("=" * 70)
