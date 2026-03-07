# Ad hoc: verify Card_Type distribution for VVD experiment clients (2025-2026)
# Checks VISA_DR_CRD_BRND_CD and other card-related fields in DDWTA_VISA_DR_CRD

from pyspark.sql import functions as F

CARD_DATA_PATH = "/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT={year}*"

# Load card table for 2025-2026
card_paths = [CARD_DATA_PATH.format(year=y) for y in [2025, 2026]]
raw_card = spark.read.parquet(*card_paths)

# What columns are available?
print("Card table columns:")
for c in sorted(raw_card.columns):
    print(f"  {c}")

# Filter to VVD (SRVC_ID=36) active cards
vvd_cards = (
    raw_card
    .filter(F.col("SRVC_ID") == 36)
    .filter(F.col("STS_CD").isin(["06", "08"]))
    .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("CLNT_NO")), "^0+", ""))
)

print(f"\nVVD cards (2025-2026): {vvd_cards.count():,}")

# VISA_DR_CRD_BRND_CD distribution (the Card_Type source field)
print("\n--- VISA_DR_CRD_BRND_CD distribution ---")
vvd_cards.groupBy("VISA_DR_CRD_BRND_CD").agg(
    F.count("*").alias("count"),
    (F.count("*") / vvd_cards.count() * 100).alias("pct")
).orderBy(F.desc("count")).show(30, truncate=False)

# Null rate
total = vvd_cards.count()
null_count = vvd_cards.filter(F.col("VISA_DR_CRD_BRND_CD").isNull()).count()
print(f"VISA_DR_CRD_BRND_CD null rate: {null_count:,} / {total:,} = {null_count/total*100:.1f}%")

# Narrow to experiment clients only
experiment_clients = result_df.select("CLNT_NO").distinct()
vvd_experiment = vvd_cards.join(experiment_clients, "CLNT_NO", "left_semi")
print(f"\nVVD cards (experiment clients only): {vvd_experiment.count():,}")

print("\n--- VISA_DR_CRD_BRND_CD for experiment clients ---")
vvd_experiment.groupBy("VISA_DR_CRD_BRND_CD").agg(
    F.count("*").alias("count"),
    (F.count("*") / vvd_experiment.count() * 100).alias("pct")
).orderBy(F.desc("count")).show(30, truncate=False)

# Check other potentially interesting card fields
for col_name in ["CRD_TP", "CRD_BRND_CD", "PROD_CD", "PROD_TP_CD"]:
    if col_name in vvd_cards.columns:
        print(f"\n--- {col_name} distribution (experiment clients) ---")
        vvd_experiment.groupBy(col_name).agg(
            F.count("*").alias("count")
        ).orderBy(F.desc("count")).show(20, truncate=False)
