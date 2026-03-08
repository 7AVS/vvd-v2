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

# ============================================================
# Monthly time series — card type evolution by ISS_DT
# ============================================================

from pyspark.sql import Window

CARD_CODES = ["01", "03", "04"]

vvd_cards_ts = (
    vvd_cards
    .filter(F.col("ISS_DT").isNotNull())
    .filter(F.col("VISA_DR_CRD_BRND_CD").isin(CARD_CODES))
    .withColumn("year_month", F.date_format(F.col("ISS_DT"), "yyyy-MM"))
)

# --- All VVD cards: monthly volume by card type ---
print("\n" + "="*70)
print("MONTHLY TIME SERIES — ALL VVD CARDS (codes 01, 03, 04)")
print("="*70)

monthly_all = (
    vvd_cards_ts
    .groupBy("year_month", "VISA_DR_CRD_BRND_CD")
    .agg(F.count("*").alias("count"))
)

# Total per month for percentage calc
monthly_total = (
    monthly_all
    .groupBy("year_month")
    .agg(F.sum("count").alias("month_total"))
)

monthly_all_pct = (
    monthly_all
    .join(monthly_total, "year_month")
    .withColumn("pct", F.round(F.col("count") / F.col("month_total") * 100, 2))
    .orderBy("year_month", "VISA_DR_CRD_BRND_CD")
)

print("\n--- Volume per month per card type ---")
monthly_all_pct.select("year_month", "VISA_DR_CRD_BRND_CD", "count", "pct", "month_total").show(200, truncate=False)

# --- Experiment clients only: monthly volume by card type ---
print("\n" + "="*70)
print("MONTHLY TIME SERIES — EXPERIMENT CLIENTS ONLY (codes 01, 03, 04)")
print("="*70)

vvd_experiment_ts = vvd_cards_ts.join(experiment_clients, "CLNT_NO", "left_semi")

monthly_exp = (
    vvd_experiment_ts
    .groupBy("year_month", "VISA_DR_CRD_BRND_CD")
    .agg(F.count("*").alias("count"))
)

monthly_exp_total = (
    monthly_exp
    .groupBy("year_month")
    .agg(F.sum("count").alias("month_total"))
)

monthly_exp_pct = (
    monthly_exp
    .join(monthly_exp_total, "year_month")
    .withColumn("pct", F.round(F.col("count") / F.col("month_total") * 100, 2))
    .orderBy("year_month", "VISA_DR_CRD_BRND_CD")
)

print("\n--- Volume per month per card type (experiment clients) ---")
monthly_exp_pct.select("year_month", "VISA_DR_CRD_BRND_CD", "count", "pct", "month_total").show(200, truncate=False)

# --- Plotly visualization (percentage share over time) ---
try:
    import plotly.graph_objects as go

    # Collect for charting — all VVD cards
    rows_all = monthly_all_pct.select("year_month", "VISA_DR_CRD_BRND_CD", "pct").collect()
    data_all = {}
    for r in rows_all:
        code = str(r["VISA_DR_CRD_BRND_CD"])
        data_all.setdefault(code, {"x": [], "y": []})
        data_all[code]["x"].append(str(r["year_month"]))
        data_all[code]["y"].append(float(r["pct"]))

    fig = go.Figure()
    for code in sorted(data_all.keys()):
        fig.add_trace(go.Scatter(
            x=data_all[code]["x"],
            y=data_all[code]["y"],
            mode="lines+markers",
            name=f"Code {code}"
        ))
    fig.update_layout(
        title="Card Type % Share by Month (All VVD Cards)",
        xaxis_title="Month",
        yaxis_title="% Share",
        yaxis=dict(range=[0, 100]),
        hovermode="x unified"
    )
    displayHTML(fig.to_html(include_plotlyjs='cdn'))

    # Collect for charting — experiment clients
    rows_exp = monthly_exp_pct.select("year_month", "VISA_DR_CRD_BRND_CD", "pct").collect()
    data_exp = {}
    for r in rows_exp:
        code = str(r["VISA_DR_CRD_BRND_CD"])
        data_exp.setdefault(code, {"x": [], "y": []})
        data_exp[code]["x"].append(str(r["year_month"]))
        data_exp[code]["y"].append(float(r["pct"]))

    fig2 = go.Figure()
    for code in sorted(data_exp.keys()):
        fig2.add_trace(go.Scatter(
            x=data_exp[code]["x"],
            y=data_exp[code]["y"],
            mode="lines+markers",
            name=f"Code {code}"
        ))
    fig2.update_layout(
        title="Card Type % Share by Month (Experiment Clients)",
        xaxis_title="Month",
        yaxis_title="% Share",
        yaxis=dict(range=[0, 100]),
        hovermode="x unified"
    )
    displayHTML(fig2.to_html(include_plotlyjs='cdn'))

except Exception as e:
    print(f"\nPlotly chart skipped: {e}")
