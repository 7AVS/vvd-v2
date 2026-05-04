# =============================================================================
# VVD VINTAGE CURVES — STANDALONE HDFS
# Implements VVD_SUCCESS_LOGIC_FINAL.sql logic in PySpark, reads HDFS directly.
# Independent of vvd_v3_pipeline.py — drop-in for a Lumina notebook.
#
# Output: MNE | METRIC | COHORT | TST_GRP_CD | VINTAGE_DAY | LEADS | SUCCESS_CUM
# Windows: VCN 30d  VDA 90d  VDT 30d  VUI 30d  VUT 90d  VAW 30d
#
# Requires in scope: spark (SparkSession), EDW (Teradata cursor for VUT/VAW
# wallet pull). VUT/VAW primary will be empty if EDW is unavailable.
# =============================================================================

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# ---- Config -----------------------------------------------------------------
TACTIC_BASE = "/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/"
CARD_BASE   = "/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/"
POS_BASE    = "/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/"

YEARS     = [2024, 2025, 2026]
DATE_FROM = "2025-01-01"
DATE_TO   = "2026-03-31"

# campaign -> [(METRIC label, success-event key, window in days)]
CAMPAIGN_METRICS = {
    "VCN": [("PRIMARY_ACQUISITION",  "card_acquisition",    30)],
    "VDA": [("PRIMARY_ACQUISITION",  "card_acquisition",    90),
            ("SECONDARY_USAGE",      "card_usage",          90)],
    "VDT": [("PRIMARY_ACTIVATION",   "card_activation",     30)],
    "VUI": [("PRIMARY_USAGE",        "card_usage",          30)],
    "VUT": [("PRIMARY_PROVISIONING", "wallet_provisioning", 90),
            ("SECONDARY_USAGE",      "card_usage",          90)],
    "VAW": [("PRIMARY_PROVISIONING", "wallet_provisioning", 30),
            ("SECONDARY_USAGE",      "card_usage",          30)],
}

# ---- Step 1: Tactic population ---------------------------------------------
tactic_paths = [TACTIC_BASE + f"EVNT_STRT_DT={y}*" for y in YEARS]
tactic = (
    spark.read.option("basePath", TACTIC_BASE).parquet(*tactic_paths)
    .withColumn("MNE", F.substring(F.col("TACTIC_ID"), 8, 3))
    .filter(F.col("MNE").isin(list(CAMPAIGN_METRICS.keys())))
    .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("TACTIC_EVNT_ID")), "^0+", ""))
    .withColumn("TST_GRP_CD", F.trim(F.col("TST_GRP_CD")))
    .filter(F.col("TST_GRP_CD").isin(["TG4", "TG7"]))
    .filter((F.col("TREATMT_STRT_DT") >= F.lit(DATE_FROM)) &
            (F.col("TREATMT_STRT_DT") <= F.lit(DATE_TO)))
    .withColumn("COHORT", F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM"))
    .select("CLNT_NO", "MNE", "TST_GRP_CD", "TREATMT_STRT_DT", "COHORT")
    .distinct()
)
tactic.persist()
print(f"Tactic population: {tactic.count():,} rows, "
      f"{tactic.select('CLNT_NO').distinct().count():,} unique clients")


# ---- Step 2: Success event tables (FINAL.sql filters verbatim) -------------

# 2a. Card data: acquisition (ISS_DT) + activation (ACTV_DT)
card_paths = [CARD_BASE + f"CAPTR_DT={y}*" for y in YEARS]
raw_card = spark.read.parquet(*card_paths)
latest_snap = raw_card.agg(F.max("SNAP_DT")).collect()[0][0]

card = (
    raw_card
    .filter(F.col("STS_CD").isin(["06", "08"]))
    .filter(F.col("SRVC_ID") == 36)
    .filter(F.col("SNAP_DT") == F.lit(latest_snap))
    .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("CLNT_NO")), "^0+", ""))
)

card_acquisition = (
    card.filter(F.col("ISS_DT").isNotNull())
        .select("CLNT_NO", F.col("ISS_DT").cast("date").alias("SUCCESS_DT"))
        .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
)

card_activation = (
    card.filter(F.col("ACTV_DT").isNotNull())
        .select("CLNT_NO", F.col("ACTV_DT").cast("date").alias("SUCCESS_DT"))
        .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
)

# 2b. Card usage — settled purchase / settled refund (FINAL.sql verbatim)
pos_paths = [POS_BASE + f"SNAP_DT={y}*" for y in YEARS]
raw_pos = spark.read.parquet(*pos_paths)

card_usage = (
    raw_pos
    .filter(F.col("SRVC_CD") == 36)
    .filter(F.col("MSG_TP") == "0220")
    .filter(F.col("AMT1") > 0)
    .filter(
        ((F.col("TXN_TP").isin([10, 13])) & (F.col("RCNCL_REAS_CD").isin(["M", "S"]))) |
        ((F.col("TXN_TP") == 22)          & (F.col("RCNCL_REAS_CD").isin(["P", "E"])))
    )
    .withColumn("CLNT_NO", F.regexp_replace(F.substring(F.col("CLNT_CRD_NO"), 7, 9), "^0+", ""))
    .select("CLNT_NO", F.col("TXN_DT").cast("date").alias("SUCCESS_DT"))
    .filter(F.col("SUCCESS_DT").isNotNull())
    .dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
)

# 2c. Wallet provisioning — Teradata only (FINAL.sql verbatim)
wallet_query = """
SELECT DISTINCT
    CAST(SUBSTR(B.CLNT_CRD_NO, 7, 9) AS INTEGER) AS CLNT_NO,
    B.TXN_DT AS SUCCESS_DT
FROM DDWV05.CLNT_CRD_POS_LOG B
INNER JOIN DL_DECMAN.TOKEN_LIST C
    ON B.TOKN_REQSTR_ID = C.TOKEN_ID
WHERE B.SRVC_CD = 36
    AND B.AMT1 = 0
    AND (B.VISA_DR_CRD_NO LIKE '45190%' OR B.VISA_DR_CRD_NO LIKE '45199%')
    AND B.POS_ENTR_MODE_CD_NON_EMV = '000'
    AND B.APPROVAL_CODE IS NOT NULL
    AND C.TOKEN_WALLET_IND = 'Y'
    AND B.TXN_DT >= DATE '2025-01-01'
"""

try:
    cur = EDW.cursor()
    cur.execute(wallet_query)
    rows = cur.fetchall()
    cur.close()
    wallet_provisioning = spark.createDataFrame(
        [(str(int(r[0])), r[1]) for r in rows],
        "CLNT_NO STRING, SUCCESS_DT DATE"
    ).dropDuplicates(["CLNT_NO", "SUCCESS_DT"])
    print(f"Wallet provisioning events: {wallet_provisioning.count():,}")
except Exception as e:
    print(f"WARNING: wallet pull failed ({e})")
    print("VUT/VAW primary curves will be empty.")
    wallet_provisioning = spark.createDataFrame([], "CLNT_NO STRING, SUCCESS_DT DATE")

SUCCESS_EVENTS = {
    "card_acquisition":    card_acquisition,
    "card_activation":     card_activation,
    "card_usage":          card_usage,
    "wallet_provisioning": wallet_provisioning,
}


# ---- Step 3: Vintage curve builder -----------------------------------------
def vintage_for(mne, success_df, window):
    """Cumulative vintage curve for one (campaign, metric, window) leg."""
    t = tactic.filter(F.col("MNE") == mne)

    # Leads = distinct CLNT_NO per (cohort, test) within campaign
    leads = (
        t.groupBy("COHORT", "TST_GRP_CD")
         .agg(F.countDistinct("CLNT_NO").alias("LEADS"))
    )

    # First success per (CLNT_NO, COHORT, TST_GRP_CD) within window
    matches = (
        t.alias("t")
        .join(success_df.alias("s"),
              (F.col("t.CLNT_NO") == F.col("s.CLNT_NO")) &
              (F.col("s.SUCCESS_DT") >= F.col("t.TREATMT_STRT_DT")) &
              (F.col("s.SUCCESS_DT") <= F.date_add(F.col("t.TREATMT_STRT_DT"), window - 1)),
              "inner")
        .select(
            F.col("t.CLNT_NO").alias("CLNT_NO"),
            F.col("t.COHORT").alias("COHORT"),
            F.col("t.TST_GRP_CD").alias("TST_GRP_CD"),
            F.datediff(F.col("s.SUCCESS_DT"), F.col("t.TREATMT_STRT_DT")).alias("VINTAGE_DAY"),
        )
    )

    first_per_client = (
        matches
        .groupBy("CLNT_NO", "COHORT", "TST_GRP_CD")
        .agg(F.min("VINTAGE_DAY").alias("VINTAGE_DAY"))
    )

    per_day = (
        first_per_client
        .groupBy("COHORT", "TST_GRP_CD", "VINTAGE_DAY")
        .agg(F.count("*").alias("DAY_SUCCESS"))
    )

    # Dense day spine 0..window-1 per (cohort, test)
    days = spark.range(window).withColumnRenamed("id", "VINTAGE_DAY")
    spine = leads.crossJoin(days)

    w = (Window
         .partitionBy("COHORT", "TST_GRP_CD")
         .orderBy("VINTAGE_DAY")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    return (
        spine.join(per_day, ["COHORT", "TST_GRP_CD", "VINTAGE_DAY"], "left")
             .fillna({"DAY_SUCCESS": 0})
             .withColumn("SUCCESS_CUM", F.sum("DAY_SUCCESS").over(w))
             .select("COHORT", "TST_GRP_CD", "VINTAGE_DAY", "LEADS", "SUCCESS_CUM")
    )


# ---- Step 4: Run all (campaign × metric) legs ------------------------------
all_curves = []
for mne, metrics in CAMPAIGN_METRICS.items():
    for metric_label, metric_key, window in metrics:
        success_df = SUCCESS_EVENTS[metric_key]
        if success_df.head(1) == []:
            print(f"  SKIP {mne} / {metric_label} (no events for {metric_key})")
            continue

        curve = (
            vintage_for(mne, success_df, window)
            .withColumn("MNE", F.lit(mne))
            .withColumn("METRIC", F.lit(metric_label))
            .select("MNE", "METRIC", "COHORT", "TST_GRP_CD",
                    "VINTAGE_DAY", "LEADS", "SUCCESS_CUM")
        )
        all_curves.append(curve)
        print(f"  built {mne} / {metric_label} ({window}d)")

result = all_curves[0]
for c in all_curves[1:]:
    result = result.unionByName(c)

result_pd = (
    result
    .orderBy("MNE", "METRIC", "COHORT", "TST_GRP_CD", "VINTAGE_DAY")
    .toPandas()
)
print(f"\nVintage curves: {len(result_pd):,} rows")
print(result_pd.head(20).to_string(index=False))

result_pd.to_csv("vvd_vintage_curves_hdfs.csv", index=False)
print("\nSaved: vvd_vintage_curves_hdfs.csv")
