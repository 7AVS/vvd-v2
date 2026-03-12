#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ====================================================================
# VVD v3 — Evaluation Workbench
#
# Generic campaign evaluation engine. Reads result_df, runs all
# analytical methods for all campaigns, exports 3 clean CSVs.
#
# NOT tailored to specific campaigns. Same calculations for all.
# Interpretation is done by the analyst, not the code.
#
# Depends on: vvd_v3_pipeline.py (produces result_df on HDFS)
# Optional: POS transactions (for spending/DiD), UCP (for profiling)
#
# Structure: Each section below is one notebook cell.
# Cell 1: Setup (load data)
# Cell 2: Effectiveness (A/B lift at multiple grains)
# Cell 3: Credibility (SRM + cohort stability)
# Cell 4: Efficiency (contacts per incremental, frequency, diminishing returns)
# Cell 5: Dynamics — vintage curves
# Cell 6: Dynamics — spending, DiD, velocity curves
# Cell 7: Profile (UCP segments + decision tree) [optional]
# Cell 8: Scorecard assembly
# Cell 9: Export (3 CSVs)
# Cell 10: Cleanup
# ====================================================================


# ============================================================
# CELL 1: SETUP
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField
from pyspark import StorageLevel
import math
import base64

try:
    from scipy.stats import norm as scipy_norm
    from scipy.stats import chi2 as scipy_chi2
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

print(f"scipy available: {HAS_SCIPY}")

CONFIG = {
    "VCN": {"success_col": "ACQUISITION_SUCCESS", "success_dt_col": "FIRST_ACQUISITION_SUCCESS_DT", "expected_split": 0.95},
    "VDA": {"success_col": "ACQUISITION_SUCCESS", "success_dt_col": "FIRST_ACQUISITION_SUCCESS_DT", "expected_split": 0.95},
    "VDT": {"success_col": "ACTIVATION_SUCCESS", "success_dt_col": "FIRST_ACTIVATION_SUCCESS_DT", "expected_split": 0.90},
    "VUI": {"success_col": "USAGE_SUCCESS", "success_dt_col": "FIRST_USAGE_SUCCESS_DT", "expected_split": 0.95},
    "VUT": {"success_col": "PROVISIONING_SUCCESS", "success_dt_col": "FIRST_PROVISIONING_SUCCESS_DT", "expected_split": 0.95},
    "VAW": {"success_col": "PROVISIONING_SUCCESS", "success_dt_col": "FIRST_PROVISIONING_SUCCESS_DT", "expected_split": 0.80},
}
ACTION_GROUP = "TG4"
CONTROL_GROUP = "TG7"

def download_csv(df, filename):
    pdf = df.toPandas() if hasattr(df, 'toPandas') else df
    csv_data = pdf.to_csv(index=False)
    b64 = base64.b64encode(csv_data.encode()).decode()
    size_mb = len(csv_data) / 1_048_576
    from IPython.display import display, HTML
    display(HTML(f'<a href="data:text/csv;base64,{b64}" download="{filename}" '
                 f'style="font-size:16px;padding:8px 16px;background:#1a73e8;color:white;'
                 f'text-decoration:none;border-radius:4px">Download {filename}</a> '
                 f'({size_mb:.2f} MB)'))

# Load result_df
try:
    result_df.count()
    print("result_df already in memory")
except NameError:
    try:
        HDFS_V3 = "/user/427966379/vvd_v3_result"
        result_df = spark.read.parquet(HDFS_V3)
        cnt = result_df.count()
        print(f"Loaded result_df from v3 HDFS: {cnt:,} rows")
    except Exception:
        HDFS_V2 = "/user/427966379/vvd_v2_result"
        result_df = spark.read.parquet(HDFS_V2)
        cnt = result_df.count()
        print(f"Loaded result_df from v2 HDFS (fallback): {cnt:,} rows")

result_df.persist(StorageLevel.MEMORY_AND_DISK)
mnes_in_data = sorted([r.MNE for r in result_df.select("MNE").distinct().collect()])
print(f"Campaigns in data: {mnes_in_data}")

# Load POS
POS_TXN_PATH = "/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT={year}*"
YEARS = [2024, 2025, 2026]

try:
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
    experiment_clients = result_df.select("CLNT_NO").distinct()
    pos_df = pos_df.join(experiment_clients, "CLNT_NO", "left_semi")
    pos_df.persist(StorageLevel.MEMORY_AND_DISK)
    pos_count = pos_df.count()
    print(f"POS transactions (experiment clients): {pos_count:,}")
    HAS_POS = True
except Exception as e:
    print(f"POS load failed: {e}")
    HAS_POS = False

# Load UCP (optional)
UCP_PATH = "/prod/sz/tsz/00172/data/ucp4/MONTH_END_DATE={date}"
HAS_UCP = False
try:
    import subprocess
    ucp_check = subprocess.check_output(["hdfs", "dfs", "-ls", "/prod/sz/tsz/00172/data/ucp4/"], stderr=subprocess.STDOUT).decode()
    ucp_dates = sorted([line.split("=")[-1] for line in ucp_check.strip().split("\n") if "MONTH_END_DATE=" in line])
    if ucp_dates:
        latest_ucp_date = ucp_dates[-1]
        ucp_df = spark.read.parquet(UCP_PATH.format(date=latest_ucp_date))
        ucp_df = (
            ucp_df
            .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("CLNT_NO")), "^0+", ""))
            .select("CLNT_NO").distinct()
        )
        # Verify it has rows
        ucp_sample = ucp_df.limit(5).count()
        if ucp_sample > 0:
            HAS_UCP = True
            print(f"UCP loaded from {latest_ucp_date}")
        else:
            print("UCP snapshot empty, skipping")
except Exception as e:
    print(f"UCP not available: {e}")

print(f"\nSetup complete. HAS_POS={HAS_POS}, HAS_UCP={HAS_UCP}")


# ============================================================
# CELL 2: EFFECTIVENESS — A/B lift at multiple grains
# ============================================================

print("=" * 60)
print("CELL 2: EFFECTIVENESS")
print("=" * 60)

detail_rows = []

def _ztest_pvalue(a_successes, a_total, c_successes, c_total):
    if a_total == 0 or c_total == 0:
        return 1.0, 0.0, 0.0, 0.0
    a_rate = a_successes / a_total
    c_rate = c_successes / c_total
    lift_pp = (a_rate - c_rate) * 100
    p_pool = (a_successes + c_successes) / (a_total + c_total)
    if p_pool <= 0 or p_pool >= 1:
        return 1.0, lift_pp, 0.0, 0.0
    se = math.sqrt(p_pool * (1 - p_pool) * (1.0 / a_total + 1.0 / c_total))
    if se == 0:
        return 1.0, lift_pp, 0.0, 0.0
    z = (a_rate - c_rate) / se
    if HAS_SCIPY:
        pvalue = 2 * (1 - scipy_norm.cdf(abs(z)))
    else:
        pvalue = math.erfc(abs(z) / math.sqrt(2))
    ci_half = 1.96 * se * 100
    ci_lower = lift_pp - ci_half
    ci_upper = lift_pp + ci_half
    return pvalue, lift_pp, ci_lower, ci_upper

def _stars(p):
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    return ""

for mne in mnes_in_data:
    if mne not in CONFIG:
        print(f"  SKIP {mne} — not in CONFIG")
        continue

    cfg = CONFIG[mne]
    mne_df = (
        result_df
        .filter(F.col("MNE") == mne)
        .withColumn("_SUCCESS", F.col(cfg["success_col"]).cast("int"))
    )

    grain_specs = [
        ("OVERALL", F.lit("ALL")),
        ("COHORT", F.col("COHORT")),
        ("CHANNEL", F.col("TACTIC_CELL_CD")),
        ("RPT_GRP", F.col("RPT_GRP_CD")),
    ]

    for grain_name, slice_col in grain_specs:
        agg_df = (
            mne_df
            .withColumn("_SLICE", slice_col)
            .groupBy("TST_GRP_CD", "_SLICE")
            .agg(
                F.countDistinct("CLNT_NO").alias("clients"),
                F.sum("_SUCCESS").alias("successes"),
            )
            .collect()
        )

        slices = sorted(set(r._SLICE for r in agg_df if r._SLICE is not None))
        for s in slices:
            action = [r for r in agg_df if r._SLICE == s and r.TST_GRP_CD is not None and r.TST_GRP_CD.strip() == ACTION_GROUP]
            control = [r for r in agg_df if r._SLICE == s and r.TST_GRP_CD is not None and r.TST_GRP_CD.strip() == CONTROL_GROUP]
            if not action or not control:
                continue

            a = action[0]
            c = control[0]
            a_clients = int(a.clients)
            c_clients = int(c.clients)
            a_successes = int(a.successes or 0)
            c_successes = int(c.successes or 0)
            a_rate = a_successes / a_clients if a_clients > 0 else 0.0
            c_rate = c_successes / c_clients if c_clients > 0 else 0.0

            pvalue, lift_pp, ci_lower, ci_upper = _ztest_pvalue(a_successes, a_clients, c_successes, c_clients)
            rel_lift = (lift_pp / (c_rate * 100) * 100) if c_rate > 0 else 0.0
            incremental = a_clients * lift_pp / 100

            detail_rows.append({
                "MNE": mne,
                "GRAIN": grain_name,
                "SLICE": str(s),
                "CLIENTS_ACTION": a_clients,
                "CLIENTS_CONTROL": c_clients,
                "SUCCESSES_ACTION": a_successes,
                "SUCCESSES_CONTROL": c_successes,
                "RATE_ACTION": round(a_rate * 100, 4),
                "RATE_CONTROL": round(c_rate * 100, 4),
                "LIFT_PP": round(lift_pp, 4),
                "REL_LIFT_PCT": round(rel_lift, 2),
                "PVALUE": round(pvalue, 8),
                "STARS": _stars(pvalue),
                "CI_LOWER": round(ci_lower, 4),
                "CI_UPPER": round(ci_upper, 4),
                "INCREMENTAL": round(incremental, 1),
            })

    print(f"  {mne}: {sum(1 for r in detail_rows if r['MNE'] == mne)} rows across all grains")

print(f"\nTotal effectiveness rows: {len(detail_rows)}")
# Verify: show OVERALL rows
for r in detail_rows:
    if r["GRAIN"] == "OVERALL":
        print(f"  {r['MNE']}: lift={r['LIFT_PP']:.2f}pp, p={r['PVALUE']:.4f} {r['STARS']}, incremental={r['INCREMENTAL']:.0f}")


# ============================================================
# CELL 3: CREDIBILITY — SRM + cohort stability
# ============================================================

print("=" * 60)
print("CELL 3: CREDIBILITY")
print("=" * 60)

credibility_rows = []

for mne in mnes_in_data:
    if mne not in CONFIG:
        continue

    cfg = CONFIG[mne]
    overall = [r for r in detail_rows if r["MNE"] == mne and r["GRAIN"] == "OVERALL"]
    if not overall:
        continue
    ov = overall[0]

    # 3a: SRM
    total_clients = ov["CLIENTS_ACTION"] + ov["CLIENTS_CONTROL"]
    exp_split = cfg["expected_split"]
    exp_action = total_clients * exp_split
    exp_control = total_clients * (1 - exp_split)
    obs_action = ov["CLIENTS_ACTION"]
    obs_control = ov["CLIENTS_CONTROL"]

    if exp_action > 0 and exp_control > 0:
        chi2_val = ((obs_action - exp_action) ** 2 / exp_action +
                    (obs_control - exp_control) ** 2 / exp_control)
        if HAS_SCIPY:
            srm_pvalue = 1 - scipy_chi2.cdf(chi2_val, 1)
        else:
            # Approximation for chi2 with df=1
            z_equiv = math.sqrt(chi2_val)
            srm_pvalue = math.erfc(z_equiv / math.sqrt(2))
    else:
        chi2_val = 0.0
        srm_pvalue = 1.0

    srm_status = "FAIL" if srm_pvalue < 0.01 else "PASS"

    # 3b: Cohort stability
    cohort_rows = [r for r in detail_rows if r["MNE"] == mne and r["GRAIN"] == "COHORT"]
    lifts = [r["LIFT_PP"] for r in cohort_rows]

    if len(lifts) >= 2:
        mean_lift = sum(lifts) / len(lifts)
        variance = sum((x - mean_lift) ** 2 for x in lifts) / (len(lifts) - 1)
        std_lift = math.sqrt(variance)
        if abs(mean_lift) > 0.001:
            cohort_cv = std_lift / abs(mean_lift)
        else:
            cohort_cv = 999.0
    elif len(lifts) == 1:
        cohort_cv = 0.0
    else:
        cohort_cv = 999.0

    if cohort_cv < 0.5:
        stability = "STABLE"
    elif cohort_cv < 1.0:
        stability = "MODERATE"
    else:
        stability = "UNSTABLE"

    credibility_rows.append({
        "MNE": mne,
        "TOTAL_CLIENTS": total_clients,
        "OBS_ACTION_PCT": round(obs_action / total_clients * 100, 2) if total_clients > 0 else 0,
        "EXP_ACTION_PCT": round(exp_split * 100, 1),
        "SRM_CHI2": round(chi2_val, 2),
        "SRM_PVALUE": round(srm_pvalue, 6),
        "SRM_STATUS": srm_status,
        "NUM_COHORTS": len(lifts),
        "COHORT_CV": round(cohort_cv, 3),
        "STABILITY_RATING": stability,
    })

    print(f"  {mne}: SRM={srm_status} (p={srm_pvalue:.4f}), CV={cohort_cv:.3f} ({stability})")

print(f"\nCredibility rows: {len(credibility_rows)}")


# ============================================================
# CELL 4: EFFICIENCY — contacts, frequency, diminishing returns
# ============================================================

print("=" * 60)
print("CELL 4: EFFICIENCY")
print("=" * 60)

efficiency_rows = []
curve_rows = []

for mne in mnes_in_data:
    if mne not in CONFIG:
        continue

    cfg = CONFIG[mne]
    mne_df = result_df.filter(F.col("MNE") == mne)
    action_df = mne_df.filter(F.trim(F.col("TST_GRP_CD")) == ACTION_GROUP)

    total_deployments = action_df.count()
    total_clients = action_df.select("CLNT_NO").distinct().count()
    avg_contacts = total_deployments / total_clients if total_clients > 0 else 0

    ov = [r for r in detail_rows if r["MNE"] == mne and r["GRAIN"] == "OVERALL"]
    incremental = ov[0]["INCREMENTAL"] if ov else 0
    deploys_per_inc = total_deployments / incremental if incremental > 0 else 0

    efficiency_rows.append({
        "MNE": mne,
        "TOTAL_DEPLOYMENTS": total_deployments,
        "TOTAL_CLIENTS": total_clients,
        "AVG_CONTACTS": round(avg_contacts, 2),
        "INCREMENTAL_CLIENTS": round(incremental, 1),
        "DEPLOYS_PER_INCREMENTAL": round(deploys_per_inc, 1),
    })

    print(f"  {mne}: {total_deployments:,} deploys, {total_clients:,} clients, "
          f"avg={avg_contacts:.2f} contacts, {deploys_per_inc:.1f} deploys/incremental")

# Contact frequency distribution
print("\n--- Contact Frequency Distribution ---")

freq_df = (
    result_df
    .filter(F.trim(F.col("TST_GRP_CD")) == ACTION_GROUP)
    .groupBy("MNE", "CLNT_NO")
    .agg(
        F.count("*").alias("contact_count"),
        F.max("SUCCESS").alias("SUCCESS"),
    )
)

freq_bucketed = freq_df.withColumn(
    "FREQ_BUCKET",
    F.when(F.col("contact_count") == 1, "1")
    .when(F.col("contact_count") == 2, "2")
    .when(F.col("contact_count") == 3, "3")
    .when(F.col("contact_count") <= 5, "4-5")
    .when(F.col("contact_count") <= 10, "6-10")
    .otherwise("11+")
)

freq_agg = (
    freq_bucketed
    .groupBy("MNE", "FREQ_BUCKET")
    .agg(
        F.count("*").alias("clients"),
        F.sum("SUCCESS").alias("successes"),
    )
    .collect()
)

for r in freq_agg:
    clients = int(r.clients)
    successes = int(r.successes or 0)
    rate = successes / clients * 100 if clients > 0 else 0

    detail_rows.append({
        "MNE": r.MNE,
        "GRAIN": "FREQUENCY",
        "SLICE": r.FREQ_BUCKET,
        "CLIENTS_ACTION": clients,
        "CLIENTS_CONTROL": 0,
        "SUCCESSES_ACTION": successes,
        "SUCCESSES_CONTROL": 0,
        "RATE_ACTION": round(rate, 4),
        "RATE_CONTROL": 0.0,
        "LIFT_PP": 0.0,
        "REL_LIFT_PCT": 0.0,
        "PVALUE": 1.0,
        "STARS": "",
        "CI_LOWER": 0.0,
        "CI_UPPER": 0.0,
        "INCREMENTAL": 0.0,
    })

print(f"  Frequency rows added: {len(freq_agg)}")

# Diminishing returns: success rate by contact sequence number
print("\n--- Diminishing Returns ---")

seq_df = result_df.filter(F.trim(F.col("TST_GRP_CD")) == ACTION_GROUP)
w_seq = Window.partitionBy("MNE", "CLNT_NO").orderBy("TREATMT_STRT_DT")
seq_df = seq_df.withColumn("CONTACT_SEQ", F.row_number().over(w_seq))

dim_returns = (
    seq_df
    .filter(F.col("CONTACT_SEQ") <= 10)
    .groupBy("MNE", "CONTACT_SEQ")
    .agg(
        F.count("*").alias("deployments"),
        F.sum("SUCCESS").alias("successes"),
    )
    .collect()
)

for r in dim_returns:
    deploys = int(r.deployments)
    successes = int(r.successes or 0)
    rate = successes / deploys * 100 if deploys > 0 else 0

    curve_rows.append({
        "MNE": r.MNE,
        "COHORT": "ALL",
        "TST_GRP_CD": ACTION_GROUP,
        "CURVE_TYPE": "diminishing_returns",
        "DAY": int(r.CONTACT_SEQ),
        "VALUE": round(rate, 4),
        "CLIENT_CNT": deploys,
    })

print(f"  Diminishing returns rows: {len(dim_returns)}")
print(f"\nEfficiency rows: {len(efficiency_rows)}")


# ============================================================
# CELL 5: DYNAMICS — Vintage Curves (Spark-native)
# ============================================================

print("=" * 60)
print("CELL 5: VINTAGE CURVES")
print("=" * 60)

# Build dynamic SUCCESS_DT column per MNE
success_dt_expr = F.lit(None).cast("date")
for mne, cfg in CONFIG.items():
    success_dt_expr = F.when(F.col("MNE") == mne, F.col(cfg["success_dt_col"])).otherwise(success_dt_expr)

vintage_base = (
    result_df
    .filter(F.col("MNE").isin(list(CONFIG.keys())))
    .withColumn("SUCCESS_DT", success_dt_expr)
    .withColumn(
        "DAYS_TO_SUCCESS",
        F.when(F.col("SUCCESS") == 1, F.datediff(F.col("SUCCESS_DT"), F.col("TREATMT_STRT_DT")))
    )
)

# Client counts per group (denominator)
group_counts = (
    vintage_base
    .groupBy("MNE", "COHORT", "TST_GRP_CD")
    .agg(
        F.countDistinct("CLNT_NO").alias("CLIENT_CNT"),
        F.expr("percentile_approx(WINDOW_DAYS, 0.5)").alias("MEDIAN_WINDOW"),
    )
)

# Success day counts (numerator pieces)
day_counts = (
    vintage_base
    .filter(F.col("SUCCESS") == 1)
    .filter(F.col("DAYS_TO_SUCCESS").isNotNull())
    .filter(F.col("DAYS_TO_SUCCESS") >= 0)
    .groupBy("MNE", "COHORT", "TST_GRP_CD", "DAYS_TO_SUCCESS")
    .agg(F.countDistinct("CLNT_NO").alias("DAY_CNT"))
)

# Cumulative sum via window
w_cum = (
    Window
    .partitionBy("MNE", "COHORT", "TST_GRP_CD")
    .orderBy("DAYS_TO_SUCCESS")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

day_cumulative = day_counts.withColumn("CUM_CNT", F.sum("DAY_CNT").over(w_cum))

# Join with group counts for rate
vintage_curves = (
    day_cumulative
    .join(group_counts, ["MNE", "COHORT", "TST_GRP_CD"])
    .filter(F.col("DAYS_TO_SUCCESS") <= F.col("MEDIAN_WINDOW"))
    .withColumn("RATE", F.col("CUM_CNT") / F.col("CLIENT_CNT") * 100)
    .select(
        "MNE", "COHORT", "TST_GRP_CD",
        F.col("DAYS_TO_SUCCESS").alias("DAY"),
        F.round(F.col("RATE"), 4).alias("VALUE"),
        "CLIENT_CNT",
    )
    .orderBy("MNE", "COHORT", "TST_GRP_CD", "DAY")
)

vintage_collected = vintage_curves.collect()

for r in vintage_collected:
    curve_rows.append({
        "MNE": r.MNE,
        "COHORT": str(r.COHORT),
        "TST_GRP_CD": r.TST_GRP_CD,
        "CURVE_TYPE": "vintage",
        "DAY": int(r.DAY),
        "VALUE": float(r.VALUE),
        "CLIENT_CNT": int(r.CLIENT_CNT),
    })

print(f"  Vintage curve rows: {len(vintage_collected)}")

# Ramp metrics per MNE (from OVERALL action group)
ramp_metrics = {}
for mne in mnes_in_data:
    if mne not in CONFIG:
        continue
    mne_vintage = [r for r in vintage_collected
                   if r.MNE == mne and r.TST_GRP_CD is not None and r.TST_GRP_CD.strip() == ACTION_GROUP]
    if not mne_vintage:
        ramp_metrics[mne] = {"RAMP_50PCT_DAY": None, "RAMP_90PCT_DAY": None}
        continue

    final_rate = max(float(r.VALUE) for r in mne_vintage)
    threshold_50 = final_rate * 0.50
    threshold_90 = final_rate * 0.90

    day_50 = None
    day_90 = None
    for r in sorted(mne_vintage, key=lambda x: x.DAY):
        if day_50 is None and float(r.VALUE) >= threshold_50:
            day_50 = int(r.DAY)
        if day_90 is None and float(r.VALUE) >= threshold_90:
            day_90 = int(r.DAY)

    ramp_metrics[mne] = {"RAMP_50PCT_DAY": day_50, "RAMP_90PCT_DAY": day_90}
    print(f"  {mne}: ramp 50%=day {day_50}, 90%=day {day_90}, final_rate={final_rate:.2f}%")

print(f"\nTotal curve rows so far: {len(curve_rows)}")


# ============================================================
# CELL 6: DYNAMICS — Spending, DiD, Velocity Curves
# ============================================================

print("=" * 60)
print("CELL 6: SPENDING & DiD")
print("=" * 60)

spending_rows = []
did_rows = []

if not HAS_POS:
    print("POS not available — skipping spending analysis")
else:
    # Build dynamic SUCCESS_DT for all campaigns
    all_clients = (
        result_df
        .filter(F.col("MNE").isin(list(CONFIG.keys())))
        .withColumn("SUCCESS_DT", success_dt_expr)
        .select("CLNT_NO", "MNE", "COHORT", "TST_GRP_CD", "SUCCESS",
                "TREATMT_STRT_DT", "TREATMT_END_DT", "WINDOW_DAYS", "SUCCESS_DT")
        .dropDuplicates(["CLNT_NO", "MNE"])
    )

    # PRE period: WINDOW_DAYS before treatment start
    # POST period: treatment start to treatment end
    all_txn = all_clients.join(pos_df, "CLNT_NO", "inner")

    pre_txn = (
        all_txn
        .filter(F.col("TXN_DT") < F.col("TREATMT_STRT_DT"))
        .filter(F.col("TXN_DT") >= F.expr("date_sub(TREATMT_STRT_DT, WINDOW_DAYS)"))
        .withColumn("PERIOD", F.lit("PRE"))
    )

    post_txn = (
        all_txn
        .filter(F.col("TXN_DT") >= F.col("TREATMT_STRT_DT"))
        .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
        .withColumn("PERIOD", F.lit("POST"))
    )

    combined_txn = pre_txn.unionByName(post_txn)

    # Total clients per group (denominator — all clients, not just those with spend)
    client_counts = (
        all_clients
        .groupBy("MNE", "COHORT", "TST_GRP_CD")
        .agg(F.countDistinct("CLNT_NO").alias("total_clients"))
    )

    # Spending aggregation
    spend_agg = (
        combined_txn
        .groupBy("MNE", "COHORT", "TST_GRP_CD", "PERIOD")
        .agg(
            F.countDistinct("CLNT_NO").alias("clients_with_spend"),
            F.count("*").alias("txn_count"),
            F.sum("TXN_AMT").alias("total_spend"),
        )
    )

    spend_joined = (
        spend_agg
        .join(client_counts, ["MNE", "COHORT", "TST_GRP_CD"], "left")
        .withColumn("avg_spend_per_client",
                    F.when(F.col("total_clients") > 0, F.col("total_spend") / F.col("total_clients")).otherwise(0))
        .withColumn("avg_txn_per_client",
                    F.when(F.col("total_clients") > 0, F.col("txn_count") / F.col("total_clients")).otherwise(0))
    )

    spend_collected = spend_joined.collect()
    for r in spend_collected:
        spending_rows.append({
            "MNE": r.MNE, "COHORT": str(r.COHORT), "TST_GRP_CD": r.TST_GRP_CD,
            "PERIOD": r.PERIOD, "TOTAL_CLIENTS": int(r.total_clients),
            "CLIENTS_WITH_SPEND": int(r.clients_with_spend),
            "TXN_COUNT": int(r.txn_count),
            "TOTAL_SPEND": round(float(r.total_spend or 0), 2),
            "AVG_SPEND_PER_CLIENT": round(float(r.avg_spend_per_client or 0), 2),
            "AVG_TXN_PER_CLIENT": round(float(r.avg_txn_per_client or 0), 2),
        })

    print(f"  Spending rows: {len(spending_rows)}")

    # DiD: (Action_post - Action_pre) - (Control_post - Control_pre)
    # Compute per MNE overall
    spend_overall = (
        combined_txn
        .groupBy("MNE", "TST_GRP_CD", "PERIOD")
        .agg(
            F.countDistinct("CLNT_NO").alias("clients_with_spend"),
            F.sum("TXN_AMT").alias("total_spend"),
        )
    )

    client_counts_overall = (
        all_clients
        .groupBy("MNE", "TST_GRP_CD")
        .agg(F.countDistinct("CLNT_NO").alias("total_clients"))
    )

    spend_overall = (
        spend_overall
        .join(client_counts_overall, ["MNE", "TST_GRP_CD"], "left")
        .withColumn("avg_spend",
                    F.when(F.col("total_clients") > 0, F.col("total_spend") / F.col("total_clients")).otherwise(0))
    )

    # Pivot to get PRE/POST side by side
    did_pivot = (
        spend_overall
        .groupBy("MNE", "TST_GRP_CD")
        .pivot("PERIOD", ["PRE", "POST"])
        .agg(F.first("avg_spend").alias("avg_spend"))
        .withColumn("spend_change",
                    F.coalesce(F.col("POST_avg_spend"), F.lit(0)) - F.coalesce(F.col("PRE_avg_spend"), F.lit(0)))
    )

    did_collected = did_pivot.collect()

    for mne in mnes_in_data:
        if mne not in CONFIG:
            continue
        action = [r for r in did_collected if r.MNE == mne and r.TST_GRP_CD is not None and r.TST_GRP_CD.strip() == ACTION_GROUP]
        control = [r for r in did_collected if r.MNE == mne and r.TST_GRP_CD is not None and r.TST_GRP_CD.strip() == CONTROL_GROUP]
        if action and control:
            a_change = float(action[0].spend_change or 0)
            c_change = float(control[0].spend_change or 0)
            did_val = a_change - c_change
            did_rows.append({
                "MNE": mne,
                "ACTION_PRE_SPEND": round(float(action[0].PRE_avg_spend or 0), 2),
                "ACTION_POST_SPEND": round(float(action[0].POST_avg_spend or 0), 2),
                "CONTROL_PRE_SPEND": round(float(control[0].PRE_avg_spend or 0), 2),
                "CONTROL_POST_SPEND": round(float(control[0].POST_avg_spend or 0), 2),
                "DID_SPEND_DELTA": round(did_val, 2),
            })
            print(f"  {mne} DiD: ${did_val:,.2f}/client")

    print(f"  DiD rows: {len(did_rows)}")

    # Post-success spending (SUCCESS=1 clients only)
    print("\n--- Post-Success Spending ---")

    success_clients = (
        all_clients
        .filter(F.col("SUCCESS") == 1)
        .filter(F.col("SUCCESS_DT").isNotNull())
    )

    success_spend = (
        success_clients
        .join(pos_df, "CLNT_NO", "inner")
        .filter(F.col("TXN_DT") >= F.col("SUCCESS_DT"))
        .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
    )

    success_total = (
        success_clients
        .groupBy("MNE", "TST_GRP_CD")
        .agg(F.countDistinct("CLNT_NO").alias("success_clients"))
    )

    success_agg = (
        success_spend
        .groupBy("MNE", "TST_GRP_CD")
        .agg(
            F.countDistinct("CLNT_NO").alias("clients_with_spend"),
            F.count("*").alias("txn_count"),
            F.sum("TXN_AMT").alias("total_spend"),
        )
    )

    post_success_df = (
        success_total
        .join(success_agg, ["MNE", "TST_GRP_CD"], "left")
        .fillna(0, subset=["clients_with_spend", "txn_count", "total_spend"])
        .withColumn("avg_spend_per_success",
                    F.when(F.col("success_clients") > 0, F.col("total_spend") / F.col("success_clients")).otherwise(0))
        .withColumn("avg_txn_per_success",
                    F.when(F.col("success_clients") > 0, F.col("txn_count") / F.col("success_clients")).otherwise(0))
    )

    post_success_collected = post_success_df.collect()
    for r in post_success_collected:
        print(f"  {r.MNE} ({r.TST_GRP_CD.strip()}): {int(r.success_clients)} success clients, "
              f"avg spend=${float(r.avg_spend_per_success or 0):,.2f}")

    # Velocity curves: cumulative avg spend per client per day since success
    print("\n--- Velocity Curves ---")

    velocity_base = (
        success_clients
        .join(pos_df, "CLNT_NO", "inner")
        .filter(F.col("TXN_DT") >= F.col("SUCCESS_DT"))
        .filter(F.col("TXN_DT") <= F.col("TREATMT_END_DT"))
        .withColumn("DAY", F.datediff(F.col("TXN_DT"), F.col("SUCCESS_DT")))
        .filter(F.col("DAY") >= 0)
    )

    velocity_daily = (
        velocity_base
        .groupBy("MNE", "TST_GRP_CD", "DAY")
        .agg(F.sum("TXN_AMT").alias("day_total_spend"))
    )

    w_vel = (
        Window
        .partitionBy("MNE", "TST_GRP_CD")
        .orderBy("DAY")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    velocity_cum = velocity_daily.withColumn("cum_spend", F.sum("day_total_spend").over(w_vel))

    velocity_denoms = (
        success_clients
        .groupBy("MNE", "TST_GRP_CD")
        .agg(F.countDistinct("CLNT_NO").alias("success_client_cnt"))
    )

    velocity_result = (
        velocity_cum
        .join(velocity_denoms, ["MNE", "TST_GRP_CD"])
        .withColumn("avg_cum_spend", F.col("cum_spend") / F.col("success_client_cnt"))
        .filter(F.col("DAY") <= 90)
        .select("MNE", "TST_GRP_CD", "DAY",
                F.round(F.col("avg_cum_spend"), 4).alias("VALUE"),
                F.col("success_client_cnt").alias("CLIENT_CNT"))
        .orderBy("MNE", "TST_GRP_CD", "DAY")
    )

    velocity_collected = velocity_result.collect()
    for r in velocity_collected:
        curve_rows.append({
            "MNE": r.MNE,
            "COHORT": "ALL",
            "TST_GRP_CD": r.TST_GRP_CD,
            "CURVE_TYPE": "velocity",
            "DAY": int(r.DAY),
            "VALUE": float(r.VALUE),
            "CLIENT_CNT": int(r.CLIENT_CNT),
        })

    print(f"  Velocity curve rows: {len(velocity_collected)}")

print(f"\nTotal curve rows: {len(curve_rows)}")


# ============================================================
# CELL 7: PROFILE (UCP segments + decision tree) [optional]
# ============================================================

print("=" * 60)
print("CELL 7: PROFILING")
print("=" * 60)

profile_rows = []

if not HAS_UCP:
    print("UCP not available — skipping profiling")
else:
    try:
        # Reload UCP with full columns
        ucp_full = spark.read.parquet(UCP_PATH.format(date=latest_ucp_date))
        ucp_full = (
            ucp_full
            .withColumn("CLNT_NO", F.regexp_replace(F.trim(F.col("CLNT_NO")), "^0+", ""))
        )

        # Select useful columns (age, tenure, income if available)
        ucp_cols = [c.upper() for c in ucp_full.columns]
        age_col = None
        tenure_col = None
        income_col = None

        for c in ucp_full.columns:
            cu = c.upper()
            if "AGE" in cu and age_col is None:
                age_col = c
            if "TENURE" in cu and tenure_col is None:
                tenure_col = c
            if "INCOME" in cu or "REVENUE" in cu and income_col is None:
                income_col = c

        if age_col:
            print(f"  Age column: {age_col}")
        if tenure_col:
            print(f"  Tenure column: {tenure_col}")
        if income_col:
            print(f"  Income column: {income_col}")

        # Join to result_df
        ucp_select_cols = ["CLNT_NO"]
        if age_col:
            ucp_select_cols.append(age_col)
        if tenure_col:
            ucp_select_cols.append(tenure_col)
        if income_col:
            ucp_select_cols.append(income_col)

        ucp_slim = ucp_full.select(*ucp_select_cols).dropDuplicates(["CLNT_NO"])

        enriched = (
            result_df
            .filter(F.col("MNE").isin(list(CONFIG.keys())))
            .join(ucp_slim, "CLNT_NO", "left")
        )

        # Age bucketing
        if age_col:
            enriched = enriched.withColumn(
                "AGE_BUCKET",
                F.when(F.col(age_col).cast("int") < 25, "18-24")
                .when(F.col(age_col).cast("int") < 35, "25-34")
                .when(F.col(age_col).cast("int") < 45, "35-44")
                .when(F.col(age_col).cast("int") < 55, "45-54")
                .when(F.col(age_col).cast("int") < 65, "55-64")
                .otherwise("65+")
            )

            # Effectiveness per age bucket
            age_agg = (
                enriched
                .groupBy("MNE", "AGE_BUCKET", "TST_GRP_CD")
                .agg(
                    F.countDistinct("CLNT_NO").alias("clients"),
                    F.sum("SUCCESS").alias("successes"),
                )
                .collect()
            )

            for mne in mnes_in_data:
                if mne not in CONFIG:
                    continue
                buckets = sorted(set(r.AGE_BUCKET for r in age_agg if r.MNE == mne and r.AGE_BUCKET is not None))
                for bucket in buckets:
                    action = [r for r in age_agg if r.MNE == mne and r.AGE_BUCKET == bucket
                              and r.TST_GRP_CD is not None and r.TST_GRP_CD.strip() == ACTION_GROUP]
                    control = [r for r in age_agg if r.MNE == mne and r.AGE_BUCKET == bucket
                               and r.TST_GRP_CD is not None and r.TST_GRP_CD.strip() == CONTROL_GROUP]
                    if not action or not control:
                        continue
                    a = action[0]
                    c = control[0]
                    a_clients = int(a.clients)
                    c_clients = int(c.clients)
                    a_successes = int(a.successes or 0)
                    c_successes = int(c.successes or 0)
                    a_rate = a_successes / a_clients if a_clients > 0 else 0
                    c_rate = c_successes / c_clients if c_clients > 0 else 0
                    pvalue, lift_pp, ci_lower, ci_upper = _ztest_pvalue(a_successes, a_clients, c_successes, c_clients)

                    detail_rows.append({
                        "MNE": mne, "GRAIN": "AGE", "SLICE": bucket,
                        "CLIENTS_ACTION": a_clients, "CLIENTS_CONTROL": c_clients,
                        "SUCCESSES_ACTION": a_successes, "SUCCESSES_CONTROL": c_successes,
                        "RATE_ACTION": round(a_rate * 100, 4), "RATE_CONTROL": round(c_rate * 100, 4),
                        "LIFT_PP": round(lift_pp, 4), "REL_LIFT_PCT": round((lift_pp / (c_rate * 100) * 100) if c_rate > 0 else 0, 2),
                        "PVALUE": round(pvalue, 8), "STARS": _stars(pvalue),
                        "CI_LOWER": round(ci_lower, 4), "CI_UPPER": round(ci_upper, 4),
                        "INCREMENTAL": round(a_clients * lift_pp / 100, 1),
                    })

            print(f"  Age bucket rows added to detail_rows")

        # Decision tree per MNE (feature importance)
        try:
            from pyspark.ml.feature import VectorAssembler, StringIndexer
            from pyspark.ml.classification import DecisionTreeClassifier

            # Build features: contact_count + channel + age if available
            contact_counts = (
                result_df
                .filter(F.trim(F.col("TST_GRP_CD")) == ACTION_GROUP)
                .groupBy("MNE", "CLNT_NO")
                .agg(F.count("*").alias("contact_count"))
            )

            tree_base = (
                enriched
                .filter(F.trim(F.col("TST_GRP_CD")) == ACTION_GROUP)
                .select("MNE", "CLNT_NO", "SUCCESS", "TACTIC_CELL_CD")
                .dropDuplicates(["MNE", "CLNT_NO"])
                .join(contact_counts, ["MNE", "CLNT_NO"], "left")
            )

            if age_col:
                tree_base = tree_base.join(
                    ucp_slim.select("CLNT_NO", F.col(age_col).cast("double").alias("AGE_NUM")),
                    "CLNT_NO", "left"
                ).fillna(0, subset=["AGE_NUM"])

            tree_base = tree_base.fillna(0, subset=["contact_count"])
            tree_base = tree_base.fillna("UNKNOWN", subset=["TACTIC_CELL_CD"])

            # Index channel
            channel_indexer = StringIndexer(inputCol="TACTIC_CELL_CD", outputCol="channel_idx", handleInvalid="keep")

            feature_cols = ["contact_count", "channel_idx"]
            if age_col:
                feature_cols.append("AGE_NUM")

            for mne in mnes_in_data:
                if mne not in CONFIG:
                    continue
                mne_tree = tree_base.filter(F.col("MNE") == mne)
                cnt = mne_tree.count()
                if cnt < 100:
                    continue

                try:
                    indexed = channel_indexer.fit(mne_tree).transform(mne_tree)
                    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
                    assembled = assembler.transform(indexed).select("features", F.col("SUCCESS").cast("double").alias("label"))

                    dt = DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=4)
                    model = dt.fit(assembled)

                    importances = model.featureImportances.toArray()
                    for i, col_name in enumerate(feature_cols):
                        if i < len(importances):
                            profile_rows.append({
                                "MNE": mne,
                                "FEATURE": col_name,
                                "IMPORTANCE": round(float(importances[i]), 4),
                            })

                    top_feat = feature_cols[importances.argmax()] if len(importances) > 0 else "N/A"
                    print(f"  {mne}: top feature = {top_feat} ({importances.max():.3f})")
                except Exception as e:
                    print(f"  {mne} tree failed: {e}")

        except ImportError:
            print("  MLlib not available — skipping decision tree")
        except Exception as e:
            print(f"  Decision tree section failed: {e}")

    except Exception as e:
        print(f"  UCP profiling failed: {e}")

print(f"\nProfile rows: {len(profile_rows)}")
print(f"Total detail_rows: {len(detail_rows)}")


# ============================================================
# CELL 8: SCORECARD ASSEMBLY
# ============================================================

print("=" * 60)
print("CELL 8: SCORECARD")
print("=" * 60)

scorecard_rows = []

for mne in mnes_in_data:
    if mne not in CONFIG:
        continue

    # Effectiveness
    ov = [r for r in detail_rows if r["MNE"] == mne and r["GRAIN"] == "OVERALL"]
    if not ov:
        continue
    ov = ov[0]

    # Credibility
    cred = [r for r in credibility_rows if r["MNE"] == mne]
    cred = cred[0] if cred else {}

    # Efficiency
    eff = [r for r in efficiency_rows if r["MNE"] == mne]
    eff = eff[0] if eff else {}

    # Ramp
    ramp = ramp_metrics.get(mne, {})

    # DiD
    did = [r for r in did_rows if r["MNE"] == mne]
    did = did[0] if did else {}

    # Post-success spending
    post_success_action = [r for r in (post_success_collected if HAS_POS else [])
                           if r.MNE == mne and r.TST_GRP_CD is not None and r.TST_GRP_CD.strip() == ACTION_GROUP]
    post_success_avg = round(float(post_success_action[0].avg_spend_per_success or 0), 2) if post_success_action else None

    # Profile: top feature
    mne_profile = [r for r in profile_rows if r["MNE"] == mne]
    if mne_profile:
        top_feat_row = max(mne_profile, key=lambda x: x["IMPORTANCE"])
        top_feature = top_feat_row["FEATURE"]
        top_feature_imp = top_feat_row["IMPORTANCE"]
    else:
        top_feature = None
        top_feature_imp = None

    scorecard_rows.append({
        "MNE": mne,
        "LIFT_PP": ov["LIFT_PP"],
        "PVALUE": ov["PVALUE"],
        "STARS": ov["STARS"],
        "RATE_ACTION": ov["RATE_ACTION"],
        "RATE_CONTROL": ov["RATE_CONTROL"],
        "INCREMENTAL_CLIENTS": ov["INCREMENTAL"],
        "SRM_STATUS": cred.get("SRM_STATUS"),
        "SRM_PVALUE": cred.get("SRM_PVALUE"),
        "COHORT_CV": cred.get("COHORT_CV"),
        "STABILITY_RATING": cred.get("STABILITY_RATING"),
        "TOTAL_DEPLOYMENTS": eff.get("TOTAL_DEPLOYMENTS"),
        "AVG_CONTACTS": eff.get("AVG_CONTACTS"),
        "DEPLOYS_PER_INCREMENTAL": eff.get("DEPLOYS_PER_INCREMENTAL"),
        "RAMP_50PCT_DAY": ramp.get("RAMP_50PCT_DAY"),
        "RAMP_90PCT_DAY": ramp.get("RAMP_90PCT_DAY"),
        "DID_SPEND_DELTA": did.get("DID_SPEND_DELTA"),
        "POST_SUCCESS_AVG_SPEND": post_success_avg,
        "TOP_FEATURE": top_feature,
        "TOP_FEATURE_IMPORTANCE": top_feature_imp,
    })

# Display
for s in scorecard_rows:
    print(f"\n{s['MNE']}:")
    print(f"  Lift: {s['LIFT_PP']:.2f}pp {s['STARS']} (p={s['PVALUE']:.4f})")
    print(f"  Incremental: {s['INCREMENTAL_CLIENTS']:.0f} clients")
    print(f"  SRM: {s['SRM_STATUS']} | Stability: {s['STABILITY_RATING']} (CV={s['COHORT_CV']})")
    print(f"  Efficiency: {s['AVG_CONTACTS']} avg contacts, {s['DEPLOYS_PER_INCREMENTAL']} deploys/inc")
    print(f"  Ramp: 50% at day {s['RAMP_50PCT_DAY']}, 90% at day {s['RAMP_90PCT_DAY']}")
    if s["DID_SPEND_DELTA"] is not None:
        print(f"  DiD: ${s['DID_SPEND_DELTA']:,.2f}/client")
    if s["POST_SUCCESS_AVG_SPEND"] is not None:
        print(f"  Post-success avg spend: ${s['POST_SUCCESS_AVG_SPEND']:,.2f}")
    if s["TOP_FEATURE"]:
        print(f"  Top feature: {s['TOP_FEATURE']} ({s['TOP_FEATURE_IMPORTANCE']:.3f})")

print(f"\nScorecard: {len(scorecard_rows)} campaigns")


# ============================================================
# CELL 9: EXPORT
# ============================================================

print("=" * 60)
print("CELL 9: EXPORT")
print("=" * 60)

import pandas as pd

# 1. Scorecard
scorecard_pdf = pd.DataFrame(scorecard_rows)
print(f"Scorecard: {len(scorecard_pdf)} rows, {list(scorecard_pdf.columns)}")
download_csv(scorecard_pdf, "vvd_evaluation_scorecard.csv")

# 2. Detail (effectiveness + frequency + profile grains)
detail_pdf = pd.DataFrame(detail_rows)
print(f"Detail: {len(detail_pdf)} rows, grains={detail_pdf['GRAIN'].unique().tolist()}")
download_csv(detail_pdf, "vvd_evaluation_detail.csv")

# 3. Curves (vintage + velocity + diminishing returns)
curves_pdf = pd.DataFrame(curve_rows)
print(f"Curves: {len(curves_pdf)} rows, types={curves_pdf['CURVE_TYPE'].unique().tolist()}")
download_csv(curves_pdf, "vvd_evaluation_curves.csv")

print("\nExport complete.")


# ============================================================
# CELL 10: CLEANUP
# ============================================================

if HAS_POS:
    pos_df.unpersist()
    print("pos_df unpersisted")

result_df.unpersist()
print("result_df unpersisted")
print("Done.")
