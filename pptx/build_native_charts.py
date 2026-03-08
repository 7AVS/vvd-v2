# build_native_charts.py
# Builds PowerPoint with native editable line charts (right-click -> Edit Data to see tables).
#
# Dependencies: pip install python-pptx
#
# Input file:
#   - vintage_curves.csv (REQUIRED) — from pipeline Cell 10
#     Columns: MNE, COHORT, TST_GRP_CD, RPT_GRP_CD, METRIC, DAY, WINDOW_DAYS, CLIENT_CNT, SUCCESS_CNT, RATE
#     RATE is already cumulative (SUCCESS_CNT/CLIENT_CNT * 100). Used directly as Y value.
#
# Output: VVD_v2_NativeCharts.pptx

import csv
import math
from collections import defaultdict

from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION, XL_MARKER_STYLE, XL_TICK_LABEL_POSITION, XL_TICK_MARK
from pptx.enum.dml import MSO_LINE_DASH_STYLE
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor

# ============================================================
# EDIT THESE PATHS before running
# ============================================================
INPUT_VINTAGE = r"\\maple.FG.RBC.com\data\toronto\WRKGRP\WRGroup16\Marketing Services In% Transformation\Marketing Analytics\CPB\Payment\VDD Vintage\VDD PowerPack 2026 Mar\Data Source\PVD_P2_vintage_curves.csv"
OUTPUT_PPTX   = r"\\maple.FG.RBC.com\data\toronto\WRKGRP\WRGroup16\Marketing Services In% Transformation\Marketing Analytics\CPB\Payment\VDD Vintage\VDD PowerPack 2026 Mar\VVD_v2_NativeCharts.pptx"
# ============================================================

# --- campaign metadata ---
CAMPAIGNS = ["VCN", "VDA", "VDT", "VUI", "VUT", "VAW"]
CAMPAIGN_META = {
    "VCN": ("Contextual Notification", "Card Acquisition"),
    "VDA": ("Seasonal Acquisition", "Card Acquisition"),
    "VDT": ("Activation Trigger", "Card Activation"),
    "VUI": ("Usage Trigger", "Card Usage"),
    "VUT": ("Tokenization", "Wallet Provisioning"),
    "VAW": ("Add To Wallet", "Wallet Provisioning"),
}
PRIMARY_METRIC = {
    "VCN": "card_acquisition",
    "VDA": "card_acquisition",
    "VDT": "card_activation",
    "VUI": "card_usage",
    "VUT": "wallet_provisioning",
    "VAW": "wallet_provisioning",
}
MAX_DAYS = {"VDA": 30, "VDT": 30, "VAW": 30}

# --- color palette ---
COHORT_COLORS_RANKED = [
    RGBColor(0, 49, 104),    # rank 1 (most recent) — darkest
    RGBColor(0, 106, 198),   # rank 2 — bright blue
    RGBColor(91, 155, 213),  # rank 3 — medium blue
]
COHORT_COLOR_OLD = RGBColor(190, 210, 230)


def get_cohort_color(cohort, cohorts_sorted):
    rank = len(cohorts_sorted) - 1 - cohorts_sorted.index(cohort)  # 0 = most recent
    if rank < len(COHORT_COLORS_RANKED):
        return COHORT_COLORS_RANKED[rank]
    return COHORT_COLOR_OLD


def sig_label(p):
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    return "n.s."


def z_test_proportions(rate_a, n_a, rate_b, n_b):
    """Two-proportion z-test. Returns p-value using math.erfc (no scipy)."""
    p_a = rate_a / 100.0
    p_b = rate_b / 100.0
    p_pool = (p_a * n_a + p_b * n_b) / (n_a + n_b)
    if p_pool <= 0 or p_pool >= 1:
        return 1.0
    se = math.sqrt(p_pool * (1 - p_pool) * (1.0 / n_a + 1.0 / n_b))
    if se == 0:
        return 1.0
    z = abs(p_a - p_b) / se
    return math.erfc(z / math.sqrt(2))


# --- load vintage curves, filtering to primary metric only ---
# key: (MNE, COHORT, TST_GRP_CD) -> sorted list of (day, rate, client_cnt)
raw = defaultdict(list)
with open(INPUT_VINTAGE, newline="") as f:
    for row in csv.DictReader(f):
        mne = row["MNE"]
        if row["METRIC"] != PRIMARY_METRIC.get(mne, ""):
            continue
        key = (mne, row["COHORT"], row["TST_GRP_CD"])
        raw[key].append((int(row["DAY"]), float(row["RATE"]), int(row["CLIENT_CNT"])))

# RATE is already cumulative — use directly
curves = {}
for key, records in raw.items():
    records.sort(key=lambda r: r[0])
    curves[key] = [(day, rate, client_cnt) for day, rate, client_cnt in records]

# auto-detect cohorts per campaign, sorted chronologically
campaign_cohorts = defaultdict(set)
for (mne, cohort, grp) in curves:
    campaign_cohorts[mne].add(cohort)
for mne in campaign_cohorts:
    campaign_cohorts[mne] = sorted(campaign_cohorts[mne])

# --- compute lift and p-value from latest cohort's final day ---
summary = {}
for mne in CAMPAIGNS:
    cohorts = campaign_cohorts.get(mne, [])
    if not cohorts:
        summary[mne] = {"lift": 0.0, "p_value": 1.0}
        continue
    latest = cohorts[-1]
    max_day = MAX_DAYS.get(mne, 90)
    action_key = (mne, latest, "TG4")
    control_key = (mne, latest, "TG7")
    action_pts = [(d, r, n) for d, r, n in curves.get(action_key, []) if d <= max_day]
    control_pts = [(d, r, n) for d, r, n in curves.get(control_key, []) if d <= max_day]
    if action_pts and control_pts:
        _, rate_a, n_a = action_pts[-1]
        _, rate_b, n_b = control_pts[-1]
        lift = rate_a - rate_b
        p_val = z_test_proportions(rate_a, n_a, rate_b, n_b)
    else:
        lift, p_val = 0.0, 1.0
    summary[mne] = {"lift": lift, "p_value": p_val}

# --- build presentation ---
prs = Presentation()
prs.slide_width = Inches(13.333)
prs.slide_height = Inches(7.5)

# --- title slide ---
slide = prs.slides.add_slide(prs.slide_layouts[6])  # blank
txbox = slide.shapes.add_textbox(Inches(1), Inches(2.5), Inches(11), Inches(2))
tf = txbox.text_frame
tf.word_wrap = True
p = tf.paragraphs[0]
p.text = "VVD v2 — Vintage Curve Analysis"
p.font.size = Pt(36)
p.font.bold = True
p.font.color.rgb = RGBColor(0, 49, 104)
p.alignment = 1  # CENTER

p2 = tf.add_paragraph()
p2.text = "Campaign Performance by Cohort"
p2.font.size = Pt(20)
p2.font.color.rgb = RGBColor(100, 100, 100)
p2.alignment = 1

# --- one slide per campaign ---
for mne in CAMPAIGNS:
    info = summary.get(mne, {"lift": 0, "p_value": 1})
    lift = info["lift"]
    p_val = info["p_value"]
    sig = sig_label(p_val)
    full_name, metric = CAMPAIGN_META[mne]
    cohorts = campaign_cohorts.get(mne, [])

    lift_str = f"+{lift:.2f}" if lift >= 0 else f"{lift:.2f}"
    title_text = f"{mne} — {full_name} | {metric} | {lift_str}pp {sig}"

    slide = prs.slides.add_slide(prs.slide_layouts[6])

    # title
    ttl = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(12.3), Inches(0.7))
    tf = ttl.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = title_text
    p.font.size = Pt(22)
    p.font.bold = True
    p.font.color.rgb = RGBColor(0, 49, 104)

    max_day = MAX_DAYS.get(mne, 90)
    for cohort in cohorts:
        for grp in ["TG4", "TG7"]:
            key = (mne, cohort, grp)
            if key in curves:
                curves[key] = [(d, r, n) for d, r, n in curves[key] if d <= max_day]

    categories = [str(d) for d in range(max_day + 1)]

    chart_data = CategoryChartData()
    chart_data.categories = categories

    series_order = []
    for cohort in cohorts:
        for grp, label in [("TG4", "Action"), ("TG7", "Control")]:
            series_name = f"{cohort} {label}"
            key = (mne, cohort, grp)
            if key in curves:
                day_map = {str(d): r for d, r, n in curves[key]}
                values = [day_map.get(str(d), None) for d in range(max_day + 1)]
            else:
                values = [None] * (max_day + 1)
            chart_data.add_series(series_name, values)
            series_order.append((cohort, grp))

    chart = slide.shapes.add_chart(
        XL_CHART_TYPE.LINE,
        Inches(0.5), Inches(1.2), Inches(12.3), Inches(5.8),
        chart_data,
    ).chart

    chart.has_legend = True
    chart.legend.position = XL_LEGEND_POSITION.BOTTOM
    chart.legend.include_in_layout = False
    chart.legend.font.size = Pt(9)

    # axis formatting
    cat_axis = chart.category_axis
    cat_axis.has_title = True
    cat_axis.axis_title.text_frame.paragraphs[0].text = "Days Since Treatment"
    cat_axis.axis_title.text_frame.paragraphs[0].font.size = Pt(11)
    cat_axis.tick_labels.font.size = Pt(8)
    cat_axis.tick_label_position = XL_TICK_LABEL_POSITION.LOW
    cat_axis.major_tick_mark = XL_TICK_MARK.OUTSIDE
    cat_axis.minor_tick_mark = XL_TICK_MARK.NONE

    val_axis = chart.value_axis
    val_axis.has_title = True
    val_axis.axis_title.text_frame.paragraphs[0].text = "Cumulative Success Rate (%)"
    val_axis.axis_title.text_frame.paragraphs[0].font.size = Pt(11)
    val_axis.tick_labels.font.size = Pt(9)
    val_axis.has_major_gridlines = True
    val_axis.has_minor_gridlines = False
    val_axis.major_gridlines.format.line.color.rgb = RGBColor(210, 210, 210)
    val_axis.major_gridlines.format.line.width = Pt(0.5)

    # style each series
    plot = chart.plots[0]
    plot.has_data_labels = False

    for idx, (cohort, grp) in enumerate(series_order):
        series = plot.series[idx]
        series.smooth = True

        color = get_cohort_color(cohort, cohorts)
        series.format.line.color.rgb = color
        series.format.line.width = Pt(1.5)
        series.format.line.dash_style = MSO_LINE_DASH_STYLE.DASH if grp == "TG7" else MSO_LINE_DASH_STYLE.SOLID
        series.marker.style = XL_MARKER_STYLE.NONE

prs.save(OUTPUT_PPTX)
print(f"Saved: {OUTPUT_PPTX}")
