"""
Track B — Native Editable PowerPoint Charts
Charts contain underlying data; stakeholders can click to edit.
Uses CategoryChartData + XL_CHART_TYPE.
"""

import csv
from collections import defaultdict
from pptx.chart.data import CategoryChartData
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor

from theme import (
    ACTION_COLOR, CONTROL_COLOR, DARK_BLUE, BRIGHT_BLUE,
    CHART_COLORS, FONT_FAMILY, FONT_SMALL, FONT_BODY,
)


def read_csv(path):
    """Read CSV into list of dicts."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def style_chart(chart, title=None, legend_pos=XL_LEGEND_POSITION.BOTTOM):
    """Apply consistent styling to any chart."""
    if title:
        chart.has_title = True
        chart.chart_title.text_frame.text = title
        chart.chart_title.text_frame.paragraphs[0].font.size = Pt(12)
        chart.chart_title.text_frame.paragraphs[0].font.color.rgb = DARK_BLUE
        chart.chart_title.text_frame.paragraphs[0].font.name = FONT_FAMILY
        chart.chart_title.text_frame.paragraphs[0].font.bold = True

    chart.has_legend = True
    chart.legend.position = legend_pos
    chart.legend.include_in_layout = False
    chart.legend.font.size = FONT_SMALL
    chart.legend.font.name = FONT_FAMILY


def style_series_colors(chart, colors=None):
    """Apply brand colors to chart series."""
    if colors is None:
        colors = CHART_COLORS
    for i, series in enumerate(chart.series):
        color = colors[i % len(colors)]
        series.format.line.color.rgb = color
        series.format.line.width = Pt(2)
        if hasattr(series, "format") and hasattr(series.format, "fill"):
            try:
                series.format.fill.solid()
                series.format.fill.fore_color.rgb = color
            except Exception:
                pass


def style_action_control(chart):
    """Style a 2-series chart as Action (solid) vs Control (dashed)."""
    if len(chart.series) >= 2:
        # Action = series 0
        chart.series[0].format.line.color.rgb = ACTION_COLOR
        chart.series[0].format.line.width = Pt(2.5)
        # Control = series 1
        chart.series[1].format.line.color.rgb = CONTROL_COLOR
        chart.series[1].format.line.width = Pt(2)
        chart.series[1].format.line.dash_style = 4  # XL_LINE_DASH.DASH


# ── Vintage Curve (Line Chart) ───────────────────────────────────

def add_vintage_curve(slide, vintage_data, mne, metric="PRIMARY",
                      left=Inches(0.3), top=Inches(1.5),
                      width=Inches(6), height=Inches(3)):
    """
    Add a vintage curve line chart (Action vs Control over days).

    vintage_data: list of dicts from vintage_curves.csv
    Filters to specific MNE + METRIC, aggregates across cohorts.
    """
    # Filter to this campaign + metric
    rows = [r for r in vintage_data
            if r["MNE"] == mne and r["METRIC"] == metric]

    if not rows:
        return None

    # Separate Action (TG4) and Control (TG7)
    action_rows = sorted(
        [r for r in rows if r["TST_GRP_CD"].strip() == "TG4"],
        key=lambda r: int(r["DAY"])
    )
    control_rows = sorted(
        [r for r in rows if r["TST_GRP_CD"].strip() == "TG7"],
        key=lambda r: int(r["DAY"])
    )

    if not action_rows:
        return None

    # Aggregate across cohorts: average rate per day
    action_by_day = defaultdict(list)
    control_by_day = defaultdict(list)

    for r in action_rows:
        action_by_day[int(r["DAY"])].append(float(r["RATE"]))
    for r in control_rows:
        control_by_day[int(r["DAY"])].append(float(r["RATE"]))

    days = sorted(action_by_day.keys())

    chart_data = CategoryChartData()
    chart_data.categories = [str(d) for d in days]
    chart_data.add_series("Action (TG4)", [
        sum(action_by_day[d]) / len(action_by_day[d]) for d in days
    ])
    if control_by_day:
        chart_data.add_series("Control (TG7)", [
            sum(control_by_day.get(d, [0])) / max(len(control_by_day.get(d, [1])), 1)
            for d in days
        ])

    chart_frame = slide.shapes.add_chart(
        XL_CHART_TYPE.LINE_MARKERS, left, top, width, height, chart_data
    )
    chart = chart_frame.chart

    style_chart(chart, title=f"{mne} — Vintage Curve ({metric.title()})")
    style_action_control(chart)

    # Reduce category axis clutter for 30-90 day series
    try:
        cat_axis = chart.category_axis
        cat_axis.tick_labels.font.size = Pt(8)
        cat_axis.tick_labels.font.name = FONT_FAMILY
        if len(days) > 15:
            cat_axis.tick_label_position = 1  # LOW
    except Exception:
        pass

    try:
        val_axis = chart.value_axis
        val_axis.tick_labels.font.size = Pt(8)
        val_axis.tick_labels.font.name = FONT_FAMILY
    except Exception:
        pass

    return chart_frame


# ── Lift Bar Chart ───────────────────────────────────────────────

def add_lift_bar_chart(slide, summary_data, mnes=None,
                       left=Inches(0.3), top=Inches(1.5),
                       width=Inches(6), height=Inches(3)):
    """
    Add a clustered bar chart showing lift by campaign.

    summary_data: list of dicts from campaign_summary.csv
    Columns: MNE, TST_GRP_CD, lift, p_value, etc.
    """
    if mnes is None:
        mnes = ["VCN", "VDA", "VDT", "VUI", "VUT", "VAW"]

    # Filter to Action group lift
    rows = [r for r in summary_data
            if r["MNE"] in mnes and r["TST_GRP_CD"].strip() == "TG4"]
    rows.sort(key=lambda r: mnes.index(r["MNE"]))

    if not rows:
        return None

    chart_data = CategoryChartData()
    chart_data.categories = [r["MNE"] for r in rows]
    chart_data.add_series("Lift (pp)", [float(r["lift"]) for r in rows])

    chart_frame = slide.shapes.add_chart(
        XL_CHART_TYPE.COLUMN_CLUSTERED, left, top, width, height, chart_data
    )
    chart = chart_frame.chart

    style_chart(chart, title="Campaign Lift (Action vs Control)")

    # Color bars by sign: positive = tundra, negative = sunburst
    from pptx.oxml.ns import qn
    series = chart.series[0]
    for i, row in enumerate(rows):
        lift = float(row["lift"])
        pt = series.points[i]
        pt.format.fill.solid()
        if lift >= 0:
            pt.format.fill.fore_color.rgb = RGBColor(0x07, 0xAF, 0xBF)  # TUNDRA
        else:
            pt.format.fill.fore_color.rgb = RGBColor(0xFC, 0xA3, 0x11)  # SUNBURST

    return chart_frame


# ── Multi-Series Line (e.g., cohort breakdown) ──────────────────

def add_multi_series_line(slide, vintage_data, mne, metric="PRIMARY",
                          group="TG4",
                          left=Inches(0.3), top=Inches(1.5),
                          width=Inches(6), height=Inches(3)):
    """
    Add a line chart with one series per cohort for a specific MNE + group.
    Shows how different cohorts perform over time.
    """
    rows = [r for r in vintage_data
            if r["MNE"] == mne
            and r["METRIC"] == metric
            and r["TST_GRP_CD"].strip() == group]

    if not rows:
        return None

    # Group by cohort
    cohorts = defaultdict(list)
    for r in rows:
        cohorts[r["COHORT"]].append(r)

    # Get union of all days
    all_days = sorted(set(int(r["DAY"]) for r in rows))

    chart_data = CategoryChartData()
    chart_data.categories = [str(d) for d in all_days]

    for cohort in sorted(cohorts.keys()):
        day_rate = {int(r["DAY"]): float(r["RATE"]) for r in cohorts[cohort]}
        chart_data.add_series(cohort, [day_rate.get(d, None) for d in all_days])

    chart_frame = slide.shapes.add_chart(
        XL_CHART_TYPE.LINE, left, top, width, height, chart_data
    )
    chart = chart_frame.chart
    style_chart(chart, title=f"{mne} — {group} Vintage by Cohort")
    style_series_colors(chart)

    return chart_frame


# ── Success Rate Comparison (Grouped Bar) ────────────────────────

def add_rate_comparison(slide, summary_data, mnes=None,
                        left=Inches(0.3), top=Inches(1.5),
                        width=Inches(6), height=Inches(3)):
    """
    Grouped bar: Action rate vs Control rate per campaign.
    """
    if mnes is None:
        mnes = ["VCN", "VDA", "VDT", "VUI", "VUT", "VAW"]

    action_rows = {r["MNE"]: r for r in summary_data
                   if r["TST_GRP_CD"].strip() == "TG4" and r["MNE"] in mnes}
    control_rows = {r["MNE"]: r for r in summary_data
                    if r["TST_GRP_CD"].strip() == "TG7" and r["MNE"] in mnes}

    present_mnes = [m for m in mnes if m in action_rows]
    if not present_mnes:
        return None

    chart_data = CategoryChartData()
    chart_data.categories = present_mnes
    chart_data.add_series("Action (TG4)", [
        float(action_rows[m]["success_rate"]) for m in present_mnes
    ])
    chart_data.add_series("Control (TG7)", [
        float(control_rows[m]["success_rate"]) if m in control_rows else 0
        for m in present_mnes
    ])

    chart_frame = slide.shapes.add_chart(
        XL_CHART_TYPE.COLUMN_CLUSTERED, left, top, width, height, chart_data
    )
    chart = chart_frame.chart
    style_chart(chart, title="Success Rate — Action vs Control")
    style_action_control(chart)

    return chart_frame
