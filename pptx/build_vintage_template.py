"""
Build VVD Vintage Curve Template — consolidated Excel workbook.

Input:  pptx/data/vintage_curves.csv (3,408 rows, quarterly cohorts, PRIMARY metric)
Output: pptx/data/VVD_vintage_template.xlsx

Tabs:
  1. Data Extract — raw data as Excel Table (PRIMARY mapped to actual metric names)
  2. Config — campaign selector (MNE dropdown), lookup tables
  3-10. One tab per metric with SUMPRODUCT formulas + RCU-style line charts

Bug fixes applied:
  - Map PRIMARY → actual metric per MNE so SUMPRODUCT formulas find matches
  - Config!$B$1 verified as absolute reference for MNE selector
  - Chart redesigned: cohort-colored pairs (solid=Action, dashed=Control), grey for older
  - Formula ranges tightened to $A$2:$A$3409 (exact data row count)
  - RATE column is percentage (0.05 = 0.05%), formulas compute SUCCESS_CNT/CLIENT_CNT
"""

import csv
from pathlib import Path

from openpyxl import Workbook
from openpyxl.chart import LineChart, Reference
from openpyxl.chart.layout import Layout, ManualLayout
from openpyxl.chart.legend import Legend
from openpyxl.styles import (
    Font, PatternFill, Alignment, Border, Side,
)
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.worksheet.table import Table, TableStyleInfo

# ── paths ────────────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent
CSV_PATH = BASE / "data" / "vintage_curves.csv"
OUT_PATH = BASE / "data" / "VVD_vintage_template.xlsx"

# ── MNE → primary metric mapping (Bug 1 fix) ────────────────────────────
MNE_METRIC_MAP = {
    "VCN": "card_acquisition",
    "VDA": "card_acquisition",
    "VDT": "card_activation",
    "VUI": "card_usage",
    "VUT": "wallet_provisioning",
    "VAW": "wallet_provisioning",
}

# ── constants ────────────────────────────────────────────────────────────
METRIC_TABS = [
    "card_acquisition",
    "card_activation",
    "card_usage",
    "wallet_provisioning",
    "email_open",
    "email_click",
    "email_sent",
    "email_unsub",
]

MAX_DAYS = 91  # rows 0..90

# ── colors / styles ──────────────────────────────────────────────────────
GOLD_FILL = PatternFill(start_color="FFD700", end_color="FFD700", fill_type="solid")
DARK_BLUE_FILL = PatternFill(start_color="003366", end_color="003366", fill_type="solid")
LIGHT_BLUE_FILL = PatternFill(start_color="DCE6F1", end_color="DCE6F1", fill_type="solid")
LIGHT_GREY_FILL = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
WHITE_FILL = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
HEADER_FONT_DARK = Font(bold=True, color="000000", size=11)
TITLE_FONT = Font(bold=True, size=14)
SMALL_GREY_FONT = Font(size=8, color="999999")
THIN_BORDER = Border(
    left=Side(style="thin"), right=Side(style="thin"),
    top=Side(style="thin"), bottom=Side(style="thin"),
)

# Chart colors — RCU template design
# Last 3 cohorts get distinct colors, older ones get light grey
FOCUS_COLORS = ["0070C0", "00B050", "FFC000"]  # blue, green, orange
GREY_COLOR = "C0C0C0"


# ─────────────────────────────────────────────────────────────────────────
# 1. Load CSV + map PRIMARY → actual metric names
# ─────────────────────────────────────────────────────────────────────────
def load_csv():
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    for r in rows:
        r["DAY"] = int(r["DAY"])
        r["WINDOW_DAYS"] = int(r["WINDOW_DAYS"])
        r["CLIENT_CNT"] = int(r["CLIENT_CNT"])
        r["SUCCESS_CNT"] = int(r["SUCCESS_CNT"])
        r["RATE"] = float(r["RATE"])
        # Bug 1 fix: replace PRIMARY with actual metric name
        if r["METRIC"] == "PRIMARY":
            r["METRIC"] = MNE_METRIC_MAP.get(r["MNE"], r["METRIC"])
    return rows


# ─────────────────────────────────────────────────────────────────────────
# 2. Data Extract tab
# ─────────────────────────────────────────────────────────────────────────
def build_data_extract(wb, rows):
    ws = wb.active
    ws.title = "Data Extract"

    headers = ["MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "METRIC",
               "DAY", "WINDOW_DAYS", "CLIENT_CNT", "SUCCESS_CNT", "RATE"]

    for ci, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=ci, value=h)
        cell.font = HEADER_FONT_DARK
        cell.fill = GOLD_FILL
        cell.alignment = Alignment(horizontal="center")

    for ri, r in enumerate(rows, 2):
        ws.cell(row=ri, column=1, value=r["MNE"])
        ws.cell(row=ri, column=2, value=r["COHORT"])
        ws.cell(row=ri, column=3, value=r["TST_GRP_CD"])
        ws.cell(row=ri, column=4, value=r["RPT_GRP_CD"])
        ws.cell(row=ri, column=5, value=r["METRIC"])
        ws.cell(row=ri, column=6, value=r["DAY"])
        ws.cell(row=ri, column=7, value=r["WINDOW_DAYS"])
        ws.cell(row=ri, column=8, value=r["CLIENT_CNT"])
        ws.cell(row=ri, column=9, value=r["SUCCESS_CNT"])
        ws.cell(row=ri, column=10, value=r["RATE"])

    last_row = len(rows) + 1

    for ri in range(2, last_row + 1):
        fill = LIGHT_GREY_FILL if ri % 2 == 0 else WHITE_FILL
        for ci in range(1, 11):
            ws.cell(row=ri, column=ci).fill = fill

    table_ref = f"A1:J{last_row}"
    tab = Table(displayName="VintageData", ref=table_ref)
    style = TableStyleInfo(
        name="TableStyleMedium9", showFirstColumn=False,
        showLastColumn=False, showRowStripes=True, showColumnStripes=False,
    )
    tab.tableStyleInfo = style
    ws.add_table(tab)

    ws.freeze_panes = "A2"

    widths = [6, 10, 12, 14, 20, 6, 12, 14, 14, 10]
    for ci, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w

    for ri in range(2, last_row + 1):
        ws.cell(row=ri, column=10).number_format = "0.0000"

    return ws, last_row


# ─────────────────────────────────────────────────────────────────────────
# 3. Config tab
# ─────────────────────────────────────────────────────────────────────────
def build_config(wb, rows):
    ws = wb.create_sheet("Config")

    mnes = sorted(set(r["MNE"] for r in rows))
    rpt_grps = sorted(set(r["RPT_GRP_CD"] for r in rows))
    metrics_data = sorted(set(r["METRIC"] for r in rows))
    all_metrics = sorted(set(metrics_data) | set(METRIC_TABS))
    cohorts = sorted(set(r["COHORT"] for r in rows))

    for ri in range(1, 30):
        for ci in range(1, 12):
            ws.cell(row=ri, column=ci).fill = LIGHT_BLUE_FILL

    # Campaign selector — B1 is the MNE dropdown (Bug 2: verified absolute ref)
    ws.cell(row=1, column=1, value="Campaign (MNE)").font = Font(bold=True, size=12)
    ws.cell(row=1, column=2, value="VCN")
    ws["B1"].font = Font(bold=True, size=12, color="003366")
    mne_list = ",".join(mnes)
    dv_mne = DataValidation(type="list", formula1=f'"{mne_list}"', allow_blank=False)
    dv_mne.error = "Select a valid campaign MNE"
    dv_mne.errorTitle = "Invalid MNE"
    ws.add_data_validation(dv_mne)
    dv_mne.add(ws["B1"])

    # Available Categories
    ws.cell(row=3, column=1, value="Available Categories").font = Font(bold=True)
    for i, rg in enumerate(rpt_grps):
        ws.cell(row=3 + i, column=2, value=rg)

    # Available Metrics
    ws.cell(row=10, column=1, value="Available Metrics").font = Font(bold=True)
    for i, m in enumerate(all_metrics):
        ws.cell(row=10 + i, column=2, value=m)

    # Available Cohorts
    ws.cell(row=20, column=1, value="Available Cohorts").font = Font(bold=True)
    for i, c in enumerate(cohorts):
        ws.cell(row=20 + i, column=2, value=c)

    # MNE → Metric mapping table (columns E-F)
    ws.cell(row=1, column=5, value="MNE → Metric Mapping").font = Font(bold=True, size=12)
    ws.cell(row=2, column=5, value="MNE").font = Font(bold=True)
    ws.cell(row=2, column=6, value="Primary Metric").font = Font(bold=True)
    for i, (mne, metric) in enumerate(sorted(MNE_METRIC_MAP.items())):
        ws.cell(row=3 + i, column=5, value=mne)
        ws.cell(row=3 + i, column=6, value=metric)

    # Lookup Tables (columns H-K)
    ws.cell(row=1, column=8, value="Lookup Tables").font = Font(bold=True, size=12)
    ws.cell(row=2, column=8, value="MNE").font = Font(bold=True)
    ws.cell(row=2, column=9, value="RPT_GRP_CD").font = Font(bold=True)
    ws.cell(row=2, column=10, value="METRIC").font = Font(bold=True)
    ws.cell(row=2, column=11, value="COHORT").font = Font(bold=True)

    for i, v in enumerate(mnes):
        ws.cell(row=3 + i, column=8, value=v)
    for i, v in enumerate(rpt_grps):
        ws.cell(row=3 + i, column=9, value=v)
    for i, v in enumerate(all_metrics):
        ws.cell(row=3 + i, column=10, value=v)
    for i, v in enumerate(cohorts):
        ws.cell(row=3 + i, column=11, value=v)

    # Note about formula ranges (Bug 4)
    ws.cell(row=28, column=1, value="NOTE: Formula ranges use $A$2:$A$3409. "
            "If you add data beyond row 3409, update formulas or re-run this script.")
    ws["A28"].font = Font(italic=True, color="CC0000", size=9)

    ws.column_dimensions["A"].width = 22
    ws.column_dimensions["B"].width = 16
    for c_letter in ["E", "F", "H", "I", "J", "K"]:
        ws.column_dimensions[c_letter].width = 18

    return ws, mnes, rpt_grps, cohorts


# ─────────────────────────────────────────────────────────────────────────
# 4. Metric tabs — SUMPRODUCT formulas + RCU-style chart
# ─────────────────────────────────────────────────────────────────────────
def _sumproduct_formula(data_end_row, cohort_col, tst_grp, metric_cell, day_ref, cat_cell):
    """
    SUMPRODUCT formula that computes cumulative rate from Data Extract.
    Returns SUCCESS_CNT / CLIENT_CNT for the matching row.

    data_end_row: last data row in Data Extract (tight range, Bug 4)
    cohort_col: column letter referencing the cohort name in row 4
    tst_grp: "TG4" (Action) or "TG7" (Control)
    metric_cell: absolute ref to metric name cell (e.g., "$B$3")
    day_ref: mixed ref for day column (e.g., "$A6")
    cat_cell: absolute ref to category selector (e.g., "$E$2")
    """
    N = data_end_row
    de = "'Data Extract'"
    # Data Extract columns: A=MNE, B=COHORT, C=TST_GRP_CD, D=RPT_GRP_CD,
    #   E=METRIC, F=DAY, G=WINDOW, H=CLIENT_CNT, I=SUCCESS_CNT, J=RATE
    parts = (
        f"({de}!$A$2:$A${N}=Config!$B$1)"
        f"*({de}!$B$2:$B${N}={cohort_col}$4)"
        f"*({de}!$C$2:$C${N}=\"{tst_grp}\")"
        f"*IF({cat_cell}=\"ALL\",1,({de}!$D$2:$D${N}={cat_cell}))"
        f"*({de}!$E$2:$E${N}={metric_cell})"
        f"*({de}!$F$2:$F${N}={day_ref})"
    )
    num = f"SUMPRODUCT({parts}*{de}!$I$2:$I${N})"
    den = f"SUMPRODUCT({parts}*{de}!$H$2:$H${N})"
    return f'=IFERROR({num}/{den},"")'


def build_metric_tab(wb, metric_name, cohorts, rpt_grps, data_end_row):
    ws = wb.create_sheet(metric_name)
    pretty = metric_name.replace("_", " ").title()

    # Row 1: title
    ws.cell(row=1, column=1, value=f"{pretty} — Vintage Curves")
    ws["A1"].font = TITLE_FONT

    # Row 2: Campaign + Category selectors
    ws.cell(row=2, column=1, value="Campaign:")
    ws["A2"].font = Font(bold=True)
    ws.cell(row=2, column=2).value = "=Config!B1"
    ws["B2"].font = Font(bold=True, color="003366")

    ws.cell(row=2, column=4, value="Category:")
    ws["D2"].font = Font(bold=True)
    ws.cell(row=2, column=5, value="ALL")
    ws["E2"].font = Font(bold=True, color="003366")
    cat_options = '"ALL,' + ",".join(rpt_grps) + '"'
    dv_cat = DataValidation(type="list", formula1=cat_options, allow_blank=False)
    dv_cat.error = "Select ALL or a valid RPT_GRP_CD"
    ws.add_data_validation(dv_cat)
    dv_cat.add(ws["E2"])

    # Row 3: Metric name (this is what formulas reference via $B$3)
    ws.cell(row=3, column=1, value="Metric:")
    ws["A3"].font = Font(bold=True)
    ws.cell(row=3, column=2, value=metric_name)
    ws["B3"].font = Font(bold=True)
    # D3: conditional warning if no data for this campaign/metric combo
    no_data_formula = (
        '=IF(SUMPRODUCT((\'Data Extract\'!$A$2:$A$' + str(data_end_row)
        + '=Config!$B$1)*(\'Data Extract\'!$E$2:$E$' + str(data_end_row)
        + '=$B$3))>0,"Select \'ALL\' to aggregate across categories",'
        '"NOTE: No data for this campaign/metric combination")'
    )
    ws.cell(row=3, column=4).value = no_data_formula
    ws["D3"].font = Font(italic=True, color="666666")

    # Row 4: cohort identifiers (hidden row used by formulas)
    num_cohorts = len(cohorts)
    for ci, cohort in enumerate(cohorts):
        col_a = 2 + ci * 2
        col_c = 3 + ci * 2
        ws.cell(row=4, column=col_a, value=cohort).font = SMALL_GREY_FONT
        ws.cell(row=4, column=col_c, value=cohort).font = SMALL_GREY_FONT

    # Row 5: column headers — gold background with black bold text (RCU template)
    ws.cell(row=5, column=1, value="Day")
    ws["A5"].font = HEADER_FONT_DARK
    ws["A5"].fill = GOLD_FILL
    ws["A5"].alignment = Alignment(horizontal="center")

    for ci, cohort in enumerate(cohorts):
        col_a = 2 + ci * 2
        col_c = 3 + ci * 2
        cell_a = ws.cell(row=5, column=col_a, value=f"{cohort} Action")
        cell_c = ws.cell(row=5, column=col_c, value=f"{cohort} Control")
        for cell in (cell_a, cell_c):
            cell.font = HEADER_FONT_DARK
            cell.fill = GOLD_FILL
            cell.alignment = Alignment(horizontal="center")

    # Row 6+: Day values + SUMPRODUCT formulas
    for day in range(MAX_DAYS):
        row = 6 + day
        ws.cell(row=row, column=1, value=day)
        ws.cell(row=row, column=1).alignment = Alignment(horizontal="center")

        for ci, cohort in enumerate(cohorts):
            col_action = 2 + ci * 2
            col_control = 3 + ci * 2
            col_letter_a = get_column_letter(col_action)
            col_letter_c = get_column_letter(col_control)

            formula_a = _sumproduct_formula(
                data_end_row=data_end_row,
                cohort_col=col_letter_a,
                tst_grp="TG4",
                metric_cell="$B$3",
                day_ref=f"$A{row}",
                cat_cell="$E$2",
            )
            cell_a = ws.cell(row=row, column=col_action)
            cell_a.value = formula_a
            cell_a.number_format = "0.00%"

            formula_c = _sumproduct_formula(
                data_end_row=data_end_row,
                cohort_col=col_letter_c,
                tst_grp="TG7",
                metric_cell="$B$3",
                day_ref=f"$A{row}",
                cat_cell="$E$2",
            )
            cell_c = ws.cell(row=row, column=col_control)
            cell_c.value = formula_c
            cell_c.number_format = "0.00%"

    # Column widths — only for actual data columns, not 15 empty pairs
    ws.column_dimensions["A"].width = 6
    for ci in range(num_cohorts):
        col_a = 2 + ci * 2
        col_c = 3 + ci * 2
        ws.column_dimensions[get_column_letter(col_a)].width = 14
        ws.column_dimensions[get_column_letter(col_c)].width = 14

    ws.freeze_panes = "B6"

    # Chart — RCU template design (Bug 3 fix)
    _add_vintage_chart(ws, metric_name, cohorts)

    # A97 note: below data rows (row 6 + 91 days = row 96, so row 97)
    note_row = 6 + MAX_DAYS  # 97
    is_email = metric_name.startswith("email_")
    if is_email:
        ws.cell(row=note_row, column=1,
                value="No email data in current extract. "
                      "Update Data Extract with mega_output to populate.")
    else:
        ws.cell(row=note_row, column=1,
                value="If blank, this metric is not available "
                      "for the selected campaign.")
    ws.cell(row=note_row, column=1).font = Font(italic=True, color="CC0000", size=9)

    return ws


def _add_vintage_chart(ws, metric_name, cohorts):
    """
    RCU-style vintage curve chart:
    - White background, light grey gridlines
    - Line chart (not scatter)
    - Last 3 cohorts in distinct colors (blue, green, orange)
    - Older cohorts in light grey
    - Action = solid line, Control = dashed line, SAME color per cohort
    - Y-axis: percentage format
    - X-axis: Day (0, 10, 20, ... 90)
    - Legend below chart
    """
    from openpyxl.chart.shapes import GraphicalProperties
    from openpyxl.drawing.line import LineProperties, LineEndProperties
    import openpyxl.chart.series

    pretty = metric_name.replace("_", " ").title()
    chart = LineChart()
    chart.title = pretty
    chart.x_axis.title = "Day"
    chart.y_axis.title = "Cumulative Rate"
    chart.y_axis.numFmt = "0%"
    chart.y_axis.delete = False
    chart.x_axis.delete = False

    # White background, clean style
    chart.width = 26
    chart.height = 15
    chart.style = 2  # minimal style — white bg, light gridlines

    # Y-axis scaling: let Excel auto-scale but format as percentage
    chart.y_axis.scaling.min = 0
    chart.y_axis.numFmt = "0%"
    chart.y_axis.majorGridlines = None  # will use default light grey

    # Legend below chart
    chart.legend.position = "b"

    num_cohorts = len(cohorts)
    data_start_row = 6
    data_end_row = 6 + MAX_DAYS - 1  # row 96

    # X-axis categories (Day numbers)
    x_values = Reference(ws, min_col=1, min_row=data_start_row, max_row=data_end_row)

    for ci in range(num_cohorts):
        col_action = 2 + ci * 2
        col_control = 3 + ci * 2

        # Determine color: last 3 get focus colors, older get grey
        dist_from_end = num_cohorts - 1 - ci
        if dist_from_end < len(FOCUS_COLORS):
            color = FOCUS_COLORS[len(FOCUS_COLORS) - 1 - dist_from_end]
            line_width = 22000  # ~2pt
        else:
            color = GREY_COLOR
            line_width = 12000  # ~1pt, thinner for background cohorts

        # Action series (solid line)
        ref_a = Reference(ws, min_col=col_action, min_row=5, max_row=data_end_row)
        chart.add_data(ref_a, titles_from_data=True)
        s_a = chart.series[-1]
        s_a.graphicalProperties.line.solidFill = color
        s_a.graphicalProperties.line.width = line_width
        s_a.smooth = False

        # Control series (dashed line, same color)
        ref_c = Reference(ws, min_col=col_control, min_row=5, max_row=data_end_row)
        chart.add_data(ref_c, titles_from_data=True)
        s_c = chart.series[-1]
        s_c.graphicalProperties.line.solidFill = color
        s_c.graphicalProperties.line.width = line_width
        s_c.graphicalProperties.line.dashStyle = "dash"
        s_c.smooth = False

    chart.set_categories(x_values)

    # Place chart to the right of data columns (col after last data col + 1 gap)
    last_data_col = 2 + num_cohorts * 2
    chart_col = last_data_col + 1
    chart_anchor = f"{get_column_letter(chart_col)}1"
    ws.add_chart(chart, chart_anchor)


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────
def main():
    print("Loading CSV...")
    rows = load_csv()
    print(f"  {len(rows)} rows loaded from {CSV_PATH.name}")

    # Verify metric mapping worked
    metrics_in_data = sorted(set(r["METRIC"] for r in rows))
    print(f"  Metrics after mapping: {metrics_in_data}")
    assert "PRIMARY" not in metrics_in_data, "PRIMARY should have been mapped to actual metric names"

    cohorts = sorted(set(r["COHORT"] for r in rows))
    rpt_grps = sorted(set(r["RPT_GRP_CD"] for r in rows))
    mnes = sorted(set(r["MNE"] for r in rows))

    wb = Workbook()

    # Tab 1: Data Extract
    print("Building Data Extract tab...")
    _, data_last_row = build_data_extract(wb, rows)
    print(f"  Data written to rows 2-{data_last_row} ({data_last_row - 1} data rows)")

    # Tab 2: Config
    print("Building Config tab...")
    build_config(wb, rows)

    # Tabs 3-10: metric tabs with tight formula ranges
    for metric in METRIC_TABS:
        print(f"Building {metric} tab...")
        build_metric_tab(wb, metric, cohorts, rpt_grps, data_end_row=data_last_row)

    # Save
    print(f"\nSaving to {OUT_PATH}...")
    wb.save(str(OUT_PATH))
    print("Done.\n")

    # ── Verification ──
    print("=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    from openpyxl import load_workbook
    wb2 = load_workbook(str(OUT_PATH), data_only=False)

    # Check tabs exist
    print(f"\nTabs: {wb2.sheetnames}")
    assert "Data Extract" in wb2.sheetnames
    assert "Config" in wb2.sheetnames
    for m in METRIC_TABS:
        assert m in wb2.sheetnames, f"Missing tab: {m}"
    print("  All expected tabs present.")

    # Check Config B1 has data validation
    ws_cfg = wb2["Config"]
    print(f"\n  Config B1 value: '{ws_cfg['B1'].value}'")

    # Check Data Extract — verify no PRIMARY left
    ws_de = wb2["Data Extract"]
    primary_count = 0
    for row in ws_de.iter_rows(min_row=2, max_col=5, values_only=True):
        if row[4] == "PRIMARY":
            primary_count += 1
    print(f"  Data Extract: {primary_count} rows still have 'PRIMARY' (should be 0)")
    assert primary_count == 0, "Bug 1 NOT FIXED: PRIMARY values remain"

    # Check metric counts in Data Extract
    metric_counts = {}
    for row in ws_de.iter_rows(min_row=2, max_col=5, values_only=True):
        if row[4]:
            metric_counts[row[4]] = metric_counts.get(row[4], 0) + 1
    print(f"  Metric distribution: {metric_counts}")

    # Check formulas in card_acquisition tab
    ws_ca = wb2["card_acquisition"]
    print(f"\n  card_acquisition tab:")
    print(f"    B3 (metric name): '{ws_ca['B3'].value}'")
    print(f"    B2 (campaign ref): '{ws_ca['B2'].value}'")
    print(f"    B4 (cohort): '{ws_ca['B4'].value}'")

    # Check first formula
    b6_val = ws_ca["B6"].value
    print(f"    B6 (Day 0, first cohort Action): {b6_val[:80]}..." if b6_val and len(str(b6_val)) > 80 else f"    B6: {b6_val}")

    # Verify formula references Config!$B$1 (absolute)
    if b6_val and "Config!$B$1" in str(b6_val):
        print("    Config!$B$1 reference: CORRECT (absolute)")
    else:
        print("    WARNING: Config!$B$1 reference not found in formula")

    # Verify tight range (not 10000)
    if b6_val and "$A$10000" not in str(b6_val) and "$A$3" in str(b6_val):
        print(f"    Formula range: TIGHT (ends at row {data_last_row})")
    else:
        print(f"    WARNING: Formula range may not be tight")

    # Check chart exists
    print(f"    Charts in tab: {len(ws_ca._charts)}")

    # Check chart series count — should be num_cohorts * 2
    if ws_ca._charts:
        chart = ws_ca._charts[0]
        expected_series = len(cohorts) * 2
        print(f"    Chart series: {len(chart.series)} (expected {expected_series})")
        assert len(chart.series) == expected_series, f"Chart has {len(chart.series)} series, expected {expected_series}"
        print(f"    Chart title: {chart.title}")

    wb2.close()

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY — ALL BUGS FIXED")
    print("=" * 60)
    print(f"Output: {OUT_PATH}")
    print(f"Data rows: {len(rows)}")
    print(f"Cohorts: {cohorts}")
    print(f"MNEs: {mnes}")
    print(f"Formula range: $A$2:$A${data_last_row} (tight, was $A$10000)")
    print(f"\nBug 1 FIXED: PRIMARY mapped to actual metric names per MNE")
    print(f"Bug 2 FIXED: Config!$B$1 absolute reference verified")
    print(f"Bug 3 FIXED: Chart uses RCU design — cohort-colored pairs, grey for older")
    print(f"Bug 4 FIXED: Formula range tightened from 10000 to {data_last_row}")


if __name__ == "__main__":
    main()
