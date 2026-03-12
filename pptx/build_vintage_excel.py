"""
Build 6 Excel workbooks (one per VVD campaign) from vintage curve data.
Each workbook has: Data Extract tab, Calculations (overall) tab, per-channel tabs, and charts.
"""

import csv
from pathlib import Path
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.chart import LineChart, Reference
from openpyxl.chart.series import DataPoint
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

# ── Paths ──
INPUT_CSV = Path(r"C:\Users\andre\New_projects\VVD\pptx\data\vintage_curves.csv")
OUTPUT_DIR = Path(r"C:\Users\andre\New_projects\VVD\pptx\data")

# ── Campaign metadata ──
METRIC_NAMES = {
    "VCN": "card_acquisition",
    "VDA": "card_acquisition",
    "VDT": "card_activation",
    "VUI": "card_usage",
    "VUT": "wallet_provisioning",
    "VAW": "wallet_provisioning",
}

TST_GRP_MAP = {"TG4": "Test", "TG7": "Control"}

# ── Styles ──
HEADER_FILL = PatternFill(start_color="FFD700", end_color="FFD700", fill_type="solid")
HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
HEADER_FONT_DARK = Font(bold=True, color="000000", size=11)
TITLE_FONT = Font(bold=True, size=14)
SUBTITLE_FONT = Font(bold=True, size=11)
ALT_ROW_FILL = PatternFill(start_color="DCE6F1", end_color="DCE6F1", fill_type="solid")
THIN_BORDER = Border(
    left=Side(style="thin"), right=Side(style="thin"),
    top=Side(style="thin"), bottom=Side(style="thin"),
)
PCT_FMT = "0.00%"


def load_data():
    """Load CSV and group by campaign."""
    campaigns = defaultdict(list)
    with open(INPUT_CSV, "r") as f:
        for row in csv.DictReader(f):
            row["DAY"] = int(row["DAY"])
            row["CLIENT_CNT"] = int(row["CLIENT_CNT"])
            row["SUCCESS_CNT"] = int(row["SUCCESS_CNT"])
            row["RATE"] = float(row["RATE"])
            row["WINDOW_DAYS"] = int(row["WINDOW_DAYS"])
            campaigns[row["MNE"]].append(row)
    return campaigns


def build_data_extract(wb, rows):
    """Tab 1: Data Extract — raw data with table formatting."""
    ws = wb.active
    ws.title = "Data Extract"

    headers = ["Cohort", "Metric", "Test Group", "RPT_GRP_CD", "Period", "Metric Value", "Base Value"]
    for c, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=c, value=h)
        cell.font = HEADER_FONT_DARK
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center")
        cell.border = THIN_BORDER

    # Sort: Cohort, Test Group, RPT_GRP_CD, Period
    sorted_rows = sorted(rows, key=lambda r: (r["COHORT"], TST_GRP_MAP[r["TST_GRP_CD"]], r["RPT_GRP_CD"], r["DAY"]))

    for i, r in enumerate(sorted_rows, 2):
        ws.cell(row=i, column=1, value=r["COHORT"]).border = THIN_BORDER
        ws.cell(row=i, column=2, value=r["METRIC"]).border = THIN_BORDER
        ws.cell(row=i, column=3, value=TST_GRP_MAP[r["TST_GRP_CD"]]).border = THIN_BORDER
        ws.cell(row=i, column=4, value=r["RPT_GRP_CD"]).border = THIN_BORDER
        ws.cell(row=i, column=5, value=r["DAY"]).border = THIN_BORDER
        ws.cell(row=i, column=6, value=r["SUCCESS_CNT"]).border = THIN_BORDER
        ws.cell(row=i, column=7, value=r["CLIENT_CNT"]).border = THIN_BORDER

        # Alternating row colors
        if i % 2 == 0:
            for c in range(1, 8):
                ws.cell(row=i, column=c).fill = ALT_ROW_FILL

    # Add Excel table
    last_row = len(sorted_rows) + 1
    table_ref = f"A1:G{last_row}"
    tab = Table(displayName="DataExtract", ref=table_ref)
    tab.tableStyleInfo = TableStyleInfo(
        name="TableStyleMedium9", showFirstColumn=False,
        showLastColumn=False, showRowStripes=True, showColumnStripes=False,
    )
    ws.add_table(tab)

    # Column widths
    ws.column_dimensions["A"].width = 12
    ws.column_dimensions["B"].width = 14
    ws.column_dimensions["C"].width = 12
    ws.column_dimensions["D"].width = 14
    ws.column_dimensions["E"].width = 10
    ws.column_dimensions["F"].width = 14
    ws.column_dimensions["G"].width = 14

    # Freeze panes at row 2
    ws.freeze_panes = "A2"

    return len(sorted_rows)


def build_sumproduct_formula(cohort_ref, group_ref, period_ref, value_col, rpt_grp_ref=None):
    """
    Build a SUMPRODUCT formula that pulls from Data Extract.
    cohort_ref: cell reference for cohort (e.g., "B$5" — the header cell)
    group_ref: cell reference for group (e.g., "$B$3" or "$B$4")
    period_ref: cell reference for period (e.g., "$A6")
    value_col: "F" for SUCCESS_CNT, "G" for CLIENT_CNT
    rpt_grp_ref: optional cell ref for RPT_GRP_CD filter
    """
    rng = "'Data Extract'"
    cond_cohort = f"({rng}!$A$2:$A$5000={cohort_ref})"
    cond_group = f"({rng}!$C$2:$C$5000={group_ref})"
    cond_period = f"({rng}!$E$2:$E$5000={period_ref})"
    cond_rpt = f"*({rng}!$D$2:$D$5000={rpt_grp_ref})" if rpt_grp_ref else ""
    values = f"({rng}!${value_col}$2:${value_col}$5000)"

    return f"SUMPRODUCT({cond_cohort}*{cond_group}*{cond_period}{cond_rpt}*{values})"


def build_calc_tab(wb, mne, rows, cohorts, tab_name, rpt_grp=None):
    """
    Build a Calculations tab (overall or per-channel).
    Uses SUMPRODUCT formulas referencing Data Extract.
    """
    ws = wb.create_sheet(title=tab_name)
    metric_name = METRIC_NAMES[mne]
    max_day = max(r["DAY"] for r in rows)
    view_label = "Overall" if rpt_grp is None else rpt_grp

    # Row 1: Title
    ws.cell(row=1, column=1, value=f"{mne} - {metric_name}").font = TITLE_FONT
    if rpt_grp:
        ws.cell(row=1, column=4, value=f"Channel: {rpt_grp}").font = SUBTITLE_FONT

    # Row 2: "Total" label + metric name + chart title hint
    ws.cell(row=2, column=1, value="Total").font = SUBTITLE_FONT
    ws.cell(row=2, column=2, value=metric_name).font = SUBTITLE_FONT
    ws.cell(row=2, column=11, value="Total Clients <- Enter the Chart Title").font = Font(italic=True, color="888888")

    # Row 3: Test group ref
    ws.cell(row=3, column=1, value="Test").font = SUBTITLE_FONT
    ws.cell(row=3, column=2, value="Test").font = SUBTITLE_FONT

    # Row 4: Control group ref
    ws.cell(row=4, column=1, value="Control").font = SUBTITLE_FONT
    ws.cell(row=4, column=2, value="Control").font = SUBTITLE_FONT

    # Row 5: Column headers
    ws.cell(row=5, column=1, value="Period")
    ws.cell(row=5, column=1).font = HEADER_FONT_DARK
    ws.cell(row=5, column=1).fill = HEADER_FILL
    ws.cell(row=5, column=1).border = THIN_BORDER
    ws.cell(row=5, column=1).alignment = Alignment(horizontal="center")

    # Build column pairs: Test then Control for each cohort
    col = 2
    col_map = []  # (col_idx, cohort, group_label, group_row_ref)
    for cohort in cohorts:
        for group_label, group_row in [("Test", "$B$3"), ("Control", "$B$4")]:
            header = f"{cohort} {group_label}"
            cell = ws.cell(row=5, column=col, value=header)
            cell.font = HEADER_FONT_DARK
            cell.fill = HEADER_FILL
            cell.border = THIN_BORDER
            cell.alignment = Alignment(horizontal="center")
            col_map.append((col, cohort, group_label, group_row))
            col += 1

    last_data_col = col - 1

    # Rows 6+: Period data with formulas
    for day in range(max_day + 1):
        row_num = day + 6
        ws.cell(row=row_num, column=1, value=day).border = THIN_BORDER

        for col_idx, cohort, group_label, group_row in col_map:
            col_letter = get_column_letter(col_idx)
            # The cohort value is in the header cell for this column at row 5
            cohort_ref = f"{col_letter}$5"
            period_ref = f"$A{row_num}"

            # We can't directly reference the header cell for cohort since it says
            # "2024Q3 Test" not just "2024Q3". Instead, write the cohort in a hidden
            # reference row or embed it directly.
            # Simpler: just put the cohort name in a helper row (row 5 has combined name).
            # Let's use a different approach: write the pure cohort in a helper row above.
            # Actually, cleanest: use SUMPRODUCT with literal strings via helper cells.

            # For formula cleanliness, build the SUMPRODUCT with literal cohort string
            rng = "'Data Extract'"
            cond_cohort = f'({rng}!$A$2:$A$5000="{cohort}")'
            cond_group = f"({rng}!$C$2:$C$5000={group_row})"
            cond_period = f"({rng}!$E$2:$E$5000={period_ref})"
            cond_rpt = f'*({rng}!$D$2:$D$5000="{rpt_grp}")' if rpt_grp else ""

            success_sum = f"SUMPRODUCT({cond_cohort}*{cond_group}*{cond_period}{cond_rpt}*({rng}!$F$2:$F$5000))"
            client_sum = f"SUMPRODUCT({cond_cohort}*{cond_group}*{cond_period}{cond_rpt}*({rng}!$G$2:$G$5000))"

            formula = f'=IFERROR({success_sum}/{client_sum},"")'

            cell = ws.cell(row=row_num, column=col_idx)
            cell.value = formula
            cell.number_format = PCT_FMT
            cell.border = THIN_BORDER

    # Column widths
    ws.column_dimensions["A"].width = 10
    for col_idx, _, _, _ in col_map:
        ws.column_dimensions[get_column_letter(col_idx)].width = 16

    # Freeze panes at B6
    ws.freeze_panes = "B6"

    # ── Chart ──
    chart = LineChart()
    chart.title = f"{mne} - Vintage Curve ({view_label})"
    chart.x_axis.title = "Period (Days)"
    chart.y_axis.title = "Cumulative Rate"
    chart.y_axis.numFmt = "0.00%"
    chart.style = 10
    chart.width = 30
    chart.height = 15

    # X axis: Period values from column A
    last_row = max_day + 6
    x_data = Reference(ws, min_col=1, min_row=6, max_row=last_row)
    chart.set_categories(x_data)

    # Series colors — cohort-based
    COHORT_COLORS = ["1F77B4", "FF7F0E", "2CA02C", "D62728", "9467BD", "8C564B"]

    for i, (col_idx, cohort, group_label, _) in enumerate(col_map):
        values = Reference(ws, min_col=col_idx, min_row=5, max_row=last_row)
        chart.add_data(values, titles_from_data=True)
        series = chart.series[-1]

        # Color by cohort
        cohort_idx = cohorts.index(cohort)
        color = COHORT_COLORS[cohort_idx % len(COHORT_COLORS)]
        series.graphicalProperties.line.solidFill = color
        series.graphicalProperties.line.width = 20000  # EMUs (~2pt)

        if group_label == "Control":
            series.graphicalProperties.line.dashStyle = "dash"

    # Place chart to the right of data
    chart_col = get_column_letter(last_data_col + 2)
    ws.add_chart(chart, f"{chart_col}1")

    return max_day + 1  # number of data rows


def build_workbook(mne, rows):
    """Build a complete workbook for one campaign."""
    wb = Workbook()

    # Gather metadata
    cohorts = sorted(set(r["COHORT"] for r in rows))
    rpt_grps = sorted(set(r["RPT_GRP_CD"] for r in rows))
    max_day = max(r["DAY"] for r in rows)

    # Tab 1: Data Extract
    extract_rows = build_data_extract(wb, rows)

    # Tab 2: Calculations (overall)
    calc_rows = build_calc_tab(wb, mne, rows, cohorts, "Calculations", rpt_grp=None)

    # Per-channel tabs
    for rpt_grp in rpt_grps:
        channel_rows = [r for r in rows if r["RPT_GRP_CD"] == rpt_grp]
        build_calc_tab(wb, mne, channel_rows, cohorts, f"Calc {rpt_grp}", rpt_grp=rpt_grp)

    # Save
    output_path = OUTPUT_DIR / f"{mne}_vintage.xlsx"
    wb.save(str(output_path))

    return {
        "mne": mne,
        "cohorts": cohorts,
        "rpt_grps": rpt_grps,
        "max_day": max_day,
        "extract_rows": extract_rows,
        "tabs": 2 + len(rpt_grps),  # Data Extract + Calculations + per-channel
        "path": str(output_path),
    }


def main():
    campaigns = load_data()
    print(f"Loaded {sum(len(v) for v in campaigns.values())} rows from {INPUT_CSV.name}")
    print(f"Campaigns: {sorted(campaigns.keys())}\n")

    results = []
    for mne in sorted(campaigns.keys()):
        info = build_workbook(mne, campaigns[mne])
        results.append(info)
        print(f"{mne}: {info['extract_rows']} rows, {info['tabs']} tabs, "
              f"cohorts={info['cohorts']}, RPT_GRP_CDs={info['rpt_grps']}, "
              f"max_day={info['max_day']}")

    print(f"\nGenerated {len(results)} Excel files in {OUTPUT_DIR}")
    for r in results:
        print(f"  {r['path']}")


if __name__ == "__main__":
    main()
