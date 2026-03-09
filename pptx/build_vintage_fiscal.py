"""
Build 6 Excel workbooks (one per VVD campaign) from vintage curve data.
Maps monthly cohorts to RBC fiscal quarters before aggregating.
Each workbook has: Data Extract tab, Calculations (overall) tab, per-channel tabs, and charts.
"""

import csv
import re
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


def month_to_fiscal_quarter(year, month):
    """Map a calendar month to RBC fiscal quarter.
    RBC FY starts Nov 1. FY2025 = Nov 2024 - Oct 2025.
    Nov, Dec → Q1 of FY(year+1); Jan → Q1 of FY(year);
    Feb-Apr → Q2; May-Jul → Q3; Aug-Oct → Q4.
    """
    if month in (11, 12):
        return f"FY{year + 1}Q1"
    elif month == 1:
        return f"FY{year}Q1"
    elif month in (2, 3, 4):
        return f"FY{year}Q2"
    elif month in (5, 6, 7):
        return f"FY{year}Q3"
    else:
        return f"FY{year}Q4"


def calendar_quarter_to_fiscal(cohort_str):
    """Approximate mapping from calendar quarter (e.g. '2024Q3') to fiscal quarter.
    Uses middle month of calendar quarter to determine fiscal quarter.
    Cal Q1 (mid=Feb) → FQ2, Cal Q2 (mid=May) → FQ3,
    Cal Q3 (mid=Aug) → FQ4, Cal Q4 (mid=Nov) → FQ1 of next FY.
    """
    m = re.match(r"(\d{4})Q(\d)", cohort_str)
    if not m:
        return None
    year, q = int(m.group(1)), int(m.group(2))
    mid_months = {1: 2, 2: 5, 3: 8, 4: 11}
    return month_to_fiscal_quarter(year, mid_months[q])


def detect_cohort_format(cohorts):
    """Detect whether cohorts are YYYY-MM or YYYYQX."""
    sample = next(iter(cohorts))
    if re.match(r"\d{4}-\d{2}$", sample):
        return "monthly"
    if re.match(r"\d{4}Q\d$", sample):
        return "quarterly"
    raise ValueError(f"Unrecognized cohort format: {sample}")


def map_cohort_to_fiscal(cohort_str, fmt):
    """Map a cohort string to its fiscal quarter label."""
    if fmt == "monthly":
        parts = cohort_str.split("-")
        return month_to_fiscal_quarter(int(parts[0]), int(parts[1]))
    else:
        return calendar_quarter_to_fiscal(cohort_str)


def load_and_aggregate():
    """Load CSV, filter March 2026, map to fiscal quarters, aggregate."""
    raw_rows = []
    with open(INPUT_CSV, "r") as f:
        for row in csv.DictReader(f):
            if row["COHORT"] == "2026-03":
                continue
            row["DAY"] = int(row["DAY"])
            row["CLIENT_CNT"] = int(row["CLIENT_CNT"])
            row["SUCCESS_CNT"] = int(row["SUCCESS_CNT"])
            row["RATE"] = float(row["RATE"])
            row["WINDOW_DAYS"] = int(row["WINDOW_DAYS"])
            raw_rows.append(row)

    if not raw_rows:
        raise ValueError("No rows after filtering")

    all_cohorts = set(r["COHORT"] for r in raw_rows)
    fmt = detect_cohort_format(all_cohorts)
    if fmt == "quarterly":
        print("WARNING: Input has calendar quarters — fiscal mapping is approximate (uses middle month).")

    # Map each row's cohort to fiscal quarter
    for row in raw_rows:
        row["FISCAL_QUARTER"] = map_cohort_to_fiscal(row["COHORT"], fmt)

    # Aggregate: group by (MNE, FISCAL_QUARTER, TST_GRP_CD, RPT_GRP_CD, METRIC, DAY, WINDOW_DAYS)
    agg = defaultdict(lambda: {"CLIENT_CNT": 0, "SUCCESS_CNT": 0})
    for row in raw_rows:
        key = (row["MNE"], row["FISCAL_QUARTER"], row["TST_GRP_CD"],
               row["RPT_GRP_CD"], row["METRIC"], row["DAY"], row["WINDOW_DAYS"])
        agg[key]["CLIENT_CNT"] += row["CLIENT_CNT"]
        agg[key]["SUCCESS_CNT"] += row["SUCCESS_CNT"]

    # Build aggregated rows
    campaigns = defaultdict(list)
    for (mne, fq, tst, rpt, metric, day, window), vals in agg.items():
        client_cnt = vals["CLIENT_CNT"]
        success_cnt = vals["SUCCESS_CNT"]
        rate = success_cnt / client_cnt if client_cnt > 0 else 0.0
        campaigns[mne].append({
            "MNE": mne,
            "COHORT": fq,
            "TST_GRP_CD": tst,
            "RPT_GRP_CD": rpt,
            "METRIC": metric,
            "DAY": day,
            "WINDOW_DAYS": window,
            "CLIENT_CNT": client_cnt,
            "SUCCESS_CNT": success_cnt,
            "RATE": rate,
        })

    return campaigns, fmt


def build_data_extract(wb, rows):
    """Tab 1: Data Extract — aggregated data with table formatting."""
    ws = wb.active
    ws.title = "Data Extract"

    headers = ["Cohort", "Metric", "Test Group", "RPT_GRP_CD", "Period", "Metric Value", "Base Value"]
    for c, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=c, value=h)
        cell.font = HEADER_FONT_DARK
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center")
        cell.border = THIN_BORDER

    sorted_rows = sorted(rows, key=lambda r: (r["COHORT"], TST_GRP_MAP[r["TST_GRP_CD"]], r["RPT_GRP_CD"], r["DAY"]))

    for i, r in enumerate(sorted_rows, 2):
        ws.cell(row=i, column=1, value=r["COHORT"]).border = THIN_BORDER
        ws.cell(row=i, column=2, value=r["METRIC"]).border = THIN_BORDER
        ws.cell(row=i, column=3, value=TST_GRP_MAP[r["TST_GRP_CD"]]).border = THIN_BORDER
        ws.cell(row=i, column=4, value=r["RPT_GRP_CD"]).border = THIN_BORDER
        ws.cell(row=i, column=5, value=r["DAY"]).border = THIN_BORDER
        ws.cell(row=i, column=6, value=r["SUCCESS_CNT"]).border = THIN_BORDER
        ws.cell(row=i, column=7, value=r["CLIENT_CNT"]).border = THIN_BORDER

        if i % 2 == 0:
            for c in range(1, 8):
                ws.cell(row=i, column=c).fill = ALT_ROW_FILL

    last_row = len(sorted_rows) + 1
    table_ref = f"A1:G{last_row}"
    tab = Table(displayName="DataExtract", ref=table_ref)
    tab.tableStyleInfo = TableStyleInfo(
        name="TableStyleMedium9", showFirstColumn=False,
        showLastColumn=False, showRowStripes=True, showColumnStripes=False,
    )
    ws.add_table(tab)

    ws.column_dimensions["A"].width = 14
    ws.column_dimensions["B"].width = 14
    ws.column_dimensions["C"].width = 12
    ws.column_dimensions["D"].width = 14
    ws.column_dimensions["E"].width = 10
    ws.column_dimensions["F"].width = 14
    ws.column_dimensions["G"].width = 14

    ws.freeze_panes = "A2"

    return len(sorted_rows)


def build_calc_tab(wb, mne, rows, cohorts, tab_name, rpt_grp=None):
    """Build a Calculations tab (overall or per-channel) with SUMPRODUCT formulas and chart."""
    ws = wb.create_sheet(title=tab_name)
    metric_name = METRIC_NAMES[mne]
    max_day = max(r["DAY"] for r in rows)
    view_label = "Overall" if rpt_grp is None else rpt_grp

    ws.cell(row=1, column=1, value=f"{mne} - {metric_name}").font = TITLE_FONT
    if rpt_grp:
        ws.cell(row=1, column=4, value=f"Channel: {rpt_grp}").font = SUBTITLE_FONT

    ws.cell(row=2, column=1, value="Total").font = SUBTITLE_FONT
    ws.cell(row=2, column=2, value=metric_name).font = SUBTITLE_FONT
    ws.cell(row=2, column=11, value="Total Clients <- Enter the Chart Title").font = Font(italic=True, color="888888")

    ws.cell(row=3, column=1, value="Test").font = SUBTITLE_FONT
    ws.cell(row=3, column=2, value="Test").font = SUBTITLE_FONT

    ws.cell(row=4, column=1, value="Control").font = SUBTITLE_FONT
    ws.cell(row=4, column=2, value="Control").font = SUBTITLE_FONT

    ws.cell(row=5, column=1, value="Period")
    ws.cell(row=5, column=1).font = HEADER_FONT_DARK
    ws.cell(row=5, column=1).fill = HEADER_FILL
    ws.cell(row=5, column=1).border = THIN_BORDER
    ws.cell(row=5, column=1).alignment = Alignment(horizontal="center")

    col = 2
    col_map = []
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

    for day in range(max_day + 1):
        row_num = day + 6
        ws.cell(row=row_num, column=1, value=day).border = THIN_BORDER

        for col_idx, cohort, group_label, group_row in col_map:
            rng = "'Data Extract'"
            cond_cohort = f'({rng}!$A$2:$A$5000="{cohort}")'
            cond_group = f"({rng}!$C$2:$C$5000={group_row})"
            cond_period = f"({rng}!$E$2:$E$5000=$A{row_num})"
            cond_rpt = f'*({rng}!$D$2:$D$5000="{rpt_grp}")' if rpt_grp else ""

            success_sum = f"SUMPRODUCT({cond_cohort}*{cond_group}*{cond_period}{cond_rpt}*({rng}!$F$2:$F$5000))"
            client_sum = f"SUMPRODUCT({cond_cohort}*{cond_group}*{cond_period}{cond_rpt}*({rng}!$G$2:$G$5000))"

            formula = f'=IFERROR({success_sum}/{client_sum},"")'

            cell = ws.cell(row=row_num, column=col_idx)
            cell.value = formula
            cell.number_format = PCT_FMT
            cell.border = THIN_BORDER

    ws.column_dimensions["A"].width = 10
    for col_idx, _, _, _ in col_map:
        ws.column_dimensions[get_column_letter(col_idx)].width = 16

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

    last_row = max_day + 6
    x_data = Reference(ws, min_col=1, min_row=6, max_row=last_row)
    chart.set_categories(x_data)

    COHORT_COLORS = ["1F77B4", "FF7F0E", "2CA02C", "D62728", "9467BD", "8C564B"]

    for i, (col_idx, cohort, group_label, _) in enumerate(col_map):
        values = Reference(ws, min_col=col_idx, min_row=5, max_row=last_row)
        chart.add_data(values, titles_from_data=True)
        series = chart.series[-1]

        cohort_idx = cohorts.index(cohort)
        color = COHORT_COLORS[cohort_idx % len(COHORT_COLORS)]
        series.graphicalProperties.line.solidFill = color
        series.graphicalProperties.line.width = 20000

        if group_label == "Control":
            series.graphicalProperties.line.dashStyle = "dash"

    chart_col = get_column_letter(last_data_col + 2)
    ws.add_chart(chart, f"{chart_col}1")

    return max_day + 1


def build_workbook(mne, rows):
    """Build a complete workbook for one campaign."""
    wb = Workbook()

    cohorts = sorted(set(r["COHORT"] for r in rows))
    rpt_grps = sorted(set(r["RPT_GRP_CD"] for r in rows))
    max_day = max(r["DAY"] for r in rows)

    extract_rows = build_data_extract(wb, rows)
    calc_rows = build_calc_tab(wb, mne, rows, cohorts, "Calculations", rpt_grp=None)

    for rpt_grp in rpt_grps:
        channel_rows = [r for r in rows if r["RPT_GRP_CD"] == rpt_grp]
        build_calc_tab(wb, mne, channel_rows, cohorts, f"Calc {rpt_grp}", rpt_grp=rpt_grp)

    output_path = OUTPUT_DIR / f"{mne}_vintage_fiscal.xlsx"
    wb.save(str(output_path))

    return {
        "mne": mne,
        "cohorts": cohorts,
        "rpt_grps": rpt_grps,
        "max_day": max_day,
        "extract_rows": extract_rows,
        "tabs": 2 + len(rpt_grps),
        "path": str(output_path),
    }


def main():
    campaigns, fmt = load_and_aggregate()
    total_rows = sum(len(v) for v in campaigns.values())
    print(f"Loaded and aggregated to {total_rows} fiscal rows from {INPUT_CSV.name}")
    print(f"Cohort format detected: {fmt}")
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
