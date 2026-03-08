#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build VVD_vintage_single.xlsx — single-tab vintage curve workbook with selector-driven dashboard.

REWRITE: Fixes circular references, removes FILTER/UNIQUE/SORT dynamic array formulas,
uses static cohort values in Row 4, and only creates chart series for actual cohorts.
"""

import os
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.chart import LineChart, Reference
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.formatting.rule import CellIsRule

# ── paths ──
BASE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE, "data", "vintage_curves.csv")
OUT_PATH = os.path.join(BASE, "data", "VVD_vintage_single.xlsx")

# ── metric mapping ──
PRIMARY_MAP = {
    "VCN": "card_acquisition",
    "VDA": "card_acquisition",
    "VDT": "card_activation",
    "VUI": "card_usage",
    "VUT": "wallet_provisioning",
    "VAW": "wallet_provisioning",
}

# ── styles ──
GREY = "C0C0C0"
GOLD_FILL = PatternFill(start_color="FFD700", end_color="FFD700", fill_type="solid")
SELECTOR_FILL = PatternFill(start_color="DCE6F1", end_color="DCE6F1", fill_type="solid")
HELPER_FILL = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
ALT_ROW_FILL = PatternFill(start_color="F5F5F5", end_color="F5F5F5", fill_type="solid")
THIN_BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)

MAX_COHORT_PAIRS = 15  # max cohort slots (only actual cohorts get formulas)
MAX_DAYS = 91  # days 0-90


def load_data():
    df = pd.read_csv(CSV_PATH)
    df["METRIC"] = df.apply(
        lambda r: PRIMARY_MAP.get(r["MNE"], r["METRIC"]) if r["METRIC"] == "PRIMARY" else r["METRIC"],
        axis=1,
    )
    return df


def build_data_tab(wb, df):
    ws = wb.active
    ws.title = "Data"

    headers = ["MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "METRIC",
               "DAY", "WINDOW_DAYS", "CLIENT_CNT", "SUCCESS_CNT", "RATE"]
    hdr_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    for c, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=c, value=h)
        cell.font = Font(bold=True, size=10, color="FFFFFF")
        cell.fill = hdr_fill
        cell.alignment = Alignment(horizontal="center")

    for r_idx, row in enumerate(df.itertuples(index=False), start=2):
        for c_idx, val in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=val)
            cell.border = THIN_BORDER
            cell.alignment = Alignment(horizontal="center")
            if r_idx % 2 == 0:
                cell.fill = ALT_ROW_FILL

    last_row = len(df) + 1
    tbl = Table(displayName="VintageData", ref=f"A1:J{last_row}")
    tbl.tableStyleInfo = TableStyleInfo(
        name="TableStyleLight1", showFirstColumn=False,
        showLastColumn=False, showRowStripes=True, showColumnStripes=False,
    )
    ws.add_table(tbl)

    for col_idx in range(1, 11):
        ws.column_dimensions[get_column_letter(col_idx)].width = 16
    ws.freeze_panes = "A2"

    return last_row


def build_dashboard(wb, df, data_last_row):
    ws = wb.create_sheet("Dashboard")

    all_mnes = sorted(df["MNE"].unique())
    all_metrics = sorted(df["METRIC"].unique())
    all_rpts = sorted(df["RPT_GRP_CD"].unique())
    all_cohorts = sorted(df["COHORT"].unique())
    n_actual = len(all_cohorts)

    N = data_last_row  # last row on Data tab (header at row 1, data rows 2..N)

    # Data tab column range strings (absolute references)
    rA = f"Data!$A$2:$A${N}"
    rB = f"Data!$B$2:$B${N}"
    rC = f"Data!$C$2:$C${N}"
    rD = f"Data!$D$2:$D${N}"
    rE = f"Data!$E$2:$E${N}"
    rF = f"Data!$F$2:$F${N}"
    rH = f"Data!$H$2:$H${N}"
    rI = f"Data!$I$2:$I${N}"

    # ── SELECTOR AREA (rows 1-3) ──
    for r in range(1, 4):
        for c in range(1, 12):
            ws.cell(row=r, column=c).fill = SELECTOR_FILL

    # Row 1: Campaign + Metric selectors
    ws.cell(row=1, column=1, value="Campaign:").font = Font(bold=True, size=11)
    ws.cell(row=1, column=2, value="VCN").font = Font(bold=True, size=12, color="0070C0")
    ws.cell(row=1, column=4, value="Metric:").font = Font(bold=True, size=11)
    ws.cell(row=1, column=5, value="card_acquisition").font = Font(bold=True, size=12, color="0070C0")

    # Row 2: Category selector + Status
    ws.cell(row=2, column=1, value="Category:").font = Font(bold=True, size=11)
    ws.cell(row=2, column=2, value="ALL").font = Font(bold=True, size=12, color="0070C0")
    ws.cell(row=2, column=4, value="Status:").font = Font(bold=True, size=11)

    # Status formula — references ONLY Data tab + B1, E1 on Dashboard (both are dropdown values, no circularity)
    status_formula = (
        f'=IF(SUMPRODUCT(({rA}=$B$1)*({rE}=$E$1))>0,'
        f'"OK","No data for this combination")'
    )
    ws.cell(row=2, column=5).value = status_formula
    ws.cell(row=2, column=5).font = Font(bold=True, size=11)

    # Conditional formatting for status
    ws.conditional_formatting.add(
        "E2",
        CellIsRule(operator="equal", formula=['"OK"'],
                   font=Font(bold=True, color="006100"),
                   fill=PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")),
    )
    ws.conditional_formatting.add(
        "E2",
        CellIsRule(operator="equal", formula=['"No data for this combination"'],
                   font=Font(bold=True, color="9C0006"),
                   fill=PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")),
    )

    # Row 3: Note
    ws.cell(row=3, column=1, value="Note:").font = Font(bold=True, size=9, color="808080")
    ws.cell(row=3, column=2, value="Select campaign + metric above. Chart updates automatically.").font = Font(size=9, color="808080")

    # ── DATA VALIDATION (dropdowns) ──
    dv_mne = DataValidation(type="list", formula1=f'"{",".join(all_mnes)}"', allow_blank=False)
    dv_mne.error = "Select a valid campaign"
    dv_mne.errorTitle = "Invalid Campaign"
    ws.add_data_validation(dv_mne)
    dv_mne.add(ws["B1"])

    dv_metric = DataValidation(type="list", formula1=f'"{",".join(all_metrics)}"', allow_blank=False)
    dv_metric.error = "Select a valid metric"
    dv_metric.errorTitle = "Invalid Metric"
    ws.add_data_validation(dv_metric)
    dv_metric.add(ws["E1"])

    rpt_list = "ALL," + ",".join(all_rpts)
    dv_rpt = DataValidation(type="list", formula1=f'"{rpt_list}"', allow_blank=False)
    dv_rpt.error = "Select a valid category"
    dv_rpt.errorTitle = "Invalid Category"
    ws.add_data_validation(dv_rpt)
    dv_rpt.add(ws["B2"])

    # ── HELPER AREA — static reference lists ──
    # Place AFTER all possible cohort pair columns to avoid overlap.
    # 15 pairs × 2 cols = 30 cols starting at B (col 2) → cols 2..31.
    # Helper starts at col 33 (AG).
    HELPER_COL = 33

    ws.cell(row=1, column=HELPER_COL, value="MNE_List").font = Font(bold=True, size=9, color="808080")
    ws.cell(row=1, column=HELPER_COL).fill = HELPER_FILL
    for i, mne in enumerate(all_mnes, 2):
        c = ws.cell(row=i, column=HELPER_COL, value=mne)
        c.fill = HELPER_FILL
        c.font = Font(size=9, color="808080")

    ws.cell(row=1, column=HELPER_COL + 1, value="Metrics").font = Font(bold=True, size=9, color="808080")
    ws.cell(row=1, column=HELPER_COL + 1).fill = HELPER_FILL
    for i, m in enumerate(all_metrics, 2):
        c = ws.cell(row=i, column=HELPER_COL + 1, value=m)
        c.fill = HELPER_FILL
        c.font = Font(size=9, color="808080")

    ws.cell(row=1, column=HELPER_COL + 2, value="RPT_GRP_CDs").font = Font(bold=True, size=9, color="808080")
    ws.cell(row=1, column=HELPER_COL + 2).fill = HELPER_FILL
    rpt_with_all = ["ALL"] + list(all_rpts)
    for i, r in enumerate(rpt_with_all, 2):
        c = ws.cell(row=i, column=HELPER_COL + 2, value=r)
        c.fill = HELPER_FILL
        c.font = Font(size=9, color="808080")

    # ── ROW 4: COHORT IDENTIFIERS (STATIC VALUES — only actual cohorts) ──
    data_start_col = 2  # B
    for pair_idx in range(n_actual):
        col_action = data_start_col + pair_idx * 2
        col_control = col_action + 1
        cohort_val = all_cohorts[pair_idx]

        for col in [col_action, col_control]:
            cell = ws.cell(row=4, column=col, value=cohort_val)
            cell.font = Font(size=8, color="808080")
            cell.alignment = Alignment(horizontal="center")

    # ── ROW 5: ACTION/CONTROL HEADERS (only actual cohorts) ──
    ws.cell(row=4, column=1, value="").fill = GOLD_FILL
    day_hdr = ws.cell(row=5, column=1, value="Day")
    day_hdr.fill = GOLD_FILL
    day_hdr.font = Font(bold=True, size=10)
    day_hdr.alignment = Alignment(horizontal="center")

    for pair_idx in range(n_actual):
        col_action = data_start_col + pair_idx * 2
        col_control = col_action + 1
        col_act_letter = get_column_letter(col_action)
        col_ctrl_letter = get_column_letter(col_control)

        act_hdr = ws.cell(row=5, column=col_action)
        act_hdr.value = f'=IF({col_act_letter}4="","",{col_act_letter}4&" Action")'
        act_hdr.fill = GOLD_FILL
        act_hdr.font = Font(bold=True, size=10)
        act_hdr.alignment = Alignment(horizontal="center")

        ctrl_hdr = ws.cell(row=5, column=col_control)
        ctrl_hdr.value = f'=IF({col_ctrl_letter}4="","",{col_ctrl_letter}4&" Control")'
        ctrl_hdr.fill = GOLD_FILL
        ctrl_hdr.font = Font(bold=True, size=10)
        ctrl_hdr.alignment = Alignment(horizontal="center")

    # ── ROWS 6-96: DAY 0-90, SUMPRODUCT FORMULAS ──
    # Formula computes cumulative vintage rate:
    #   numerator: SUM(SUCCESS_CNT) for matching rows where DAY <= current day
    #   denominator: CLIENT_CNT at DAY=0 for matching rows
    # References: Data tab columns (A-I) + Dashboard cells B1, E1, B2, Row 4, Col A
    # ALL of these are either on Data tab or static/dropdown values — NO circular refs possible.

    for day in range(MAX_DAYS):
        row = 6 + day
        ws.cell(row=row, column=1, value=day).alignment = Alignment(horizontal="center")

        # Only write formulas for actual cohort columns (not empty slots)
        for pair_idx in range(n_actual):
            col_action = data_start_col + pair_idx * 2
            col_control = col_action + 1
            col_act_letter = get_column_letter(col_action)
            col_ctrl_letter = get_column_letter(col_control)

            # Common filter components
            f_mne = f"({rA}=$B$1)"
            f_cohort_act = f"({rB}={col_act_letter}$4)"
            f_cohort_ctrl = f"({rB}={col_ctrl_letter}$4)"
            f_rpt = f'IF($B$2="ALL",1,({rD}=$B$2))'
            f_metric = f"({rE}=$E$1)"
            f_day_le = f"({rF}<=$A{row})"
            f_day_0 = f"({rF}=0)"

            # Action (TG4)
            f_tst_act = f'({rC}="TG4")'
            numer = f"SUMPRODUCT({f_mne}*{f_cohort_act}*{f_tst_act}*{f_rpt}*{f_metric}*{f_day_le}*{rI})"
            denom = f"SUMPRODUCT({f_mne}*{f_cohort_act}*{f_tst_act}*{f_rpt}*{f_metric}*{f_day_0}*{rH})"
            cell_act = ws.cell(row=row, column=col_action)
            cell_act.value = f'=IFERROR({numer}/{denom},"")'
            cell_act.number_format = "0.00%"
            cell_act.alignment = Alignment(horizontal="center")

            # Control (TG7)
            f_tst_ctrl = f'({rC}="TG7")'
            numer = f"SUMPRODUCT({f_mne}*{f_cohort_ctrl}*{f_tst_ctrl}*{f_rpt}*{f_metric}*{f_day_le}*{rI})"
            denom = f"SUMPRODUCT({f_mne}*{f_cohort_ctrl}*{f_tst_ctrl}*{f_rpt}*{f_metric}*{f_day_0}*{rH})"
            cell_ctrl = ws.cell(row=row, column=col_control)
            cell_ctrl.value = f'=IFERROR({numer}/{denom},"")'
            cell_ctrl.number_format = "0.00%"
            cell_ctrl.alignment = Alignment(horizontal="center")

    # ── COLUMN WIDTHS ──
    ws.column_dimensions["A"].width = 6
    for pair_idx in range(n_actual):
        col_a = data_start_col + pair_idx * 2
        col_c = col_a + 1
        ws.column_dimensions[get_column_letter(col_a)].width = 16
        ws.column_dimensions[get_column_letter(col_c)].width = 16
    for c in range(HELPER_COL, HELPER_COL + 3):
        ws.column_dimensions[get_column_letter(c)].width = 20

    ws.freeze_panes = "B6"

    # ── CHART — only series for actual cohorts ──
    chart = LineChart()
    chart.title = "Vintage Curves"
    chart.style = 10
    chart.y_axis.title = "Cumulative Rate"
    chart.x_axis.title = "Day"
    chart.y_axis.numFmt = "0%"
    chart.width = 28
    chart.height = 18
    chart.legend.position = "b"

    cats = Reference(ws, min_col=1, min_row=6, max_row=6 + MAX_DAYS - 1)

    # ALL lines in light grey as requested
    for pair_idx in range(n_actual):
        col_action = data_start_col + pair_idx * 2
        col_control = col_action + 1

        # Action series (solid)
        data_act = Reference(ws, min_col=col_action, min_row=5, max_row=5 + MAX_DAYS)
        chart.add_data(data_act, titles_from_data=True)
        s_act = chart.series[-1]
        s_act.graphicalProperties.line.solidFill = GREY
        s_act.graphicalProperties.line.width = 20000
        s_act.smooth = False

        # Control series (dashed)
        data_ctrl = Reference(ws, min_col=col_control, min_row=5, max_row=5 + MAX_DAYS)
        chart.add_data(data_ctrl, titles_from_data=True)
        s_ctrl = chart.series[-1]
        s_ctrl.graphicalProperties.line.solidFill = GREY
        s_ctrl.graphicalProperties.line.width = 20000
        s_ctrl.graphicalProperties.line.dashStyle = "dash"
        s_ctrl.smooth = False

    chart.set_categories(cats)

    # Anchor chart at K5, spanning ~20 cols x 20 rows
    ws.add_chart(chart, "K5")

    return n_actual


def verify_workbook(path):
    """Open the generated file and print diagnostics."""
    from openpyxl import load_workbook
    wb = load_workbook(path)
    print("\n=== VERIFICATION ===")
    print(f"Sheet names: {wb.sheetnames}")

    ds = wb["Dashboard"]

    print(f"\nB1 (MNE selector): {ds['B1'].value}")
    print(f"E1 (Metric selector): {ds['E1'].value}")
    print(f"B2 (Category selector): {ds['B2'].value}")
    print(f"E2 (Status formula): {ds['E2'].value}")
    print(f"B4 (Cohort 1): {ds['B4'].value}")
    print(f"D4 (Cohort 2): {ds['D4'].value}")
    print(f"F4 (Cohort 3): {ds['F4'].value}")
    print(f"H4 (Cohort 4): {ds['H4'].value}")

    print(f"\nB5 (Header formula): {ds['B5'].value}")
    print(f"C5 (Header formula): {ds['C5'].value}")

    b6 = ds['B6'].value
    c6 = ds['C6'].value
    print(f"\nB6 (Action Day 0 formula):\n  {b6}")
    print(f"\nC6 (Control Day 0 formula):\n  {c6}")

    # Check for circular reference indicators
    print("\n--- Circular Reference Check ---")
    issues = []
    if b6 and isinstance(b6, str):
        # B6 should NOT reference itself or any Dashboard formula cell
        if "B6" in b6 or "B$6" in b6 or "$B$6" in b6:
            issues.append("B6 references itself!")
        # Should only reference: Data! cells, $B$1, $E$1, $B$2, row 4 (B$4), $A6
        dashboard_refs = []
        for token in ["$B$1", "$E$1", "$B$2", "B$4", "$A6"]:
            if token in b6:
                dashboard_refs.append(token)
        print(f"  B6 Dashboard refs: {dashboard_refs} (all should be static/dropdown)")

    if c6 and isinstance(c6, str):
        if "C6" in c6 or "C$6" in c6 or "$C$6" in c6:
            issues.append("C6 references itself!")
        dashboard_refs = []
        for token in ["$B$1", "$E$1", "$B$2", "C$4", "$A6"]:
            if token in c6:
                dashboard_refs.append(token)
        print(f"  C6 Dashboard refs: {dashboard_refs} (all should be static/dropdown)")

    if issues:
        print(f"  ISSUES FOUND: {issues}")
    else:
        print("  No circular references detected.")

    # Check data validations
    print("\n--- Data Validations ---")
    for dv in ds.data_validations.dataValidation:
        print(f"  Cells: {dv.sqref}, Type: {dv.type}, Formula: {dv.formula1}")

    # Count formula cells
    formula_count = 0
    for row in ds.iter_rows():
        for cell in row:
            if cell.value and isinstance(cell.value, str) and cell.value.startswith("="):
                formula_count += 1
    print(f"\nTotal formula cells on Dashboard: {formula_count}")

    # Check chart
    print(f"\nCharts on Dashboard: {len(ds._charts)}")
    if ds._charts:
        ch = ds._charts[0]
        print(f"  Series count: {len(ch.series)}")
        if ch.series:
            print(f"  First series title ref: {ch.series[0].title}")
            print(f"  First series values: {ch.series[0].val}")

    # Check for any FILTER/UNIQUE/SORT formulas (should be NONE)
    dynamic_formulas = 0
    for row in ds.iter_rows():
        for cell in row:
            if cell.value and isinstance(cell.value, str):
                val_upper = cell.value.upper()
                if any(fn in val_upper for fn in ["FILTER(", "UNIQUE(", "SORT("]):
                    dynamic_formulas += 1
                    print(f"  WARNING: Dynamic array formula at {cell.coordinate}: {cell.value[:80]}")
    if dynamic_formulas == 0:
        print("\nNo FILTER/UNIQUE/SORT formulas found (good).")
    else:
        print(f"\nWARNING: {dynamic_formulas} dynamic array formulas found!")

    # Check helper area (col 33+) — should be all static
    print("\n--- Helper Area (col 33+) ---")
    for col in range(33, 36):
        for row in range(1, 10):
            cell = ds.cell(row=row, column=col)
            if cell.value:
                is_formula = isinstance(cell.value, str) and cell.value.startswith("=")
                tag = "FORMULA (BAD!)" if is_formula else "static"
                print(f"  {cell.coordinate}: {tag} = {cell.value}")

    print("\n=== VERIFICATION COMPLETE ===")


def main():
    print("Loading data...")
    df = load_data()
    print(f"  {len(df)} rows, {df['MNE'].nunique()} MNEs, {df['COHORT'].nunique()} cohorts")
    print(f"  Cohorts: {sorted(df['COHORT'].unique())}")
    print(f"  Metrics after mapping: {sorted(df['METRIC'].unique())}")
    print(f"  RPT_GRP_CDs: {sorted(df['RPT_GRP_CD'].unique())}")

    wb = Workbook()

    print("\nBuilding Data tab...")
    data_last_row = build_data_tab(wb, df)
    print(f"  {data_last_row - 1} data rows written (rows 2..{data_last_row})")

    print("\nBuilding Dashboard tab...")
    n_cohorts = build_dashboard(wb, df, data_last_row)
    print(f"  {n_cohorts} cohort pairs, {MAX_DAYS} days, {n_cohorts * 2} chart series")

    print(f"\nSaving to {OUT_PATH}...")
    wb.save(OUT_PATH)
    print("Done.")

    verify_workbook(OUT_PATH)


if __name__ == "__main__":
    main()
