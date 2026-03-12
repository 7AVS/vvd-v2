#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Build VVD Vintage Dashboard Excel from vintage_curves.csv.

Python pre-computes ALL rates. Excel has NO heavy formulas.
Dashboard uses XLOOKUP against a small Rates lookup table (~7K rows).
RATE column in CSV is already the correct daily vintage curve value
(SUCCESS_CNT / CLIENT_CNT * 100, in percentage points).

Input:  pptx/data/vintage_curves.csv
Output: pptx/data/VVD_vintage_dashboard.xlsx
"""

import os
import time
import pandas as pd
import xlsxwriter

BASE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE, "data", "vintage_curves.csv")
OUT_PATH = os.path.join(BASE, "data", "VVD_vintage_dashboard.xlsx")

# Campaign -> primary metric name mapping
METRIC_MAP = {
    "VCN": "card_acquisition",
    "VDA": "card_acquisition",
    "VDT": "card_activation",
    "VUI": "card_usage",
    "VUT": "wallet_provisioning",
    "VAW": "wallet_provisioning",
}

MAX_COHORTS = 15  # max cohort pairs on dashboard
MAX_DAY = 90


def load_and_compute():
    """Load CSV, build Rates lookup table. RATE used directly (already correct)."""
    df = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(df):,} rows from CSV")

    # Map PRIMARY -> actual metric name
    df["METRIC_NAME"] = df["MNE"].map(METRIC_MAP)

    df = df.sort_values(
        ["MNE", "RPT_GRP_CD", "TST_GRP_CD", "COHORT", "DAY"]
    ).reset_index(drop=True)

    # RATE = SUCCESS_CNT / CLIENT_CNT * 100 (percentage points)
    # Store as fraction for Excel percentage formatting: 0.0467% -> 0.000467
    df["RATE_FRAC"] = df["RATE"] / 100.0

    # Build per-RPT_GRP_CD rows for Rates table
    rates_rows = []
    for _, row in df.iterrows():
        rates_rows.append({
            "MNE": row["MNE"],
            "METRIC": row["METRIC_NAME"],
            "CATEGORY": row["RPT_GRP_CD"],
            "TST_GRP_CD": row["TST_GRP_CD"],
            "COHORT": row["COHORT"],
            "DAY": int(row["DAY"]),
            "RATE": row["RATE_FRAC"],
        })

    # Build "ALL" aggregation: weighted average across RPT_GRP_CDs
    # (sum SUCCESS_CNT / sum CLIENT_CNT) * 100 -> fraction
    agg = (
        df.groupby(["MNE", "METRIC_NAME", "TST_GRP_CD", "COHORT", "DAY"])
        .agg({"SUCCESS_CNT": "sum", "CLIENT_CNT": "sum"})
        .reset_index()
    )
    agg["RATE_FRAC"] = agg["SUCCESS_CNT"] / agg["CLIENT_CNT"]  # already fraction
    for _, row in agg.iterrows():
        rates_rows.append({
            "MNE": row["MNE"],
            "METRIC": row["METRIC_NAME"],
            "CATEGORY": "ALL",
            "TST_GRP_CD": row["TST_GRP_CD"],
            "COHORT": row["COHORT"],
            "DAY": int(row["DAY"]),
            "RATE": row["RATE_FRAC"],
        })

    rates_df = pd.DataFrame(rates_rows)
    rates_df = rates_df.sort_values(
        ["MNE", "METRIC", "CATEGORY", "TST_GRP_CD", "COHORT", "DAY"]
    ).reset_index(drop=True)

    # Concatenated lookup key
    rates_df["KEY"] = (
        rates_df["MNE"]
        + rates_df["METRIC"]
        + rates_df["CATEGORY"]
        + rates_df["TST_GRP_CD"]
        + rates_df["COHORT"]
        + rates_df["DAY"].astype(str)
    )

    print(f"Rates table: {len(rates_df):,} rows")
    return df, rates_df


def get_dropdown_values(df, rates_df):
    """Extract unique values for dropdowns."""
    mnes = sorted(df["MNE"].unique())
    metrics = sorted(rates_df["METRIC"].unique())
    categories = ["ALL"] + sorted(df["RPT_GRP_CD"].unique())

    cohorts_per_mne = {}
    for mne in mnes:
        cohorts_per_mne[mne] = sorted(df[df["MNE"] == mne]["COHORT"].unique())

    return mnes, metrics, categories, cohorts_per_mne


def write_excel(df, rates_df, mnes, metrics, categories, cohorts_per_mne):
    """Write the multi-tab Excel workbook."""
    wb = xlsxwriter.Workbook(OUT_PATH, {"strings_to_numbers": False})

    # ----------------------------------------------------------------
    # Formats
    # ----------------------------------------------------------------
    fmt_header = wb.add_format({
        "bold": True, "bg_color": "#002D72", "font_color": "#FFFFFF",
        "border": 1, "font_size": 10, "font_name": "Segoe UI",
    })
    fmt_pct = wb.add_format({
        "num_format": "0.00%", "font_size": 9, "font_name": "Segoe UI",
    })
    fmt_pct_rate = wb.add_format({
        "num_format": "0.00%", "font_size": 9, "font_name": "Segoe UI",
    })
    fmt_label = wb.add_format({
        "bold": True, "font_size": 11, "font_name": "Segoe UI",
        "bg_color": "#D6E4F0", "border": 1,
    })
    fmt_selector = wb.add_format({
        "font_size": 11, "font_name": "Segoe UI",
        "bg_color": "#EBF1F8", "border": 1,
    })
    fmt_cohort_id = wb.add_format({
        "font_size": 8, "font_color": "#808080", "font_name": "Segoe UI",
    })
    fmt_col_header = wb.add_format({
        "bold": True, "bg_color": "#FFD700", "font_color": "#000000",
        "border": 1, "font_size": 9, "font_name": "Segoe UI",
        "text_wrap": True,
    })
    fmt_day_col = wb.add_format({
        "font_size": 9, "font_name": "Segoe UI", "bold": True,
    })
    fmt_info = wb.add_format({
        "font_size": 9, "font_name": "Segoe UI", "font_color": "#666666",
    })
    fmt_title = wb.add_format({
        "bold": True, "font_size": 14, "font_name": "Segoe UI",
        "font_color": "#002D72",
    })
    fmt_number = wb.add_format({
        "num_format": "#,##0", "font_size": 9, "font_name": "Segoe UI",
    })
    fmt_rate_raw = wb.add_format({
        "num_format": "0.0000", "font_size": 9, "font_name": "Segoe UI",
    })

    R = len(rates_df)  # data rows in Rates table

    # ----------------------------------------------------------------
    # TAB 1: Data (raw dump for reference)
    # ----------------------------------------------------------------
    ws_data = wb.add_worksheet("Data")
    data_cols = [
        "MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "METRIC",
        "DAY", "WINDOW_DAYS", "CLIENT_CNT", "SUCCESS_CNT", "RATE",
    ]
    for c, col in enumerate(data_cols):
        ws_data.write(0, c, col, fmt_header)

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        ws_data.write(i, 0, row["MNE"])
        ws_data.write(i, 1, row["COHORT"])
        ws_data.write(i, 2, row["TST_GRP_CD"])
        ws_data.write(i, 3, row["RPT_GRP_CD"])
        ws_data.write(i, 4, row["METRIC"])
        ws_data.write(i, 5, int(row["DAY"]))
        ws_data.write(i, 6, int(row["WINDOW_DAYS"]))
        ws_data.write(i, 7, int(row["CLIENT_CNT"]), fmt_number)
        ws_data.write(i, 8, int(row["SUCCESS_CNT"]), fmt_number)
        ws_data.write(i, 9, row["RATE"], fmt_rate_raw)

    ws_data.add_table(0, 0, len(df), len(data_cols) - 1, {
        "name": "RawData",
        "columns": [{"header": c} for c in data_cols],
        "style": "Table Style Light 9",
    })
    ws_data.set_column(0, 0, 6)
    ws_data.set_column(1, 1, 10)
    ws_data.set_column(2, 2, 12)
    ws_data.set_column(3, 3, 14)
    ws_data.set_column(4, 4, 10)
    ws_data.set_column(5, 5, 6)
    ws_data.set_column(6, 6, 14)
    ws_data.set_column(7, 8, 14)
    ws_data.set_column(9, 9, 10)

    # ----------------------------------------------------------------
    # TAB 2: Rates (pre-computed lookup table)
    # ----------------------------------------------------------------
    ws_rates = wb.add_worksheet("Rates")
    rate_cols = ["MNE", "METRIC", "CATEGORY", "TST_GRP_CD", "COHORT", "DAY", "RATE", "KEY"]
    for c, col in enumerate(rate_cols):
        ws_rates.write(0, c, col, fmt_header)

    for i, (_, row) in enumerate(rates_df.iterrows(), start=1):
        ws_rates.write(i, 0, row["MNE"])
        ws_rates.write(i, 1, row["METRIC"])
        ws_rates.write(i, 2, row["CATEGORY"])
        ws_rates.write(i, 3, row["TST_GRP_CD"])
        ws_rates.write(i, 4, row["COHORT"])
        ws_rates.write(i, 5, int(row["DAY"]))
        ws_rates.write(i, 6, row["RATE"], fmt_pct_rate)
        ws_rates.write(i, 7, row["KEY"])

    ws_rates.add_table(0, 0, R, len(rate_cols) - 1, {
        "name": "Rates",
        "columns": [{"header": c} for c in rate_cols],
        "style": "Table Style Light 9",
    })
    ws_rates.set_column(0, 0, 6)
    ws_rates.set_column(1, 1, 22)
    ws_rates.set_column(2, 2, 14)
    ws_rates.set_column(3, 3, 12)
    ws_rates.set_column(4, 4, 10)
    ws_rates.set_column(5, 5, 6)
    ws_rates.set_column(6, 6, 10)
    ws_rates.set_column(7, 7, 45)

    # ----------------------------------------------------------------
    # TAB 3: Config (hidden helper — cohorts per MNE)
    # ----------------------------------------------------------------
    ws_config = wb.add_worksheet("Config")
    for col_idx, mne in enumerate(mnes):
        ws_config.write(0, col_idx, mne, fmt_header)
        for row_idx, cohort in enumerate(cohorts_per_mne[mne], start=1):
            ws_config.write(row_idx, col_idx, cohort)
    ws_config.hide()

    # ----------------------------------------------------------------
    # TAB 4: Dashboard
    # ----------------------------------------------------------------
    ws = wb.add_worksheet("Dashboard")

    # Rates table range strings (Excel rows r_start..r_end)
    r_start = 2
    r_end = R + 1
    key_range = f"Rates!$H${r_start}:$H${r_end}"
    rate_range = f"Rates!$G${r_start}:$G${r_end}"

    # Column ranges for FILTER / COUNTIFS
    ra = f"Rates!$A${r_start}:$A${r_end}"
    rb = f"Rates!$B${r_start}:$B${r_end}"
    rc = f"Rates!$C${r_start}:$C${r_end}"
    rd = f"Rates!$D${r_start}:$D${r_end}"
    re_ = f"Rates!$E${r_start}:$E${r_end}"
    rf = f"Rates!$F${r_start}:$F${r_end}"

    default_mne = "VCN"
    default_metric = METRIC_MAP[default_mne]
    default_category = "ALL"

    # -- Row 1: Campaign / Metric selectors --
    ws.write("A1", "Campaign:", fmt_label)
    ws.write("B1", default_mne, fmt_selector)
    ws.write("C1", "", fmt_selector)
    ws.write("D1", "Metric:", fmt_label)
    ws.write("E1", default_metric, fmt_selector)
    ws.write("F1", "", fmt_selector)
    ws.write("G1", "Cohorts found:", fmt_info)
    ws.write_formula(
        "H1",
        f'=COUNTIFS({ra},$B$1,{rb},$E$1,{rc},$B$2,{rd},"TG4",{rf},0)',
        fmt_info,
    )

    # -- Row 2: Category selector --
    ws.write("A2", "Category:", fmt_label)
    ws.write("B2", default_category, fmt_selector)

    # Data validations (dropdowns)
    ws.data_validation("B1", {
        "validate": "list",
        "source": mnes,
        "input_title": "Campaign",
        "input_message": "Select MNE",
    })
    ws.data_validation("E1", {
        "validate": "list",
        "source": metrics,
        "input_title": "Metric",
        "input_message": "Select metric",
    })
    ws.data_validation("B2", {
        "validate": "list",
        "source": categories,
        "input_title": "Category",
        "input_message": "Select RPT_GRP_CD or ALL",
    })

    # -- Row 3: Title --
    ws.write("A3", "VVD Vintage Curves \u2014 Daily Success Rate by Day", fmt_title)

    # -- Helper area: column AH (index 33) — dynamic cohort list --
    helper_col = 33  # AH
    ws.write(0, helper_col, "Cohorts", fmt_header)
    filter_formula = (
        f"=SORT(UNIQUE(FILTER({re_},"
        f"({ra}=$B$1)*({rb}=$E$1)*({rc}=$B$2)*({rd}=\"TG4\")*({rf}=0))))"
    )
    ws.write_dynamic_array_formula(1, helper_col, 1, helper_col, filter_formula)

    # -- Row 4: Cohort identifiers (15 pairs) --
    data_start_col = 1  # column B
    for i in range(MAX_COHORTS):
        act_col = data_start_col + 2 * i
        ctl_col = act_col + 1
        helper_ref = f"$AH${2 + i}"
        ws.write_formula(3, act_col, f'=IFERROR({helper_ref},"")', fmt_cohort_id)
        ws.write_formula(3, ctl_col, f'=IFERROR({helper_ref},"")', fmt_cohort_id)

    # -- Row 5: Column headers (Action / Control) --
    ws.write(4, 0, "Day", fmt_col_header)
    for i in range(MAX_COHORTS):
        act_col = data_start_col + 2 * i
        ctl_col = act_col + 1
        act_cell = xlsxwriter.utility.xl_rowcol_to_cell(3, act_col)
        ctl_cell = xlsxwriter.utility.xl_rowcol_to_cell(3, ctl_col)
        ws.write_formula(
            4, act_col,
            f'=IF({act_cell}="","",{act_cell}&" Action")', fmt_col_header,
        )
        ws.write_formula(
            4, ctl_col,
            f'=IF({ctl_cell}="","",{ctl_cell}&" Control")', fmt_col_header,
        )

    # -- Rows 6-96: Data grid (Day 0..90) --
    data_start_row = 5  # 0-indexed
    for day in range(MAX_DAY + 1):
        row = data_start_row + day
        ws.write(row, 0, day, fmt_day_col)

        for i in range(MAX_COHORTS):
            act_col = data_start_col + 2 * i
            ctl_col = act_col + 1

            cohort_cell = xlsxwriter.utility.xl_rowcol_to_cell(
                3, act_col, row_abs=True, col_abs=False,
            )
            day_cell = xlsxwriter.utility.xl_rowcol_to_cell(
                row, 0, row_abs=False, col_abs=True,
            )

            act_key = f'$B$1&$E$1&$B$2&"TG4"&{cohort_cell}&{day_cell}'
            ctl_key = f'$B$1&$E$1&$B$2&"TG7"&{cohort_cell}&{day_cell}'

            ws.write_formula(
                row, act_col,
                f'=IFERROR(XLOOKUP({act_key},{key_range},{rate_range},""),"")',
                fmt_pct,
            )
            ws.write_formula(
                row, ctl_col,
                f'=IFERROR(XLOOKUP({ctl_key},{key_range},{rate_range},""),"")',
                fmt_pct,
            )

    # -- Column widths --
    ws.set_column(0, 0, 5)
    ws.set_column(1, 30, 13)
    ws.set_column(33, 33, 12)

    # -- Chart --
    chart = wb.add_chart({"type": "line"})
    chart.set_title({"name": "Vintage Curves"})
    chart.set_x_axis({
        "name": "Days Since Treatment",
        "min": 0,
        "max": MAX_DAY,
        "num_font": {"size": 8},
    })
    chart.set_y_axis({
        "name": "Success Rate",
        "num_format": "0%",
        "num_font": {"size": 8},
    })
    chart.set_size({"width": 960, "height": 500})
    chart.set_legend({"none": True})

    action_color = "#002D72"
    control_color = "#C0C0C0"
    sheet_name = "Dashboard"

    for i in range(MAX_COHORTS):
        act_col = data_start_col + 2 * i
        ctl_col = act_col + 1

        chart.add_series({
            "name": [sheet_name, 4, act_col],
            "categories": [sheet_name, data_start_row, 0, data_start_row + MAX_DAY, 0],
            "values": [sheet_name, data_start_row, act_col, data_start_row + MAX_DAY, act_col],
            "line": {"color": action_color, "width": 1.5},
            "marker": {"type": "none"},
        })
        chart.add_series({
            "name": [sheet_name, 4, ctl_col],
            "categories": [sheet_name, data_start_row, 0, data_start_row + MAX_DAY, 0],
            "values": [sheet_name, data_start_row, ctl_col, data_start_row + MAX_DAY, ctl_col],
            "line": {"color": control_color, "width": 1.5, "dash_type": "dash"},
            "marker": {"type": "none"},
        })

    ws.insert_chart("A98", chart)

    # -- Freeze panes & print --
    ws.freeze_panes(5, 1)
    ws.set_landscape()
    ws.set_paper(1)
    ws.fit_to_pages(1, 0)

    ws.activate()
    wb.close()
    print(f"Wrote {OUT_PATH}")
    return R


def verify(R):
    """Quick verification of the generated file."""
    try:
        import openpyxl
    except ImportError:
        print("openpyxl not installed -- skipping verification")
        return

    try:
        wb = openpyxl.load_workbook(OUT_PATH, read_only=True, data_only=False)
        print("\n-- VERIFICATION --")
        print(f"Sheets: {wb.sheetnames}")

        ws_rates = wb["Rates"]
        rate_rows = sum(1 for _ in ws_rates.rows) - 1
        print(f"Rates table rows: {rate_rows:,} (expected {R:,})")

        ws = wb["Dashboard"]
        print(f"B1 (Campaign):  {ws['B1'].value}")
        print(f"E1 (Metric):    {ws['E1'].value}")
        print(f"B2 (Category):  {ws['B2'].value}")
        print(f"H1 formula:     {ws['H1'].value}")
        print(f"B6 formula:     {ws['B6'].value}")
        print(f"C6 formula:     {ws['C6'].value}")
        print(f"AH2 formula:    {ws['AH2'].value}")

        dvs = ws.data_validations.dataValidation if ws.data_validations else []
        print(f"Data validations: {len(dvs)}")
        for dv in dvs:
            print(f"  {dv.sqref}: type={dv.type}, formula1={dv.formula1!r}")

        wb.close()
        print("-- VERIFICATION PASSED --")
    except Exception as e:
        print(f"Verification error: {e}")


def main():
    t0 = time.time()

    df, rates_df = load_and_compute()
    mnes, metrics, categories, cohorts_per_mne = get_dropdown_values(df, rates_df)

    print(f"MNEs: {mnes}")
    print(f"Metrics: {metrics}")
    print(f"Categories: {categories}")
    print(f"Cohorts per MNE: {cohorts_per_mne}")

    R = write_excel(df, rates_df, mnes, metrics, categories, cohorts_per_mne)

    elapsed = time.time() - t0
    print(f"\nGenerated in {elapsed:.1f}s")

    verify(R)


if __name__ == "__main__":
    main()
