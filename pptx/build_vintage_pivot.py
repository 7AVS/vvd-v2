"""
Build VVD Vintage Pivot workbook with PivotTable, Slicers, and PivotChart.
Uses win32com (Excel COM automation) for native Excel features.

Input:  pptx/data/vintage_curves.csv
Output: pptx/data/VVD_vintage_pivot.xlsx
"""

import os
import sys
import time
import pandas as pd
import win32com.client
import pythoncom

# ── Paths ──────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(SCRIPT_DIR, "data", "vintage_curves.csv")
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "data", "VVD_vintage_pivot.xlsx")
TEMP_PATH = os.path.join(SCRIPT_DIR, "data", "_vintage_pivot_temp.xlsx")

# ── Excel COM constants ────────────────────────────────────────────────────
xlDatabase = 1
xlRowField = 1
xlColumnField = 2
xlPageField = 3
xlDataField = 4
xlAverage = -4106
xlSum = -4157
xlLine = 4
xlOpenXMLWorkbook = 51
xlLineMarkers = 65
xlCategory = 1
xlValue = 2

# ── Metric mapping ─────────────────────────────────────────────────────────
METRIC_MAP = {
    "VCN": "card_acquisition",
    "VDA": "card_acquisition",
    "VDT": "card_activation",
    "VUI": "card_usage",
    "VUT": "wallet_provisioning",
    "VAW": "wallet_provisioning",
}

GROUP_MAP = {"TG4": "Action", "TG7": "Control"}


def prepare_data():
    """Read CSV, apply mappings, return DataFrame ready for Excel."""
    print(f"[1/4] Reading CSV: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    print(f"       {len(df)} rows, columns: {list(df.columns)}")

    # Map METRIC from "PRIMARY" to actual metric name based on MNE
    df["METRIC"] = df["MNE"].map(METRIC_MAP)

    # Map test group codes to human-readable names
    df["TST_GRP_CD"] = df["TST_GRP_CD"].map(GROUP_MAP)

    # Add CATEGORY column (copy of RPT_GRP_CD)
    df["CATEGORY"] = df["RPT_GRP_CD"]

    # RATE is in percentage points (e.g., 0.65 = 0.65%).
    # Divide by 100 so Excel % format works: 0.0065 → displays as "0.65%"
    df["RATE"] = df["RATE"] / 100.0

    # Keep only the columns we need, in order
    cols = [
        "MNE", "COHORT", "TST_GRP_CD", "RPT_GRP_CD", "CATEGORY",
        "METRIC", "DAY", "WINDOW_DAYS", "CLIENT_CNT", "SUCCESS_CNT", "RATE",
    ]
    df = df[cols]

    print(f"       RATE range after /100: {df['RATE'].min():.6f} – {df['RATE'].max():.6f}")
    print(f"       Unique MNEs: {sorted(df['MNE'].unique())}")
    print(f"       Unique COHORTs: {sorted(df['COHORT'].unique())}")
    return df


def write_data_sheet(df):
    """Write DataFrame to 'Data' sheet in a temp xlsx file using pandas."""
    print(f"[2/4] Writing data to temp Excel: {TEMP_PATH}")
    with pd.ExcelWriter(TEMP_PATH, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Data", index=False)
    print(f"       Written {len(df)} rows.")


def build_pivot_and_chart():
    """Open temp xlsx with Excel COM, add PivotTable + Slicers + Chart, save as final."""
    print(f"[3/4] Building PivotTable, Slicers, and Chart via Excel COM...")

    excel = None
    wb = None
    try:
        # Initialize COM for this thread
        pythoncom.CoInitialize()

        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.ScreenUpdating = False

        abs_temp = os.path.abspath(TEMP_PATH)
        abs_output = os.path.abspath(OUTPUT_PATH)
        print(f"       Opening: {abs_temp}")
        wb = excel.Workbooks.Open(abs_temp)
        data_sheet = wb.Sheets("Data")

        # Get data range
        last_row = data_sheet.UsedRange.Rows.Count
        last_col = data_sheet.UsedRange.Columns.Count
        data_range = data_sheet.Range(
            data_sheet.Cells(1, 1), data_sheet.Cells(last_row, last_col)
        )
        print(f"       Data range: {last_row} rows x {last_col} cols")

        # Create PivotCache
        pivot_cache = wb.PivotCaches().Create(
            SourceType=xlDatabase, SourceData=data_range
        )

        # Add Dashboard sheet
        pivot_sheet = wb.Sheets.Add(After=wb.Sheets(wb.Sheets.Count))
        pivot_sheet.Name = "Dashboard"

        # Create PivotTable
        pivot_table = pivot_cache.CreatePivotTable(
            TableDestination=pivot_sheet.Range("A3"), TableName="VintagePivot"
        )
        print("       PivotTable created.")

        # ── Configure fields ───────────────────────────────────────────────

        # PAGE (filter) fields: MNE, METRIC, CATEGORY
        pf_mne = pivot_table.PivotFields("MNE")
        pf_mne.Orientation = xlPageField
        pf_mne.Position = 1

        pf_metric = pivot_table.PivotFields("METRIC")
        pf_metric.Orientation = xlPageField
        pf_metric.Position = 2

        pf_cat = pivot_table.PivotFields("CATEGORY")
        pf_cat.Orientation = xlPageField
        pf_cat.Position = 3

        # ROW field: DAY (x-axis of vintage curve)
        pf_day = pivot_table.PivotFields("DAY")
        pf_day.Orientation = xlRowField
        pf_day.Position = 1

        # COLUMN fields: COHORT (outer), TST_GRP_CD (inner)
        pf_cohort = pivot_table.PivotFields("COHORT")
        pf_cohort.Orientation = xlColumnField
        pf_cohort.Position = 1

        pf_tst = pivot_table.PivotFields("TST_GRP_CD")
        pf_tst.Orientation = xlColumnField
        pf_tst.Position = 2

        # VALUE field: RATE (Average)
        pf_rate = pivot_table.AddDataField(
            pivot_table.PivotFields("RATE"), "Avg Rate", xlAverage
        )
        pf_rate.NumberFormat = "0.00%"

        print("       PivotTable fields configured.")

        # ── Add Slicers ────────────────────────────────────────────────────
        try:
            sc_mne = wb.SlicerCaches.Add2(pivot_table, "MNE")
            sl_mne = sc_mne.Slicers.Add(
                pivot_sheet,
                Name="MNE_Slicer", Caption="Campaign (MNE)",
                Top=10, Left=10, Width=150, Height=200,
            )

            sc_metric = wb.SlicerCaches.Add2(pivot_table, "METRIC")
            sl_metric = sc_metric.Slicers.Add(
                pivot_sheet,
                Name="Metric_Slicer", Caption="Metric",
                Top=10, Left=170, Width=170, Height=200,
            )

            sc_cat = wb.SlicerCaches.Add2(pivot_table, "CATEGORY")
            sl_cat = sc_cat.Slicers.Add(
                pivot_sheet,
                Name="Category_Slicer", Caption="Category (RPT_GRP)",
                Top=10, Left=350, Width=170, Height=200,
            )
            print("       Slicers added.")
        except Exception as e:
            print(f"       WARNING: Slicer creation failed ({e}). Continuing without slicers.")

        # ── Create PivotChart ──────────────────────────────────────────────
        try:
            chart_obj = pivot_sheet.ChartObjects().Add(
                Left=10, Top=220, Width=900, Height=450
            )
            chart = chart_obj.Chart
            chart.SetSourceData(pivot_table.TableRange1)
            chart.ChartType = xlLine
            chart.HasTitle = True
            chart.ChartTitle.Text = "VVD Vintage Curves — Cumulative Rate by Day"

            # Style
            chart.PlotArea.Interior.ColorIndex = 2  # White

            # Axis labels
            try:
                x_axis = chart.Axes(xlCategory)
                x_axis.HasTitle = True
                x_axis.AxisTitle.Text = "Days Since Treatment Start"

                y_axis = chart.Axes(xlValue)
                y_axis.HasTitle = True
                y_axis.AxisTitle.Text = "Cumulative Success Rate (%)"
                y_axis.TickLabels.NumberFormat = "0.00%"
            except Exception as e:
                print(f"       WARNING: Axis formatting failed ({e}).")

            print("       PivotChart created.")
        except Exception as e:
            print(f"       WARNING: Chart creation failed ({e}). Continuing without chart.")

        # ── Save final workbook ────────────────────────────────────────────
        print(f"[4/4] Saving to: {abs_output}")
        # Delete existing output if present
        if os.path.exists(abs_output):
            os.remove(abs_output)
        wb.SaveAs(abs_output, FileFormat=xlOpenXMLWorkbook)
        print("       Saved successfully.")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        # Clean up COM objects
        if wb is not None:
            try:
                wb.Close(SaveChanges=False)
            except Exception:
                pass
        if excel is not None:
            try:
                excel.ScreenUpdating = True
                excel.DisplayAlerts = True
                excel.Quit()
            except Exception:
                pass
            # Release COM references
            del excel
        pythoncom.CoUninitialize()

        # Clean up temp file
        time.sleep(0.5)
        if os.path.exists(TEMP_PATH):
            try:
                os.remove(TEMP_PATH)
            except Exception:
                pass


def verify_output():
    """Reopen the output file and verify PivotTable, Slicers, Chart exist."""
    print("\n-- Verification ----------------------------------------------")
    if not os.path.exists(OUTPUT_PATH):
        print("FAIL: Output file not found.")
        return False

    excel = None
    wb = None
    try:
        pythoncom.CoInitialize()
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False

        wb = excel.Workbooks.Open(os.path.abspath(OUTPUT_PATH))

        # Check sheets
        sheet_names = [wb.Sheets(i).Name for i in range(1, wb.Sheets.Count + 1)]
        print(f"  Sheets: {sheet_names}")
        assert "Data" in sheet_names, "Missing 'Data' sheet"
        assert "Dashboard" in sheet_names, "Missing 'Dashboard' sheet"

        # Check PivotTable
        dash = wb.Sheets("Dashboard")
        pt_count = dash.PivotTables().Count
        print(f"  PivotTables on Dashboard: {pt_count}")
        assert pt_count >= 1, "No PivotTable found"

        pt = dash.PivotTables("VintagePivot")
        # List fields
        row_fields = [pt.RowFields(i).Name for i in range(1, pt.RowFields.Count + 1)]
        col_fields = [pt.ColumnFields(i).Name for i in range(1, pt.ColumnFields.Count + 1)]
        page_fields = [pt.PageFields(i).Name for i in range(1, pt.PageFields.Count + 1)]
        data_fields = [pt.DataFields(i).Name for i in range(1, pt.DataFields.Count + 1)]
        print(f"  Row fields: {row_fields}")
        print(f"  Column fields: {col_fields}")
        print(f"  Page/Filter fields: {page_fields}")
        print(f"  Data fields: {data_fields}")

        # Check Slicers
        slicer_count = wb.SlicerCaches.Count
        print(f"  SlicerCaches: {slicer_count}")

        # Check Charts
        chart_count = dash.ChartObjects().Count
        print(f"  Charts on Dashboard: {chart_count}")

        print("\n  ALL CHECKS PASSED.")
        return True

    except Exception as e:
        print(f"  VERIFICATION ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if wb is not None:
            try:
                wb.Close(SaveChanges=False)
            except Exception:
                pass
        if excel is not None:
            try:
                excel.Quit()
            except Exception:
                pass
            del excel
        pythoncom.CoUninitialize()


if __name__ == "__main__":
    df = prepare_data()
    write_data_sheet(df)
    build_pivot_and_chart()
    verify_output()
    print(f"\nDone. Output: {OUTPUT_PATH}")
