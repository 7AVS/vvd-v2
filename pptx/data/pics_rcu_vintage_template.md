# RCU Vintage Curve Excel Template — Structure Description

Source: Excel Online — "NBA Measurement & Analytics > RCU"
Photos: PXL_20260308_193953384.jpg, PXL_20260308_193958503.MP.jpg, PXL_20260308_194022711.jpg, PXL_20260308_194029386.MP.jpg

---

## Tab Structure

Visible tabs at the bottom of the workbook:
1. **Data Extract** (active in Photo 1)
2. **Calculations**
3. **Calculations MBy** (Calculations by Month — likely monthly breakdowns)
4. **Calculations MBn** (?)
5. **Calculations MBy** (second instance? or different suffix — blurry in Photo 2)
6. **Calculations MBn** (? — blurry)

Note: Photo 2 (PXL_20260308_193958503.MP.jpg) is blurry — tabs visible but text hard to read. It appears to show more rows of the Data Extract tab scrolled down, with the same tab structure visible.

---

## Tab 1: Data Extract

### Column Layout (Photo 1 — PXL_20260308_193953384.jpg)

| Column | Header | Description |
|--------|--------|-------------|
| A | Cohort | Wave identifier, e.g., "W-Feb24" |
| B | Metric | e.g., "Mobile_Active" |
| C | Test Group | e.g., "Test" |
| D | Period | Integer counter: 0, 1, 2, 3, ... up to 49+ |
| E | Metric Value | Cumulative count at each period |
| F | Base Value | Total population base (constant per cohort) |
| G | (empty) | |
| H | (notes) | "Base:", "Total - all clients selected as o..." (truncated), "Open - all clients open as of p..." (truncated) |

### Sample Data (W-Feb24, Mobile_Active, Test)

| Row | Cohort | Metric | Test Group | Period | Metric Value | Base Value |
|-----|--------|--------|------------|--------|--------------|------------|
| 2 | W-Feb24 | Mobile_Active | Test | 0 | 0 | 0 |
| 3 | W-Feb24 | Mobile_Active | Test | 1 | 11 | 56097 |
| 4 | W-Feb24 | Mobile_Active | Test | 2 | 15 | 56097 |
| 5 | W-Feb24 | Mobile_Active | Test | 3 | 19 | 56097 |
| 6 | W-Feb24 | Mobile_Active | Test | 4 | 1114 | 56097 |
| 7 | W-Feb24 | Mobile_Active | Test | 5 | 1122 | 56097 |
| 8 | W-Feb24 | Mobile_Active | Test | 6 | 1165 | 56097 |
| 9 | W-Feb24 | Mobile_Active | Test | 7 | 3832 | 56097 |
| 10 | W-Feb24 | Mobile_Active | Test | 8 | 3835 | 56097 |
| 11 | W-Feb24 | Mobile_Active | Test | 9 | 3846 | 56097 |
| 12 | W-Feb24 | Mobile_Active | Test | 10 | 3849 | 56097 |
| 13 | W-Feb24 | Mobile_Active | Test | 11 | 4827 | 56097 |
| 14 | W-Feb24 | Mobile_Active | Test | 12 | 4846 | 56097 |
| 15 | W-Feb24 | Mobile_Active | Test | 13 | 4847 | 56097 |
| 16 | W-Feb24 | Mobile_Active | Test | 14 | 6360 | 56097 |
| 17 | W-Feb24 | Mobile_Active | Test | 15 | 6362 | 56097 |
| 18 | W-Feb24 | Mobile_Active | Test | 16 | 6370 | 56097 |
| 19 | W-Feb24 | Mobile_Active | Test | 17 | 6390 | 56097 |
| 20 | W-Feb24 | Mobile_Active | Test | 18 | 7223 | 56097 |
| 21 | W-Feb24 | Mobile_Active | Test | 19 | 7230 | 56097 |
| 22 | W-Feb24 | Mobile_Active | Test | 20 | 7231 | 56097 |
| 23 | W-Feb24 | Mobile_Active | Test | 21 | 7841 | 56097 |
| 24 | W-Feb24 | Mobile_Active | Test | 22 | 7842 | 56097 |
| 25 | W-Feb24 | Mobile_Active | Test | 23 | 7852 | 56097 |
| 26 | W-Feb24 | Mobile_Active | Test | 24 | 7870 | 56097 |
| 27 | W-Feb24 | Mobile_Active | Test | 25 | 9906 | 56097 |
| 28 | W-Feb24 | Mobile_Active | Test | 26 | 9925 | 56097 |
| 29 | W-Feb24 | Mobile_Active | Test | 27 | 9931 | 56097 |
| 30 | W-Feb24 | Mobile_Active | Test | 28 | 10435 | 56097 |
| 31 | W-Feb24 | Mobile_Active | Test | 29 | 10436 | 56097 |
| 32 | W-Feb24 | Mobile_Active | Test | 30 | 10456 | 56097 |
| 33 | W-Feb24 | Mobile_Active | Test | 31 | 11473 | 56097 |
| 34 | W-Feb24 | Mobile_Active | Test | 32 | 11475 | 56097 |
| 35 | W-Feb24 | Mobile_Active | Test | 33 | 11482 | 56097 |
| 36 | W-Feb24 | Mobile_Active | Test | 34 | 11489 | 56097 |
| 37 | W-Feb24 | Mobile_Active | Test | 35 | 12385 | 56097 |
| 38 | W-Feb24 | Mobile_Active | Test | 36 | 12401 | 56097 |
| 39 | W-Feb24 | Mobile_Active | Test | 37 | 12417 | 56097 |
| 40 | W-Feb24 | Mobile_Active | Test | 38 | 12447 | 56097 |
| 41 | W-Feb24 | Mobile_Active | Test | 39 | 13216 | 56097 |
| 42 | W-Feb24 | Mobile_Active | Test | 40 | 13228 | 56097 |
| 43 | W-Feb24 | Mobile_Active | Test | 41 | 13755 | 56097 |
| 44 | W-Feb24 | Mobile_Active | Test | 42 | 13764 | 56097 |
| 45 | W-Feb24 | Mobile_Active | Test | 43 | 13784 | 56097 |
| 46 | W-Feb24 | Mobile_Active | Test | 44 | 13796 | 56097 |
| 47 | W-Feb24 | Mobile_Active | Test | 45 | 13801 | 56097 |
| 48 | W-Feb24 | Mobile_Active | Test | 46 | 14267 | 56097 |
| 49 | W-Feb24 | Mobile_Active | Test | 47 | 14277 | 56097 |
| 50 | W-Feb24 | Mobile_Active | Test | 48 | 14287 | 56097 |
| 51 | W-Feb24 | Mobile_Active | Test | 49 | 14763 | 56097 |

- Base Value = 56,097 (constant for all periods in this cohort/group)
- Metric Value is cumulative — monotonically increasing
- Period 0 starts at 0; jumps at periods 4, 7, 11, 14, 21, 25, 28, 31, etc. suggest weekly batch processing pattern

---

## Tab: Calculations (Photo 3 — PXL_20260308_194022711.jpg)

### Header area (rows 1-4):
- Row 1: Title area — top-right shows "Vintage Graph RCU 202510"
- Row 2: Cell A2 = "Total" (yellow background), instructions: "Replace with the name of the metric to be analyzed"
  - Cell K2 = "Total Clients" with note: "<- Enter the Chart Title"
- Row 3: Cell A3 = "Test" (yellow), instruction: "Replace with name of Test Group"
- Row 4: Cell A4 = "Control" (yellow), instruction: "Replace with name of Control Group"

### Column Layout (row 5 headers):
- Columns B through Z+ contain cohort wave identifiers as column headers:
  - W-Feb24, W-Feb24, W-Apr24, W-Apr24, W-Jun24, W-Jun24, W-Aug24, W-Aug24, W-Oct24, W-Oct24, W-Dec24, W-Dec24, W-Feb25, W-Feb25, W-Apr25, W-Apr25, W-Jun25, W-Jun25, W-Aug25, W-Aug25
  - Pattern: each cohort appears twice (Test and Control)

### Data area (rows 6+):
- Row numbers in column A: 0, 1, 2, 3, 4, ...
- Values are PERCENTAGES (rates), e.g.:
  - Row 7 (Period 0): 0.00%, 0.00%, 0.00%, 0.00%, ...
  - Row 8 (Period 1): 0.00%, 0.00%, 0.00%, ...
  - Values increase down the rows (cumulative rate curves)
  - Later rows show values like 6.13%, 7.75%, 8.32%, 10.26%, etc.
  - Final visible rows reach ~22-25%

### Chart (visible on right side):
- Title: (partially visible, appears to be the chart for "Total Clients")
- Y-axis: 0.00% to 30.00% (increments of 5%)
- X-axis: Period numbers (0, 1, 2, 3, 4, 5, 6, ...)
- Legend shows:
  - Test W-Feb24 (solid blue line)
  - Control W-Aug24 (dotted red/dark line)
  - Test W-Apr25 (solid green line)
  - (more cohort lines)
- Vintage curve shape: S-curve / step-function increasing from 0% toward 25%+

---

## Tab: Calculations — Full Chart View (Photo 4 — PXL_20260308_194029386.MP.jpg)

### Chart Title: "Total Clients with Credit Limit Increases"

### Chart Configuration:
- **Y-axis**: 0.00% to 30.00% (5% increments)
- **X-axis**: Periods 0 through 60 (labeled at every integer)
- **Chart type**: Line chart, vintage curve style

### Legend (all cohort series visible):
- Test W-Feb24 (solid blue)
- Control W-Feb24 (dotted blue)
- Test W-Apr24 (solid line)
- Control W-Apr24 (dotted)
- Test W-Jun24 (solid)
- Control W-Jun24 (dotted)
- Test W-Aug24 (solid dark)
- Control W-Aug24 (dotted)
- Test W-Oct24 (solid)
- Control W-Oct24 (dotted)
- Test W-Dec24 (solid)
- Control W-Dec24 (dotted)
- Test W-Feb25 (solid)
- Control W-Feb25 (dotted)
- Test W-Apr25 (solid green)
- Control W-Apr25 (dotted)
- Test W-Jun25 (solid)
- Control W-Jun25 (dotted)
- Test W-Aug25 (solid)
- Control W-Aug25 (solid red/dark)

### Visual Pattern:
- Multiple vintage curves stacked/offset by cohort start date
- Each cohort starts at 0% and rises toward ~25% over 60 periods
- Older cohorts (Feb24) have longer curves reaching higher values
- Newer cohorts (Aug25) have shorter curves
- Test and Control lines for each cohort run very close together
- One cohort (appears to be W-Dec24 or similar) has a PINK/MAGENTA line that dips noticeably lower than others (~10% when peers are at ~15%), suggesting an anomaly
- Overall shape: rapid initial rise (periods 0-10), then gradual step-wise increase through period 60

### Data columns visible on left side:
- Percentage values for Test/Control pairs by cohort
- Values range from 0.00% to ~25.54%
- Step-function pattern visible in the data (values stay flat for a few periods, then jump)

---

## Key Structural Observations

1. **This is an RCU (Retail Credit Unit?) vintage curve template** — NOT the VVD vintage curves. The metric is "Total Clients with Credit Limit Increases" and cohorts are "Mobile_Active" test groups.
2. **Template design**: Configurable — user replaces metric name, test/control group names, and the chart auto-generates.
3. **Period = weeks** (based on ~60 periods spanning ~14 months from Feb24 to Aug25, and the step-function pattern suggesting weekly batches).
4. **Base Value = 56,097** for the W-Feb24 cohort — this is the total population size.
5. **Same structural pattern as VVD vintage curves** — cumulative rate over time, Test vs Control, multiple cohorts overlaid.
6. **Anomalous cohort** visible in chart (pink/magenta line) — runs well below peers, worth investigating.
