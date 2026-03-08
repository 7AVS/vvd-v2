# VVD v2 — PowerPoint Deck Builder

## Architecture

```
Lumina (PySpark)                     Local (Windows)
────────────────                     ───────────────
Pipeline Cells 1-10                  pptx/
  result_df                            theme.py         ← brand colors, fonts
  vintage_curves.csv                   track_a.py       ← Plotly PNG → embed
  summary_tables.csv                   track_b.py       ← native editable charts
       ↓                               tables.py        ← native pptx tables
  Download CSVs → pptx/data/           build_deck.py    ← orchestrator
                                       template.pptx    ← designed in PowerPoint
                                       data/            ← CSVs from pipeline
                                         ↓
                                    VVD_v2_Report.pptx
```

## Two Tracks

| | Track A (Image) | Track B (Native) |
|---|---|---|
| Chart engine | Plotly → PNG (scale=3) | python-pptx CategoryChartData |
| Editable? | No (raster image) | Yes (click chart → see data) |
| Styling | Full Plotly control | Limited to pptx chart API |
| Combo charts | Yes | No |
| Best for | Complex charts, vintage curves | Simple bar/line, stakeholder editing |

## Slide Layout (3-4 dense dashboard slides)

Each campaign slide is a dashboard panel:
```
┌─────────────────────────────────────────────────────┐
│  CAMPAIGN TITLE (MNE — Full Name)          [STATUS] │
├──────────────────────┬──────────────────────────────┤
│                      │  KPI CARDS                   │
│   VINTAGE CURVE      │  ┌─────┐ ┌─────┐ ┌─────┐   │
│   (Action vs Control)│  │Lift │ │p-val│ │Depl │   │
│                      │  │+0.2%│ │<.001│ │38M  │   │
│                      │  └─────┘ └─────┘ └─────┘   │
├──────────────────────┼──────────────────────────────┤
│   LIFT BAR CHART     │  KEY FINDINGS                │
│   (by cohort or      │  - Finding 1                 │
│    campaign)         │  - Finding 2                 │
│                      │  RECOMMENDATION              │
│                      │  Continue / Optimize / Stop   │
└──────────────────────┴──────────────────────────────┘
```

## Slide Inventory (draft)

| # | Slide | Campaigns | Content |
|---|-------|-----------|---------|
| 1 | Title + Exec Summary | All | Headline numbers, portfolio verdict |
| 2 | Acquisition | VCN + VDA | Vintage curves, lift, KPIs |
| 3 | Activation + Usage | VDT + VUI | Same pattern |
| 4 | Provisioning | VUT + VAW | Same + discontinue VUT recommendation |

## Brand Design System (from Vintage/Vvd)

### Colors
| Name | Hex | Usage |
|------|-----|-------|
| Dark Blue | #003168 | Headers, titles, borders |
| Bright Blue | #0051A5 | Primary data series |
| Ocean | #0091DA | Action group, secondary data |
| Warm Yellow | #FFC72C | Accents, highlights |
| Tundra | #07AFBF | Positive lift, future state |
| Sunburst | #FCA311 | Negative lift, key opportunity (limit 1-2/slide) |
| Cool White | #E7EEF1 | Backgrounds, neutral cards |
| Gray | #9EA2A2 | Control group, neutral |

### Chart Rules
- Action = solid line (#0091DA Ocean), Control = dashed line (#9EA2A2 Gray)
- Cohort opacity: newest=1.0, oldest=0.3
- KPI cards: positive lift → tundra border, negative → sunburst border
- Max 5-6 colors per chart
- Font: Segoe UI

### Chart Color Sequence
```
#0051A5, #FFC72C, #0091DA, #07AFBF, #FCA311, #C1B5E0, #003168, #B58500
```

## Data Requirements

CSVs exported from pipeline (Cell 10 + new export cell):

1. **vintage_curves.csv** — MNE, COHORT, TST_GRP_CD, RPT_GRP_CD, METRIC, DAY, WINDOW_DAYS, CLIENT_CNT, SUCCESS_CNT, RATE
2. **campaign_summary.csv** — MNE, TST_GRP_CD, total_clients, success_count, success_rate, lift, p_value, ci_lower, ci_upper
3. **srm_check.csv** — MNE, COHORT, expected_ratio, actual_ratio, chi2, p_value, warning

## Dependencies

```
pip install python-pptx plotly kaleido
```

Kaleido v1 requires Chrome/Chromium. If unavailable: `pip install "kaleido<1.0"` (bundles own Chromium).
