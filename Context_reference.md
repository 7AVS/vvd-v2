# VVD v2 — Project Context Reference

## What Is VVD?

VVD (Virtual Visa Debit) is a suite of 6 marketing campaigns at RBC aimed at driving Visa Debit card acquisition, activation, usage, and digital wallet provisioning. Each campaign targets specific lifecycle stages with A/B testing (action vs control groups).

## Campaign Portfolio

| MNE | Full Name | Goal | Type | Test/Control | Status |
|-----|-----------|------|------|-------------|--------|
| VCN | VVD Contextual Notification | Acquisition | Trigger | 95/5 | Active since 2021 — dominant volume |
| VDA | VVD Black Friday Cyber Monday | Acquisition | Batch | 95/5 | Seasonal (Jul-Nov) |
| VDT | VVD Activation Trigger | Activation | Trigger | 90/10 | Active since 2024 — best performer |
| VUI | VVD Usage Trigger | Usage | Trigger | 95/5 | Active since 2024 |
| VUT | VVD Tokenization Usage | Provisioning | Trigger | 95/5 | Active — negative ROI, recommend discontinuation |
| VAW | VVD Add To Wallet | Provisioning | Trigger | 80/20 | New (Apr 2025) — promising early results |

## Project History

### v1 (Jun-Aug 2025)
- Consumed a pre-built `final_df.parquet` (27M rows, 199 fields) with no extraction pipeline
- Produced 4-phase analysis: Universe Overview → Campaign Interaction → Overcontacting → Deep Dive
- Also: Journey/Sankey analysis, Demographic exploration, PowerPoint generation
- Key deliverable: Campaign-level one-pagers with lift/significance/demographics
- Data period: Jan 2024 - May 2025 (17 months)
- All code and results archived in `archive/v1/`

### v2 (Mar 2026 — current)
- Consolidates VVD v1 analysis with Vintage/Vvd pipeline project
- Pipeline-first approach: success definitions, source tables, and measurement logic are documented and reproducible
- Structure: `catalog/` (what we learned), `pipeline/` (how to measure), `archive/v1/` (original work)
- Goal: Rebuild measurement capability, validate with fresh data, produce meeting-ready outputs

## Key Reference Documents

### Catalog (from v1 analysis)
- `catalog/CALCULATIONS_CATALOG.md` — Every metric, formula, filter from Phases 1-4 + journey + demographics
- `catalog/DATA_DICTIONARY.md` — All 199 fields from final_df, organized by functional group
- `catalog/SUCCESS_DEFINITIONS.md` — Per-campaign success mapping with source logic
- `catalog/KNOWN_ISSUES.md` — Data quality issues, SRM warnings, workarounds
- `catalog/KEY_FINDINGS.md` — Analytical conclusions and strategic recommendations

### Pipeline (from Vintage/Vvd)
- `pipeline/SUCCESS_LIBRARY.md` — 4 success metrics with complete SQL/filter logic
- `pipeline/CAMPAIGN_CONFIG.md` — MNE → metric → window mapping
- `pipeline/SOURCE_TABLES.md` — All source tables (Hive/EDW) with paths and key fields
- `pipeline/RECONCILIATION.md` — VVD v1 vs Vintage/Vvd field comparison (all resolved)
- `pipeline/UCP_GAP_ANALYSIS.md` — Demographics gap: ~100 UCP fields need sourcing

## Critical Technical Facts

1. **Test Groups**: TG4 = Action (received campaign), TG7 = Control (no contact)
2. **CLNT_NO**: String with NO leading zeros — all modules must strip via REGEXP_REPLACE("^0+", "")
3. **Success Columns**: UPPERCASE in final_df (ACQUISITION_SUCCESS, ACTIVATION_SUCCESS, USAGE_SUCCESS, PROVISIONING_SUCCESS)
4. **VDA Workaround**: v1 used ACTIVATION_SUCCESS for VDA due to backend issue; v2 uses card_acquisition (ISS_DT based)
5. **Card_Type**: 92% null/unknown — do not rely on this field
6. **UCP Fields**: Monthly snapshots repeated per deployment row — deduplicate with .select("CLNT_NO").distinct()
7. **Environment**: Enterprise Jupyter on Lumina (Spark/YARN cluster). Plotly must use CDN method. PySpark only.
8. **Data Source**: Hive parquet on HDFS + EDW/Teradata for tokenization tables

## v1 Key Findings (Hypotheses for v2 Validation)

1. **VCN overcontacting**: 82.9% of volume, 0.60% success. Returns collapse after 3 contacts. First 2-3 contacts capture 70% of results.
2. **Volume-performance inverse**: VCN (0.60%) has 100x worse success than VDT (59.6%) but 70x more volume.
3. **No orchestration**: 41% of clients have overlapping campaigns. VCN→VCN self-loops dominate (1.67M transitions).
4. **Diminishing returns**: Success peaks at 3-4 contacts, drops 30% after 5th. 24.8% of clients get 11+ contacts.
5. **Demographic opportunity**: Age 18-24 is 5X better than 65+. New customers (0-30 days): 6.35% vs 10+ years: 0.39%.
6. **VUT negative ROI**: Only campaign with negative lift (-0.2%), revenue (-$12K), ROMI (-18X).
7. **VAW promising**: +2.86% lift, 92.8% relative improvement, 99.9% significance despite being newest.
8. **Channel effectiveness**: Email outperforms desktop across all campaigns. VDA Email+Banner: 3.6X vs banner-only.

## Current Gaps & Dependencies

| Gap | Impact | Owner | Status |
|-----|--------|-------|--------|
| UCP source table | Cannot do demographic analysis | Data Engineering | BLOCKED — need table name, join logic |
| Fresh data extraction | Cannot validate v1 findings | Pipeline team | IN PROGRESS — Vintage/Vvd pipeline exists |
| Measurement windows | Implicit in v1, need explicit config | This project | RESOLVED — using Vintage/Vvd config |
| VDA/VAW metric mapping | Incorrect success columns in v1 | This project | RESOLVED — using pipeline definitions |

## Folder Structure

```
VVD/
├── archive/v1/           # All original files, untouched
├── catalog/              # What we learned from v1
│   ├── CALCULATIONS_CATALOG.md
│   ├── DATA_DICTIONARY.md
│   ├── SUCCESS_DEFINITIONS.md
│   ├── KNOWN_ISSUES.md
│   └── KEY_FINDINGS.md
├── pipeline/             # How to measure (from Vintage/Vvd)
│   ├── SUCCESS_LIBRARY.md
│   ├── CAMPAIGN_CONFIG.md
│   ├── SOURCE_TABLES.md
│   ├── RECONCILIATION.md
│   └── UCP_GAP_ANALYSIS.md
├── Context_reference.md  # This file
└── CLAUDE.md             # AI assistant instructions
```
