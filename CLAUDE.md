# VVD v2 — Project Instructions

## Project Overview
VVD (Virtual Visa Debit) campaign measurement and optimization. v2 consolidates v1 analysis findings with Vintage/Vvd pipeline logic into a reproducible, pipeline-first structure.

## Project Structure
- `archive/v1/` — Original v1 analysis (Python, Markdown, HTML). Reference only, do not modify.
- `catalog/` — Analytical knowledge from v1: calculations, data dictionary, success definitions, known issues, key findings.
- `pipeline/` — Pipeline definitions from Vintage/Vvd: success library, campaign config, source tables, reconciliation, UCP gap analysis.
- `Context_reference.md` — Start here. Single consolidated reference for the full project.

## Key Technical Rules
1. **Test groups**: TG4 = Action, TG7 = Control. Always filter appropriately.
2. **CLNT_NO**: String, no leading zeros. Strip with REGEXP_REPLACE("^0+", "").
3. **Success columns are UPPERCASE**: ACQUISITION_SUCCESS, ACTIVATION_SUCCESS, USAGE_SUCCESS, PROVISIONING_SUCCESS.
4. **PySpark only**: No pandas. Use explicit type conversions (int(), float(), str()) on Row objects.
5. **Card_Type is 92% null**: Do not use for segmentation.
6. **UCP fields are monthly snapshots**: Deduplicate with .select("CLNT_NO").distinct() for client-level analysis.
7. **Plotly CDN method**: Use `fig.to_html(include_plotlyjs='cdn')` in Lumina environment.

## Campaign Success Mapping (v2 — Resolved)
| Campaign | MNE | Primary Metric | Source |
|----------|-----|---------------|--------|
| VVD Contextual Notification | VCN | card_acquisition | HIVE |
| VVD Black Friday | VDA | card_acquisition | HIVE |
| VVD Activation Trigger | VDT | card_activation | HIVE |
| VVD Usage Trigger | VUI | card_usage | HIVE |
| VVD Tokenization | VUT | wallet_provisioning | EDW |
| VVD Add To Wallet | VAW | wallet_provisioning | EDW |

## Sibling Project
Vintage/Vvd pipeline at `C:\Users\andre\New_projects\Vintage\Vvd\` — modular pipeline (M1-M7) for vintage curve generation. Source of truth for success definitions and campaign metadata.

## Known Issues to Watch
- VCN has persistent SRM warnings (p=0.0000) across all quarters
- VDA previously used ACTIVATION_SUCCESS as workaround — v2 corrects to card_acquisition
- VUT has negative ROI — recommend discontinuation
- UCP sourcing is blocked on data engineering team
