# VVD v2 Pipeline: Campaign Configuration

Imported from Vintage/Vvd CAMPAIGN_METADATA (src/vintage_engine.py lines 109-146).

## Campaign Metadata

| MNE | Campaign Name | Success Type | Primary Metric | Secondary Metric | Deployment | Test/Control |
|-----|---------------|-------------|----------------|-----------------|------------|-------------|
| VCN | VVD Contextual Notification | ACQUISITION | card_acquisition | — | Trigger | 95/5 |
| VDA | VVD Black Friday Cyber Monday Targeted | ACQUISITION | card_acquisition | — | Batch | 95/5 |
| VDT | VVD Activation Trigger | ACTIVATION | card_activation | — | Trigger | 90/10 |
| VUI | VVD Usage Trigger | USAGE | card_usage | wallet_provisioning | Trigger | 95/5 |
| VUT | VVD Tokenization Usage Campaign | TOKENIZATION | wallet_provisioning | card_usage | Trigger | 95/5 |
| VAW | VVD Add To Wallet Notification | TOKENIZATION | wallet_provisioning | card_usage | Trigger | 80/20 |

## Measurement Windows
- Default: percentile_approx(window_days, 0.5) — median measurement window
- Fallback: 90 days if not available
- Vintage curves built for each day from 0 to min(WINDOW_DAYS, max_day_observed)

## Configuration Constants
- AGGREGATION_LEVEL = "MONTH" (always monthly yyyy-MM)
- COHORT_DATE_FORMAT = "yyyy-MM"
- YEARS_TO_INCLUDE = [2025, 2026]
- TEST_GROUP_CODE = "TG4" (TG4 = Test, all others = Control)
- CONFIDENCE_LEVEL = 0.95

## Email Engagement Disposition Codes (M5)
| Code | Meaning |
|------|---------|
| 1 | email_sent |
| 2 | email_opened |
| 3 | email_clicked |
| 4 | email_unsubscribed |
| 5 | email_hardbounce |
