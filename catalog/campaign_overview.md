# VVD Campaign Overview — Experiment Design

6 independent campaigns measured for the Virtual Visa Debit (VVD) product.
Measurement period: January 2025 – February 2026.

## Campaign Summary

| # | MNE | Campaign Name | Type | Primary Metric | Channel(s) | A/C Split | Window | Cohorts | Period |
|---|-----|--------------|------|---------------|------------|-----------|--------|---------|--------|
| 1 | VCN | Contextual Notification | Batch (monthly) | card_acquisition | MB (push) | 95/5 | 30d | 12 | Feb 2025 – Feb 2026 |
| 2 | VDA | Seasonal Acquisition | Batch (seasonal) | card_acquisition | EM+IM, IM | 95/5 | 90d | 2 | Jul 2025, Nov 2025 |
| 3 | VDT | Activation Trigger | Triggered | card_activation | EM, DO | 90/10 | 30d | 14 | Jan 2025 – Feb 2026 |
| 4 | VUI | Usage Trigger | Triggered | card_usage | EM+IM, EM, IM | 95/5 | 30d | 13 | Feb 2025 – Feb 2026 |
| 5 | VUT | Tokenization Usage | Batch (single) | wallet_provisioning | EM (email) | 95/5 | 90d | 1 | Jun 2025 |
| 6 | VAW | Add To Wallet | Batch (monthly) | wallet_provisioning | IM (in-app) | 80/20 | 30d | 10 | Apr 2025 – Feb 2026 |

## Deployment Volumes

| MNE | Total Deployments (Action) | Total Deployments (Control) | Unique Clients (Action) | Unique Clients (Control) | Avg Deployment per Cohort (Action) |
|-----|---------------------------|----------------------------|------------------------|-------------------------|-----------------------------------|
| VCN | 14,671,418 | 825,842 | 1,962,335 | 109,387 | ~1,222,618 |
| VDA | 1,510,982 | 79,506 | 894,779 | 77,817 | ~755,491 |
| VDT | 714,157 | 22,946 | 130,079 | 14,470 | ~51,011 |
| VUI | 364,612 | 7,807 | 148,822 | 7,807 | ~28,047 |
| VUT | 784,849 | 41,307 | 784,849 | 41,307 | 784,849 (single) |
| VAW | 401,708 | 100,422 | 401,708 | 100,422 | ~40,171 |

Notes:
- VCN: Deployments >> Unique clients because same client is re-targeted monthly. ~1.1M clients per cohort but only ~2M unique across 12 months.
- VDA: 2 seasonal waves. Significant client overlap between Jul and Nov cohorts (894K unique vs 1.5M total deployments).
- VDT: Triggered = each deployment is a unique event (card issued → trigger fires). ~51K triggers/month.
- VUI: Triggered = fires when usage conditions met. ~28K triggers/month. Smallest control group (N=~600 per cohort).
- VUT: Single batch deployment. No repeat targeting.
- VAW: Monthly batch but deployment = unique client (no re-targeting within window).

## Experiment Design Details

### VCN — Contextual Notification
- **Deployment**: Monthly batch. All non-VVD-holders screened through decision tree
- **Channel**: MB (mobile push notification). 2 report groups: PVCNAG01 = Age of Majority (standard marketing), PVCNAG02 = Student 14+ (operational). NOT a creative test — demographic segmentation
- **Control**: Organic — not communicated to
- **Measurement**: card_acquisition within 30 days of deployment
- **Note**: Persistent SRM (sample ratio mismatch) across all cohorts. Observed 94.7/5.3 vs designed 95/5

### VDA — Seasonal Acquisition
- **Deployment**: Seasonal batch, ~2x/year
- **Channel**: 2-arm channel test. PVDAAG03 = EM+IM (email + in-app). PVDAAG04 = IM only (in-app)
- **Control**: Organic
- **Measurement**: card_acquisition within 90 days of deployment
- **Note**: SRM observed. 92/8 vs designed 95/5. Email metrics tracked for both arms

### VDT — Activation Trigger
- **Deployment**: Triggered 7 days post-card-issuance. 7-day reminder if no activation
- **Channel**: 6 report groups. 4 × EM (email): PVDTAG01/03/11/13. 2 × DO (digital online): PVDTAG02/04
- **Control**: Organic
- **Measurement**: card_activation within 30 days of trigger
- **Note**: 95% of VVD cards auto-activate at issuance. Campaign targets the ~5% requiring manual activation. 90/10 split gives larger control for this smaller population

### VUI — Usage Trigger
- **Deployment**: Triggered based on usage inactivity conditions
- **Channel**: 4 active report groups (AoM only — Student segments PVUIAG05/06 REMOVED in config docs). PVUIAG01=AoM EM+IM, PVUIAG02=AoM EM-only, PVUIAG03=AoM IM-only (+email dedup overflow), PVUIAG04=Student 14+ EM
- **Control**: Organic. Very small control group (~580-720 per cohort)
- **Measurement**: card_usage (transaction) within 30 days of trigger
- **Note**: Oct 2025 deep dive recommended expanding target population, increasing control to 15%, relaxing 365-day resting rule. Implementation status unknown

### VUT — Tokenization Usage
- **Deployment**: Single batch, June 2025
- **Channel**: EM (email). Only PVUTAG01 is active — PVUTAG02-06 ALL REMOVED (all Student segments and all IM channels eliminated in config docs)
- **Control**: Organic
- **Measurement**: wallet_provisioning within 90 days
- **Note**: SAS success definition uses -30 day pre-treatment window (counts activity 30 days BEFORE treatment start). Known bug in flag logic (any_wallet references wrong alias)

### VAW — Add To Wallet
- **Deployment**: Monthly batch. Targets mobile-engaged iOS users with 6-day recency. Excludes already-provisioned
- **Channel**: IM (in-app). Single report group PVAWAG01
- **Control**: Organic
- **Measurement**: wallet_provisioning within 30 days of deployment
- **Note**: 80/20 split (larger control than most). iOS only (Apple Pay). Same goal as VUT but fundamentally different design

## Channel Distribution

| Channel Code | Full Name | Campaigns | Type |
|-------------|-----------|-----------|------|
| MB | Mobile push notification | VCN | Passive (notification tray) |
| EM | Email | VDA, VDT, VUI, VUT | Active (inbox delivery) |
| IM | In-app message | VAW, VDA, VUI | Active (requires app open) |
| DO | Digital Online | VDT | Unknown — needs verification |
| EM+IM | Email + In-app | VDA, VUI | Multi-touch |

## Report Group Structure

| MNE | # RPT_GRP_CDs | Codes | Channel Split |
|-----|---------------|-------|---------------|
| VAW | 1 | PVAWAG01 | IM only |
| VCN | 2 | PVCNAG01, PVCNAG02 | Both MB — AoM vs Student 14+ (demographic split, not channel/creative) |
| VDA | 2 | PVDAAG03, PVDAAG04 | EM+IM vs IM (channel test) |
| VDT | 6 | PVDTAG01/02/03/04/11/13 | 4×EM + 2×DO |
| VUI | 4 | PVUIAG01/02/03/04 | AoM: EM+IM, EM, IM(+dedup overflow), Student: EM. AG05/06 removed |
| VUT | 1 | PVUTAG01 | EM only. AG02-06 all removed (Student + IM channels eliminated) |
