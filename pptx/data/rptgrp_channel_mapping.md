# RPT_GRP_CD → Channel Mapping (from Campaign Design Documents)

## Channel Codes
- **MB** = Mobile Banking (push notification / banner)
- **EM** = Email
- **IM** = In-App Message (banner / ad-serve)
- **DO** = Digital Online
- **EM_IM** = Email + In-App Message
- **XX** = Control holdout (not communicated)

## Complete Mapping

| MNE | RPT_GRP_CD | Channel | Segment | Description |
|-----|-----------|---------|---------|-------------|
| VAW | PVAWAG01 | IM | All | iOS, active mobile, 6-day recency, never provisioned |
| VCN | PVCNAG01 | MB | Age of Majority | Standard marketing, active PBA+Mobile, no VVD |
| VCN | PVCNAG02 | MB | Student 14+ | Student operational, active PBA+Mobile, no VVD |
| VDA | PVDAAG03 | EM_IM | All | Email + In-App, 250K email cap, tenure <8yr priority |
| VDA | PVDAAG04 | IM | All | Banner/Ad-serve only, no email cap |
| VDT | PVDTAG01 | EM | Student | Initial activation email, 7 days post-issuance |
| VDT | PVDTAG02 | DO | Student | Digital Online, not email eligible |
| VDT | PVDTAG03 | EM | Non-Student | Initial activation email, 7 days post-issuance |
| VDT | PVDTAG04 | DO | Non-Student | Digital Online, not email eligible |
| VDT | PVDTAG11 | EM | Student | Reminder email, 8-12 days after initial |
| VDT | PVDTAG13 | EM | Non-Student | Reminder email, 8-12 days after initial |
| VUI | PVUIAG01 | EM_IM | Age of Majority | Email + Mobile Banner, activated <60 days, no purchase |
| VUI | PVUIAG02 | IM | Age of Majority | IM only (dropped from email dedup) |
| VUI | PVUIAG03 | EM | Age of Majority | Email only eligible |
| VUI | PVUIAG04 | EM_IM | Student 14+ | Email + Mobile Banner |
| VUI | PVUIAG05 | EM | Student 14+ | Email only eligible (not in data) |
| VUI | PVUIAG06 | IM | Student 14+ | Mobile banner only (not in data) |
| VUT | PVUTAG01 | EM | All | Email, one-time batch Jun-Sep 2025 |

## Results (from Lumina Run — vvd_v3_channel_significance.csv)

| MNE | RPT_GRP_CD | Channel | A_Rate | C_Rate | Lift (pp) | Rel Lift | Sig |
|-----|-----------|---------|--------|--------|-----------|----------|-----|
| VAW | PVAWAG01 | IM | 5.54% | 2.86% | +2.69 | +94.1% | 99.9% |
| VCN | PVCNAG01 | MB | 0.65% | 0.37% | +0.28 | +76.6% | 99.9% |
| VCN | PVCNAG02 | MB | 0.49% | 0.30% | +0.19 | +62.0% | 99.9% |
| VDA | PVDAAG03 | EM_IM | 2.59% | 1.94% | +0.65 | +33.7% | 99.9% |
| VDA | PVDAAG04 | IM | 0.75% | 0.60% | +0.14 | +23.8% | 99.9% |
| VDT | PVDTAG01 | EM | 73.54% | 70.51% | +3.03 | +4.3% | 95% |
| VDT | PVDTAG02 | DO | 61.15% | 64.21% | -3.06 | -4.8% | No |
| VDT | PVDTAG03 | EM | 67.45% | 62.75% | +4.70 | +7.5% | 99.9% |
| VDT | PVDTAG04 | DO | 58.84% | 59.25% | -0.41 | -0.7% | No |
| VDT | PVDTAG11 | EM | 55.13% | 49.75% | +5.38 | +10.8% | 99% |
| VDT | PVDTAG13 | EM | 49.92% | 45.05% | +4.87 | +10.8% | 99.9% |
| VUI | PVUIAG01 | EM_IM | 49.36% | 32.73% | +0.62 | -1.9% | No |
| VUI | PVUIAG02 | IM | 39.88% | 46.43% | -6.54 | -14.1% | No |
| VUI | PVUIAG03 | EM | 34.30% | 33.04% | +1.26 | +3.8% | No |
| VUI | PVUIAG04 | EM_IM | 47.36% | 44.72% | +2.64 | +5.9% | No |
| VUT | PVUTAG01 | EM | 8.19% | 8.13% | +0.07 | +0.8% | No |

## Key Insights

### Channel effectiveness varies by campaign goal:
- **EM (Email)**: Works for VDT activation (+3-5pp). Dead for VUT provisioning (+0.07pp) and VUI usage.
- **EM_IM (Email+IM)**: Best combo for VDA acquisition (+0.65pp). Ineffective for VUI usage.
- **IM (In-App)**: Star performer for VAW provisioning (+2.69pp). Marginal for VDA (+0.14pp). Negative for VUI (-6.5pp).
- **MB (Mobile Banking)**: Low absolute lift but significant for VCN acquisition. Passive/free channel.
- **DO (Digital Online)**: Negative/flat for VDT activation. Not effective.

### VDT channel story:
- **EM works, DO doesn't.** AG01/03 (EM) = significant lift. AG02/04 (DO) = negative/flat.
- **Reminders work.** AG11/13 (reminder) show higher relative lift (+10.8%) than initial emails (+4-7%).
- **Non-students activate more from email** than students (4.70pp vs 3.03pp).

### VDA channel story:
- **EM_IM >> IM alone.** Multi-channel (AG03) delivers 4.7x the lift of IM-only (AG04).

### VUI — nothing works:
- All 4 groups non-significant. No channel moves the needle on usage.

### Segments with no data:
- VUI AG05 (Student EM) and AG06 (Student IM) — designed but not in results. Likely zero volume or merged.
