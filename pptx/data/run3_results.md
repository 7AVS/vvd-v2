# VVD v2 — Run 3 Results

**Date:** 2026-03-07
**Pipeline:** Cells 1-4 + 4b (email engagement)
**Config:** VCN, VDA, VDT, VUI, VUT, VAW | Years 2025-2026

---

## 1. Run Summary

| Metric | Value |
|--------|-------|
| M1 rows (tactic_df) | 20,030,733 |
| Unique clients | 3,563,102 |
| Date range | 2025-01-02 to 2026-03-06 |
| Result rows (after email join) | 20,782,247 |
| Columns | 28 |
| HDFS path | `/user/427966379/vvd_v2_result.parquet` |

**M3 Success Counts:**

| Metric | Rows |
|--------|------|
| card_acquisition | 1,293,408 |
| card_activation | 1,111,572 |
| card_usage | 126,651,803 |
| wallet_provisioning | 3,850,762 |

**Email Engagement:** 2,211,370 rows from 10 RPT_GRP_CDs (PVDAAG03, PVDTAG01/03/11/13, PVUIAG01/02/04, PVUTAG01).

**M6 Note:** Result DF was 20,030,733 rows before email join, expanded to 20,782,247 after (email can create additional rows for multi-channel deployments).

---

## 2. Campaign Performance

### Summary (All Cohorts)

| Campaign | Action Rate | Control Rate | Abs Lift | Rel Lift | Sig | Incr. Clients |
|----------|------------|-------------|----------|----------|-----|---------------|
| **VAW** | 5.87% | 3.25% | **+2.62pp** | **+80.7%** | 99.9% | 10,816 |
| **VCN** | 0.47% | 0.29% | +0.18pp | +62.9% | 99.9% | 3,591 |
| **VDA** | 1.36% | 1.05% | +0.31pp | +29.9% | 99.9% | 2,793 |
| **VDT** | 61.05% | 56.40% | +4.65pp | +8.2% | 99.9% | 6,138 |
| VUI | 33.67% | 32.89% | +0.78pp | +2.4% | No | 1,232 |
| VUT | 13.86% | 13.84% | +0.02pp | +0.1% | No | 155 |

### Volume Distribution

| Campaign | Deployments | % Total | Clients | Avg Contacts |
|----------|------------|---------|---------|--------------|
| VCN | 15,812,086 | 80.5% | 1,999,540 | 7.91 |
| VDA | 1,510,982 | 7.7% | 894,779 | 1.69 |
| VUT | 784,849 | 4.0% | 784,849 | 1.00 |
| VDT | 723,566 | 3.7% | 132,015 | 5.48 |
| VAW | 412,568 | 2.1% | 412,568 | 1.00 |
| VUI | 392,717 | 2.0% | 158,895 | 2.47 |

### Success Flag Distribution

| MNE | TST_GRP_CD | Total | Successes | Success Rate |
|-----|-----------|-------|-----------|-------------|
| VAW | TG4 | 412,568 | 24,213 | 5.87% |
| VAW | TG7 | 103,137 | 3,349 | 3.25% |
| VCN | TG4 | 15,812,086 | 73,530 | 0.47% |
| VCN | TG7 | 889,895 | 2,540 | 0.29% |
| VDA | TG4 | 1,510,982 | 20,510 | 1.36% |
| VDA | TG7 | 79,506 | 831 | 1.05% |
| VDT | TG4 | 205,862 | 125,247 | 60.84% |
| VDT | TG7 | 23,298 | 13,141 | 56.40% |
| VUI | TG4 | 158,907 | 53,366 | 33.58% |
| VUI | TG7 | 8,336 | 2,742 | 32.89% |
| VUT | TG4 | 784,849 | 108,762 | 13.86% |
| VUT | TG7 | 41,307 | 5,716 | 13.84% |

### Cohort-Level Highlights

- **VAW:** All cohorts significant (95%-99.9%), lift ranges +0.70pp to +3.09pp.
- **VCN:** All cohorts significant (99.9%), lift ranges +0.05pp to +0.24pp. SRM warning persists.
- **VDA:** All cohorts significant (99.9%), lift +0.28pp to +1.13pp. Only 2 cohorts (2025-07, 2025-11). SRM warning.
- **VDT:** Mixed significance. Ranges from +1.93pp (not sig) to +9.94pp (99.9%). Several cohorts not significant (2025-02, 2025-08).
- **VUI:** NO cohort is significant. Ranges from -0.99pp to +3.64pp. All p-values > 0.05.
- **VUT:** Only 1 cohort (2026-06), +0.02pp, p=0.9094, not significant.

---

## 3. SRM Results

| Campaign | Action | Control | Ratio | Expected | Chi-sq | p-value | Status |
|----------|--------|---------|-------|----------|--------|---------|--------|
| VAW | 412,568 | 103,137 | 80.0/20.0 | 80/20 | 0.00 | 0.9889 | PASS |
| VCN | 1,999,540 | 111,408 | 94.7/5.3 | 95/5 | 342.54 | 0.0000 | **SRM** |
| VDA | 894,779 | 77,817 | 92.0/8.0 | 95/5 | 18,439.91 | 0.0000 | **SRM** |
| VDT | 132,015 | 14,703 | 90.0/10.0 | 90/10 | 0.07 | 0.7860 | PASS |
| VUI | 158,895 | 8,336 | 95.0/5.0 | 95/5 | 0.08 | 0.7744 | PASS |
| VUT | 784,849 | 41,307 | 95.0/5.0 | 95/5 | 0.00 | 0.9968 | PASS |

---

## 4. Deep Analysis — Post-Success Spending

### 4a. Post-Acquisition Spending (90 days) — VCN + VDA

| MNE | Group | Acquirers | With Spend | Txn Count | Total Spend | Avg Txn Amt | Avg Txn/Client | Avg Spend/Client | % With Spend |
|-----|-------|-----------|-----------|-----------|------------|------------|----------------|-----------------|-------------|
| VCN | TG4 | 73,479 | 37,908 | 727,408 | $50.8M | $69.82 | 19.19 | $1,339.71 | 51.59% |
| VCN | TG7 | 2,535 | 1,517 | 31,677 | $2.21M | $69.88 | 20.88 | $1,459.14 | 59.84% |
| VDA | TG4 | 20,510 | 10,943 | 184,804 | $14.0M | $75.83 | 16.89 | $1,288.68 | 53.35% |
| VDA | TG7 | 831 | 469 | 6,579 | $498K | $75.73 | 14.03 | $1,062.32 | 56.44% |

### 4b. Post-Activation Spending (90 days) — VDT

| MNE | Group | Activators | With Spend | Txn Count | Total Spend | Avg Txn Amt | Avg Txn/Client | Avg Spend/Client | % With Spend |
|-----|-------|-----------|-----------|-----------|------------|------------|----------------|-----------------|-------------|
| VDT | TG4 | 91,842 | 78,099 | 2,135,520 | $116M | $54.44 | 27.34 | $1,488.54 | 85.03% |
| VDT | TG7 | 9,552 | 8,344 | 229,718 | $12.6M | $54.82 | 27.54 | $1,509.33 | 87.35% |

### 4c. Post-Provisioning Spending (Pre vs Post) — VUT + VAW

| MNE | Group | Period | With Spend | Txn Count | Total Spend | Avg Txn Amt | Avg Txn/Client | Avg Spend/Client |
|-----|-------|--------|-----------|-----------|------------|------------|----------------|-----------------|
| VAW | TG4 | PRE | 24,002 | 843,236 | $42.2M | $50.13 | 35.13 | $1,761.14 |
| VAW | TG4 | POST | 22,943 | 1,051,436 | $50.7M | $48.14 | 45.83 | $2,206.28 |
| VAW | TG7 | PRE | 3,314 | 119,593 | $3.91M | $49.43 | 36.09 | $1,783.91 |
| VAW | TG7 | POST | 3,211 | 165,297 | $7.43M | $44.93 | 51.48 | $2,313.04 |
| VUT | TG4 | PRE | 106,499 | 5,989,054 | $249M | $41.60 | 56.24 | $2,339.28 |
| VUT | TG4 | POST | 103,239 | 6,201,501 | $254M | $40.91 | 60.07 | $2,457.38 |
| VUT | TG7 | PRE | 5,610 | 320,847 | $13.3M | $41.37 | 57.19 | $2,366.05 |
| VUT | TG7 | POST | 5,434 | 320,721 | $13.4M | $41.80 | 59.02 | $2,467.37 |

### Pre vs Post Provisioning Lift

| MNE | Group | PRE Avg Spend | POST Avg Spend | Spend Lift | PRE Avg Txn | POST Avg Txn | Txn Lift |
|-----|-------|--------------|----------------|-----------|------------|-------------|---------|
| VAW | TG4 | $1,761 | $2,206 | +25.3% | 35.13 | 45.83 | +30.5% |
| VAW | TG7 | $1,784 | $2,313 | +29.7% | 36.09 | 51.48 | +42.7% |
| VUT | TG4 | $2,339 | $2,457 | +5.1% | 56.24 | 60.07 | +6.8% |
| VUT | TG7 | $2,366 | $2,467 | +4.3% | 57.19 | 59.02 | +3.2% |

**Note:** These spend numbers span up to 2 years of history and are likely inflated by the long observation window. A time-series approach (monthly cohorts) would be more informative than collapsing into two periods.

---

## 5. Overcontacting Analysis

### Contact Frequency (Action Group)

| Bucket | Clients | % Total | Avg Contacts | Success Rate |
|--------|---------|---------|-------------|-------------|
| 1 | 915,317 | 26.9% | 1.0 | 12.53% |
| 2 | 493,194 | 14.5% | 2.0 | 7.02% |
| 3 | 258,869 | 7.6% | 3.0 | 11.07% |
| 4-5 | 349,951 | 10.3% | 4.5 | 21.15% |
| 6-10 | 616,218 | 18.1% | 7.9 | 8.69% |
| 11+ | 770,107 | 22.6% | 13.7 | 2.41% |

### Contacts Per Client by Campaign

| Campaign | Clients | Avg | Median | P90 | Max |
|----------|---------|-----|--------|-----|-----|
| VCN | 1,999,540 | 7.9 | 7 | 14 | 14 |
| VDT | 132,015 | 5.5 | 4 | 8 | 44 |
| VUI | 158,895 | 2.5 | 3 | 3 | 6 |
| VDA | 894,779 | 1.7 | 2 | 2 | 2 |
| VUT | 784,849 | 1.0 | 1 | 1 | 1 |
| VAW | 412,568 | 1.0 | 1 | 1 | 1 |

### Diminishing Returns by Contact Sequence

- **VAW:** Single contact only. 5.87%.
- **VCN:** Clear declining pattern. Contact 1=1.25%, 2=0.65%, 3=0.50%, ..., 10=0.23%.
- **VDA:** Contact 1=1.53%, 2=1.10%.
- **VDT:** Contact 1=65.55%, 2=66.61%, 3=66.68%, 4=66.53%, then drops at 5+ to ~51%, spikes again at 9-10 (~63%).
- **VUI:** Flat. Contact 1=33.58%, 2=33.73%, 3=33.73%. Contacts 4-6 have negligible volume (4-12 deployments).
- **VUT:** Single contact only. 13.86%.

### Gap Analysis

| Gap Bucket | Transitions | % Total | Clients | Avg Gap |
|-----------|------------|---------|---------|---------|
| Same day | 752,131 | 4.6% | 221,425 | 0.0d |
| 1-7 days | 687,282 | 4.2% | 495,656 | 6.5d |
| 8-15 days | 68,530 | 0.4% | 63,367 | 8.6d |
| 16-25 days | 642,619 | 4.0% | 443,959 | 23.0d |
| 26-30 days | 6,848,085 | 42.2% | 1,797,259 | 28.7d |
| 31-60 days | 6,126,275 | 37.7% | 1,795,762 | 33.2d |
| 61-90 days | 251,158 | 1.5% | 234,255 | 69.9d |
| 90+ days | 857,032 | 5.3% | 780,858 | 144.5d |

**Overlapping deployments** (gap < 25 days): 2,150,562 transitions, 727,345 clients.

### Campaign Transition Matrix (Top 20)

| From | To | Transitions | Clients | Avg Gap |
|------|-----|------------|---------|---------|
| VCN | VCN | 13,105,435 | 1,859,570 | 33.9d |
| VCN | VDA | 848,120 | 565,663 | 32.2d |
| VDA | VCN | 786,990 | 521,704 | 28.2d |
| VDT | VDT | 589,087 | 111,232 | 2.3d |
| VDA | VDA | 299,588 | 299,588 | 140.0d |
| VUI | VUI | 233,813 | 116,894 | 0.0d |
| VUT | VAW | 150,951 | 150,951 | 63.9d |
| VAW | VUT | 40,685 | 40,685 | 7.5d |
| VCN | VUI | 38,150 | 38,150 | 39.3d |
| VUI | VAW | 20,484 | 20,484 | 87.7d |
| VDT | VUT | 16,532 | 16,532 | 88.4d |
| VUT | VDT | 15,428 | 15,428 | 120.8d |
| VUI | VUT | 10,686 | 10,686 | 77.4d |
| VCN | VAW | 10,384 | 10,384 | 70.0d |
| VUT | VCN | 9,878 | 9,878 | 140.4d |
| VCN | VDT | 8,962 | 8,925 | 38.4d |
| VCN | VUT | 8,913 | 8,913 | 79.5d |
| VDT | VCN | 7,563 | 7,506 | 74.2d |
| VDA | VUI | 5,787 | 5,787 | 48.7d |
| VDT | VAW | 4,616 | 4,616 | 106.6d |

### Self-Loop Analysis

| Loop | Transitions | Clients | Avg Gap |
|------|------------|---------|---------|
| VCN -> VCN | 13,105,435 | 1,859,570 | 33.9d |
| VDT -> VDT | 589,087 | 111,232 | 2.3d |
| VDA -> VDA | 299,588 | 299,588 | 140.0d |
| VUI -> VUI | 233,813 | 116,894 | 0.0d |

---

## 6. Channel Distribution

| MNE | Group | Channel | Deployments | Clients |
|-----|-------|---------|------------|---------|
| VAW | TG4 | IM | 412,568 | 412,568 |
| VAW | TG7 | XX | 103,137 | 103,137 |
| VCN | TG4 | MB | 15,812,086 | 1,999,540 |
| VCN | TG7 | XX | 889,895 | 111,408 |
| VDA | TG4 | IM | 1,010,982 | 595,599 |
| VDA | TG4 | EM_IM | 500,000 | 317,260 |
| VDA | TG7 | XX | 79,506 | 77,817 |
| VDT | TG4 | EM | 703,005 | 112,914 |
| VDT | TG4 | DO | 20,561 | 19,564 |
| VDT | TG7 | XX | 23,298 | 14,703 |
| VUI | TG4 | EM_IM | 314,622 | 111,041 |
| VUI | TG4 | EM | 45,565 | 15,325 |
| VUI | TG4 | IM | 32,530 | 32,530 |
| VUI | TG7 | XX | 8,336 | 8,336 |
| VUT | TG4 | EM | 784,849 | 784,849 |
| VUT | TG7 | XX | 41,307 | 41,307 |

**Channel Balance:** All control groups use XX (holdout). Action channels per campaign:
- **VAW:** IM (100%)
- **VCN:** MB (100%)
- **VDA:** IM (66.9%), EM_IM (33.1%)
- **VDT:** EM (97.2%), DO (2.8%)
- **VUI:** EM_IM (80.1%), EM (11.6%), IM (8.3%)
- **VUT:** EM (100%)

---

## 7. Key Findings

### Campaign Effectiveness

1. **VAW is the standout performer.** +2.62pp absolute lift (80.7% relative), significant at 99.9% across all cohorts, generating 10,816 incremental clients. This from a single-contact, IM-only campaign with only 2.1% of total deployments. Highest ROI per deployment in the portfolio.

2. **VCN delivers volume, not depth.** Statistically significant (+0.18pp) but on a 0.47% base rate. The 80.5% share of total deployments (15.8M) yields only 3,591 incremental clients. Diminishing returns are severe: contact 1 converts at 1.25%, contact 10 at 0.23%. VCN also dominates the self-loop matrix (13.1M VCN->VCN transitions).

3. **VDA has a significant lift (+0.31pp) but now has SRM.** The observed 92/8 action/control split vs expected 95/5 produces chi-sq = 18,439.91 (p=0.0000). This is new — not previously known. The lift numbers for VDA should be interpreted with caution until the randomization issue is understood.

4. **VDT has the largest absolute lift (+4.65pp) but mixed cohort reliability.** Several cohorts (2025-02, 2025-08) are not significant. The 2.3-day VDT->VDT self-loop gap suggests rapid retargeting that may inflate deployment counts without proportional benefit.

5. **VUI shows no effect.** +0.78pp lift is not significant overall, and NO individual cohort reaches significance. Ranges from -0.99pp to +3.64pp across cohorts. Recommend pause or redesign.

6. **VUT is effectively zero lift.** +0.02pp, p=0.9094. Single cohort, single contact, 784K clients contacted with no measurable impact. Recommend discontinuation (consistent with prior recommendation based on negative ROI).

### Spending Analysis — Caution Required

7. **Control acquirers spend MORE per client than Action in VCN.** TG7 avg $1,459/client vs TG4 $1,340/client. TG7 also has higher % with spend (59.8% vs 51.6%) and more transactions per client (20.88 vs 19.19). This suggests the campaign may be acquiring lower-quality clients who spend less, or that the SRM is introducing selection bias.

8. **Post-provisioning lift is higher for Control than Action in VAW.** VAW TG7 shows +29.7% spend lift (pre vs post) and +42.7% txn lift, compared to TG4's +25.3% spend lift and +30.5% txn lift. The campaign drives more provisioning, but those incremental provisioners may be less engaged than organic ones.

9. **VUT pre/post spending lift is negligible for both groups.** TG4 +5.1%, TG7 +4.3%. Combined with zero success rate lift, confirms no campaign effect.

10. **Spending numbers are inflated by long observation windows** (up to 2 years of data collapsed into PRE/POST). A time-series approach with monthly cohorts would provide cleaner attribution. The current summary overstates per-client spend because it accumulates transactions over the full available history rather than a fixed post-event window.

### Operational Issues

11. **VCN SRM persists** (chi-sq 342.54, p=0.0000). Known issue from Run 1. Observed 94.7/5.3 vs expected 95/5.

12. **Overcontacting is concentrated.** 22.6% of action clients receive 11+ contacts with only 2.41% success rate, while single-contact clients achieve 12.53%. The 4-5 contact bucket shows the highest success rate (21.15%), suggesting a possible sweet spot — but this is confounded by campaign mix (VDT clients cluster in higher contact counts).

13. **2.15M overlapping transitions** (gap < 25 days) across 727K clients. Same-day deployments (752K transitions, 221K clients) are primarily VUI->VUI (0.0d avg gap) and VDT->VDT (2.3d avg gap).
