# VVD v2 — Lumina Results (Source of Truth)
# Last updated: 2026-03-08
# Extracted from phone screenshots of Lumina/Jupyter output

---

## Pipeline Runs Overview

| Run | Date Range | Years | Rows | Clients | Notes |
|-----|-----------|-------|------|---------|-------|
| Run 2 | 2024-01 to 2026-03 | 2024-2026 | 38,766,703 | 4,262,078 | Wider range. VUI/VUT had SRM. VCN negative in early 2024. |
| Run 3 | 2025-01 to 2026-03 | 2025-2026 | 20,782,247 | 3,563,102 | Narrowed to 2025+. VUI/VUT SRM resolved. Primary run. |
| Run 3b | 2025-01 to 2026-02 | 2025-2026 | ~19,525,556 | ~3,400,000 | March excluded (DATA_END_DATE). Deep analysis re-run. |

**Current source of truth: Run 3** for pipeline metrics, **Run 3b** for deep analysis and overall success rates.

---

## 1. Campaign Performance Summary

### Run 3 (primary — includes March)

| Campaign | Action Rate | Control Rate | Abs Lift | Rel Lift | Sig | Incr. Clients |
|----------|------------|-------------|----------|----------|-----|---------------|
| **VAW** | 5.87% | 3.25% | **+2.62pp** | **+80.7%** | 99.9% | 10,816 |
| **VCN** | 0.47% | 0.29% | +0.18pp | +62.9% | 99.9% | 3,591 |
| **VDA** | 1.36% | 1.05% | +0.31pp | +29.9% | 99.9% | 2,793 |
| **VDT** | 61.05% | 56.40% | +4.65pp | +8.2% | 99.9% | 6,138 |
| VUI | 33.67% | 32.89% | +0.78pp | +2.4% | No | 1,232 |
| VUT | 13.86% | 13.84% | +0.02pp | +0.1% | No | 155 |

### Run 3b (March excluded — deep analysis overall rates)

| Campaign | Action Rate | Control Rate |
|----------|------------|-------------|
| VAW | 5.98% | 3.31% |
| VCN | 0.49% | 0.30% |
| VDA | 1.36% | 1.05% |
| VDT | ~61.6% | 57.14% |
| VUI | 35.00% | 33.94% |
| VUT | 13.86% | 13.84% |

---

## 2. Volume Distribution (Run 3)

| Campaign | Deployments | % Total | Clients | Avg Contacts |
|----------|------------|---------|---------|-------------|
| VCN | 15,812,086 | 80.5% | 1,999,540 | 7.91 |
| VDA | 1,510,982 | 7.7% | 894,779 | 1.69 |
| VUT | 784,849 | 4.0% | 784,849 | 1.00 |
| VDT | 723,566 | 3.7% | 132,015 | 5.48 |
| VAW | 412,568 | 2.1% | 412,568 | 1.00 |
| VUI | 392,717 | 2.0% | 158,895 | 2.47 |

---

## 3. SRM Check (Run 3)

| Campaign | Action | Control | Ratio | Expected | Chi-sq | p-value | Status |
|----------|--------|---------|-------|----------|--------|---------|--------|
| VAW | 412,568 | 103,137 | 80.0/20.0 | 80/20 | 0.00 | 0.9889 | PASS |
| VCN | 1,999,540 | 111,408 | 94.7/5.3 | 95/5 | 342.54 | 0.0000 | **SRM** |
| VDA | 894,779 | 77,817 | 92.0/8.0 | 95/5 | 18,439.91 | 0.0000 | **SRM** |
| VDT | 132,015 | 14,703 | 90.0/10.0 | 90/10 | 0.07 | 0.7860 | PASS |
| VUI | 158,895 | 8,336 | 95.0/5.0 | 95/5 | 0.08 | 0.7744 | PASS |
| VUT | 784,849 | 41,307 | 95.0/5.0 | 95/5 | 0.00 | 0.9968 | PASS |

---

## 4. Cohort-Level Lift & Significance (Run 3)

### VAW
| Cohort | Action Rate | Control Rate | Abs Lift | p-value | Sig |
|--------|------------|-------------|----------|---------|-----|
| 2025-04 | 3.56% | 0.48% | +3.09pp | 0.0180 | 95% |
| 2025-06 | 6.21% | 3.26% | +2.96pp | 0.0000 | 99.9% |
| 2025-07 | 6.04% | 3.10% | +2.95pp | 0.0000 | 99.9% |
| 2025-08 | 5.78% | 2.81% | +2.96pp | 0.0000 | 99.9% |
| 2025-09 | 5.72% | 3.91% | +1.81pp | 0.0000 | 99.9% |
| 2025-10 | 7.19% | 5.19% | +2.00pp | 0.0000 | 99.9% |
| 2025-11 | 6.42% | 4.63% | +1.79pp | 0.0000 | 99.9% |
| 2025-12 | 6.40% | 4.43% | +1.97pp | 0.0000 | 99.9% |
| 2026-01 | 6.19% | 5.03% | +1.16pp | 0.0062 | 99% |
| 2026-02 | 6.14% | 4.08% | +2.06pp | 0.0000 | 99.9% |
| 2026-03 | 1.77% | 1.07% | +0.70pp | 0.0100 | 99% |

### VCN
| Cohort | Action Rate | Control Rate | Abs Lift | p-value | Sig |
|--------|------------|-------------|----------|---------|-----|
| 2025-02 | 0.50% | 0.29% | +0.21pp | 0.0000 | 99.9% |
| 2025-03 | 0.48% | 0.25% | +0.23pp | 0.0000 | 99.9% |
| 2025-04 | 0.56% | 0.32% | +0.24pp | 0.0000 | 99.9% |
| 2025-05 | 0.50% | 0.32% | +0.19pp | 0.0000 | 99.9% |
| 2025-06 | 0.47% | 0.23% | +0.24pp | 0.0000 | 99.9% |
| 2025-07 | 0.54% | 0.34% | +0.20pp | 0.0000 | 99.9% |
| 2025-08 | 0.46% | 0.30% | +0.16pp | 0.0000 | 99.9% |
| 2025-09 | 0.42% | 0.30% | +0.12pp | 0.0000 | 99.9% |
| 2025-11 | 0.57% | 0.36% | +0.21pp | 0.0000 | 99.9% |
| 2025-12 | 0.49% | 0.33% | +0.17pp | 0.0000 | 99.9% |
| 2026-01 | 0.55% | 0.33% | +0.22pp | 0.0000 | 99.9% |
| 2026-02 | 0.46% | 0.29% | +0.17pp | 0.0000 | 99.9% |
| 2026-03 | 0.10% | 0.05% | +0.05pp | 0.0001 | 99.9% |

### VDA
| Cohort | Action Rate | Control Rate | Abs Lift | p-value | Sig |
|--------|------------|-------------|----------|---------|-----|
| 2025-07 | 1.25% | 0.97% | +0.28pp | 0.0000 | 99.9% |
| 2025-11 | 1.47% | 1.13% | +0.35pp | 0.0000 | 99.9% |

### VDT
| Cohort | Action Rate | Control Rate | Abs Lift | p-value | Sig |
|--------|------------|-------------|----------|---------|-----|
| 2025-01 | 62.37% | 56.43% | +5.95pp | 0.0000 | 99.9% |
| 2025-02 | 61.48% | 59.56% | +1.93pp | 0.1026 | No |
| 2025-03 | 62.66% | 56.42% | +6.24pp | 0.0000 | 99.9% |
| 2025-04 | 60.37% | 56.41% | +3.96pp | 0.0011 | 99% |
| 2025-05 | 61.11% | 54.34% | +6.77pp | 0.0000 | 99.9% |
| 2025-06 | 60.84% | 50.90% | +9.94pp | 0.0000 | 99.9% |
| 2025-07 | 62.43% | 59.67% | +2.76pp | 0.0170 | 95% |
| 2025-08 | 62.75% | 60.60% | +2.14pp | 0.0726 | No |
| 2025-09 | 60.77% | 58.51% | +2.26pp | 0.0463 | 95% |
| 2025-10 | 56.25% | 50.29% | +5.96pp | 0.0000 | 99.9% |
| 2025-11 | 64.43% | 61.74% | +2.69pp | 0.0461 | 95% |
| 2025-12 | 62.48% | 58.97% | +3.51pp | 0.0064 | 99% |
| 2026-01 | 67.19% | 62.62% | +4.57pp | 0.0004 | 99.9% |
| 2026-02 | 58.46% | 53.61% | +4.85pp | 0.0002 | 99.9% |
| 2026-03 | 17.60% | 8.52% | +9.08pp | 0.0000 | 99.9% |

### VUI (NO cohort is significant)
| Cohort | Action Rate | Control Rate | Abs Lift | p-value | Sig |
|--------|------------|-------------|----------|---------|-----|
| 2025-02 | 33.80% | 33.69% | +0.11pp | 0.9548 | No |
| 2025-03 | 35.57% | 34.48% | +1.09pp | 0.5959 | No |
| 2025-04 | 33.82% | 30.18% | +3.64pp | 0.0693 | No |
| 2025-05 | 32.99% | 31.97% | +1.03pp | 0.6021 | No |
| 2025-06 | 33.45% | 32.24% | +1.21pp | 0.5425 | No |
| 2025-07 | 33.43% | 33.80% | -0.37pp | 0.8516 | No |
| 2025-08 | 33.99% | 32.01% | +1.98pp | 0.3084 | No |
| 2025-09 | 33.14% | 30.49% | +2.66pp | 0.2053 | No |
| 2025-10 | 37.36% | 37.24% | +0.12pp | 0.9537 | No |
| 2025-11 | 35.75% | 35.56% | +0.19pp | 0.9145 | No |
| 2025-12 | 38.41% | 39.40% | -0.99pp | 0.6047 | No |
| 2026-01 | 36.29% | 36.04% | +0.26pp | 0.8937 | No |
| 2026-02 | 36.08% | 32.47% | +3.61pp | 0.0731 | No |
| 2026-03 | 16.45% | 17.39% | -0.95pp | 0.5611 | No |

### VUT (single cohort, not significant)
| Cohort | Action Rate | Control Rate | Abs Lift | p-value | Sig |
|--------|------------|-------------|----------|---------|-----|
| 2025-06 | 13.86% | 13.84% | +0.02pp | 0.9094 | No |

---

## 5. Channel Distribution (Run 3)

| MNE | Group | Channel | Deployments | Clients |
|-----|-------|---------|------------|---------|
| VAW | TG4 | IM | 412,568 | 412,568 |
| VCN | TG4 | MB | 15,812,086 | 1,999,540 |
| VDA | TG4 | IM | 1,010,982 | 595,599 |
| VDA | TG4 | EM_IM | 500,000 | 317,260 |
| VDT | TG4 | EM | 703,005 | 112,914 |
| VDT | TG4 | DO | 20,561 | 19,564 |
| VUI | TG4 | EM_IM | 314,622 | 111,041 |
| VUI | TG4 | EM | 45,565 | 15,325 |
| VUI | TG4 | IM | 32,530 | 32,530 |
| VUT | TG4 | EM | 784,849 | 784,849 |

Channel summary: VAW=IM, VCN=MB, VDA=IM(67%)+EM_IM(33%), VDT=EM(97%)+DO(3%), VUI=EM_IM(80%)+EM(12%)+IM(8%), VUT=EM(100%)

### Success Rate by Channel (Action Group)

| MNE | Channel | Rate | vs Control |
|-----|---------|------|-----------|
| VAW | IM | 5.87% | +2.62pp |
| VCN | MB | 0.47% | +0.18pp |
| VDA | EM_IM | 2.59% | +1.55pp |
| VDA | IM | 0.75% | **-0.30pp** (below control!) |
| VDT | EM | 61.16% | +4.75pp |
| VDT | DO | 57.53% | +1.13pp |
| VUI | EM | 45.04% | +12.15pp |
| VUI | EM_IM | 31.99% | -0.90pp |
| VUI | IM | 33.97% | +1.08pp |
| VUT | EM | 13.86% | +0.02pp |

---

## 6. RPT_GRP_CD Mapping & Lift

### Tactic Cell Reference
| MNE | RPT_GRP_CD | TACTIC_CELL_CD |
|-----|-----------|---------------|
| VAW | PVAWAG01 | IM |
| VCN | PVCNAG01 | MB |
| VCN | PVCNAG02 | MB |
| VDA | PVDAAG03 | EM_IM |
| VDA | PVDAAG04 | IM |
| VDT | PVDTAG01 | EM |
| VDT | PVDTAG02 | DO |
| VDT | PVDTAG03 | EM |
| VDT | PVDTAG04 | DO |
| VDT | PVDTAG11 | EM |
| VDT | PVDTAG13 | EM |
| VUI | PVUIAG01 | EM_IM |
| VUI | PVUIAG02 | EM |
| VUI | PVUIAG03 | IM |
| VUI | PVUIAG04 | EM |
| VUT | PVUTAG01 | EM |
| VUT | PVUTAG02 | EM |

### Lift by RPT_GRP_CD

| MNE | RPT_GRP_CD | Action Rate | Control Rate | Abs Lift | Rel Lift |
|-----|-----------|------------|-------------|----------|----------|
| VAW | PVAWAG01 | 5.87% | 3.25% | +2.62pp | +80.7% |
| VCN | PVCNAG01 | 0.61% | 0.34% | +0.27pp | +79.9% |
| VCN | PVCNAG02 | 0.46% | 0.28% | +0.18pp | +62.3% |
| VDA | PVDAAG03 | 2.59% | 1.94% | +0.65pp | +33.7% |
| VDA | PVDAAG04 | 0.75% | 0.60% | +0.14pp | +23.8% |
| VDT | PVDTAG01 | 72.92% | 70.10% | +2.81pp | +4.0% |
| VDT | PVDTAG02 | 58.79% | 60.84% | -2.05pp | -3.4% |
| VDT | PVDTAG03 | 66.82% | 62.02% | +4.80pp | +7.7% |
| VDT | PVDTAG04 | 57.33% | 57.45% | -0.12pp | -0.2% |
| VDT | PVDTAG11 | 54.72% | 49.33% | +5.38pp | +10.9% |
| VDT | PVDTAG13 | 49.53% | 44.59% | +4.93pp | +11.1% |
| VUI | PVUIAG01 | 31.99% | 31.59% | +0.41pp | +1.3% |
| VUI | PVUIAG02 | 37.41% | 40.62% | -3.22pp | -7.9% |
| VUI | PVUIAG03 | 33.97% | 32.65% | +1.32pp | +4.0% |
| VUI | PVUIAG04 | 45.76% | 43.27% | +2.50pp | +5.8% |
| VUT | PVUTAG01 | 13.86% | 13.84% | +0.02pp | +0.1% |

---

## 7. Deep Analysis — Post-Success Spending (Run 3)

### 7a. Post-Acquisition Spending (90 days)

| MNE | Group | Acquirers | With Spend | Txn Count | Total Spend | Avg Spend/Client | % With Spend |
|-----|-------|-----------|-----------|-----------|------------|-----------------|-------------|
| VCN | TG4 | 73,479 | 37,908 | 727,408 | $50.8M | $1,339.71 | 51.59% |
| VCN | TG7 | 2,535 | 1,517 | 31,677 | $2.21M | $1,459.14 | 59.84% |
| VDA | TG4 | 20,510 | 10,943 | 184,804 | $14.0M | $1,280.68 | 53.35% |
| VDA | TG7 | 831 | 469 | 6,579 | $498K | $1,062.32 | 56.44% |

### 7b. Post-Activation Spending (90 days)

| MNE | Group | Activators | With Spend | Txn Count | Total Spend | Avg Spend/Client | % With Spend |
|-----|-------|-----------|-----------|-----------|------------|-----------------|-------------|
| VDT | TG4 | 91,842 | 78,099 | 1,213,520 | $116M | $1,488.54 | 85.78% |
| VDT | TG7 | 9,552 | 8,344 | 229,718 | $12.6M | $1,509.33 | 87.35% |

### 7c. Post-Provisioning Pre vs Post (90 days)

| MNE | Group | Period | Clients | Txn Count | Total Spend | Avg Spend/Client |
|-----|-------|--------|---------|-----------|------------|-----------------|
| VAW | TG4 | PRE | 24,002 | 843,236 | $42.3M | $1,761.14 |
| VAW | TG4 | POST | 22,943 | 1,051,436 | $50.6M | $2,206.28 |
| VAW | TG7 | PRE | 3,314 | 119,593 | $5.91M | $1,783.91 |
| VAW | TG7 | POST | 3,211 | 165,297 | $7.43M | $2,313.04 |
| VUT | TG4 | PRE | 106,499 | 5,989,054 | $249M | $2,339.28 |
| VUT | TG4 | POST | 103,239 | 6,201,501 | $254M | $2,457.38 |
| VUT | TG7 | PRE | 5,610 | 320,847 | $13.3M | $2,366.05 |
| VUT | TG7 | POST | 5,434 | 320,721 | $13.4M | $2,467.37 |

### Spend Lift Pre→Post

| MNE | Group | PRE Spend | POST Spend | Spend Lift | Txn Lift |
|-----|-------|-----------|-----------|-----------|---------|
| VAW | TG4 | $1,761 | $2,206 | +25.3% | +30.5% |
| VAW | TG7 | $1,784 | $2,313 | +29.7% | +42.7% |
| VUT | TG4 | $2,339 | $2,457 | +5.1% | +6.8% |
| VUT | TG7 | $2,366 | $2,467 | +4.3% | +3.2% |

---

## 8. Difference-in-Differences (Run 3b — March excluded)

### Overall DiD

| MNE | Spend DiD ($/client) | Txn DiD (txn/client) |
|-----|---------------------|---------------------|
| VUT | +$58.69 | +2.79 |
| VAW | +$69.58 | +1.03 |

### VAW DiD by Cohort (highly variable)

| Cohort | Spend DiD |
|--------|----------|
| 2025-04 | -$112.59 |
| 2025-05 | +$13.48 |
| 2025-07 | -$19.30 |
| 2025-08 | +$132.86 |
| 2025-09 | -$27.42 |
| 2025-10 | -$53.74 |
| 2025-11 | +$35.82 |
| 2025-12 | -$100.90 |
| 2026-01 | +$71.49 |
| 2026-02 | -$42.96 |

---

## 9. Contact Frequency & Diminishing Returns (Run 3)

### Contact Frequency Distribution

| Bucket | Clients | % Total | Avg Contacts | Success Rate |
|--------|---------|---------|-------------|-------------|
| 1 | 915,317 | 26.9% | 1.0 | 12.53% |
| 2 | 493,194 | 14.5% | 2.0 | 7.02% |
| 3 | 258,869 | 7.6% | 3.0 | 11.07% |
| 4-5 | 349,951 | 10.3% | 4.5 | 21.15% |
| 6-10 | 616,218 | 18.1% | 7.9 | 8.69% |
| 11+ | 770,107 | 22.6% | 13.7 | 2.41% |

### Diminishing Returns by Contact Sequence

**VCN** (clear decay):
1=1.25%, 2=0.65%, 3=0.50%, 4=0.40%, 5=0.33%, 6=0.32%, 7=0.27%, 8=0.24%, 9=0.25%, 10=0.23%

**VDA**: 1=1.53%, 2=1.10%

**VDT**: 1=65.55%, 2=66.61%, 3=66.68%, 4=66.53%, 5=51.25%, 6=51.16%, 7=51.13%, 8=51.04%, 9=62.92%, 10=63.38%

**VUI**: 1=33.58%, 2=33.73%, 3=33.73% (contacts 4-6 negligible volume)

**VUT/VAW**: Single contact only

---

## 10. Orchestration Audit Results

### 10a. Cumulative Conversions (Run 3 — pre-March exclusion)

**VCN Contact-Level Conversion Distribution:**
| Contact# | Deploys | Successes | Rate% | Cum%Conv | Cum%Deploy |
|----------|---------|-----------|-------|----------|-----------|
| 1 | 1,999,540 | 24,958 | 1.25% | 33.9% | 12.7% |
| 2 | 1,864,412 | 12,059 | 0.65% | 50.3% | 24.4% |
| 3 | 1,733,376 | 8,682 | 0.50% | 62.1% | 35.4% |
| 5 | 1,371,024 | 4,539 | 0.33% | 76.4% | 53.5% |
| 7 | 1,108,668 | 3,021 | 0.27% | 85.9% | 68.4% |
| 10 | 793,572 | 1,837 | 0.23% | 94.6% | 85.4% |
| 14 | 457,808 | 154 | 0.03% | 100.0% | 100.0% |

**Cap Impact Summary:**
| Campaign | @Cap3 | @Cap5 | @Cap7 | @Cap10 |
|----------|-------|-------|-------|--------|
| VCN | 62.1% | 76.4% | 85.9% | 94.6% |
| VDT | 52.4% | 75.4% | 89.1% | 97.2% |
| VAW/VDA/VUI/VUT | 100% | 100% | 100% | 100% |

### 10b. Already-Succeeded Waste

**CORRECTED run (actual success dates, March excluded):**
| MNE | Clients Wasted | Wasted Deploys | Avg/Client | % of Success Clients |
|-----|---------------|---------------|-----------|---------------------|
| VDT | 4,850 | 26,904 | 5.5 | 5.3% |
| VCN | 3,713 | 10,118 | 2.7 | 5.1% |
| Total | 8,563 | 37,022 | — | — |

By channel: Email HIGH=26,278 (71%), MB LOW=10,118 (27.3%), DO=626 (1.7%)

**EARLIER run (TREATMT_STRT_DT as success proxy — STALE, higher numbers):**
| MNE | Clients Wasted | Wasted Deploys |
|-----|---------------|---------------|
| VDT | 31,712 | 138,088 |
| VCN | 4,282 | 11,981 |
| Total | ~36,000 | 150,093 |

### 10c. Gap Distribution (corrected run)

**VDT** (26,904 post-success deploys):
- 1-3d: 128 (33 clients) — timing artifact
- 4-7d: 331 (92)
- 8-14d: 681 (199)
- 15-30d: 1,658 (415)
- 31-60d: 3,254 (742)
- **60+: 20,852 (3,833) — 77% of VDT waste**

**VCN** (10,118 post-success deploys):
- 1-3d: 1,659 (1,659)
- 15-30d: 165 (165)
- 31-60d: 349 (346)
- **60+: 7,857 (2,038)**

### 10d. VCN Still_Success? = 0 (MAJOR FINDING)
- VCN sample clients show Still_Success? = 0 — they acquired then LOST their card
- Monthly screen correctly re-identifies them as non-holders
- Most VCN "waste" is NOT waste — platform is working correctly

### 10e. VDT Still_Success? flip (NEEDS INVESTIGATION)
- Sample client 118299940: Still_Success? flips 1→0 over time
- Activation may lapse. Card types? Status timing?
- Flagged for later session — not for this presentation

### 10f. VUT/VAW Overlap

- 191,704 clients in both VUT and VAW campaigns
- Attribution: NEITHER=176,096, BOTH_SUCCESS=10,283, VUT_ONLY=3,745, VAW_ONLY=1,580
- Gap: 0-7d=40,270, 31-60d=131,919, 60+=16,756
- Sample BOTH-success clients: VUT via EM, VAW via IM, ~7d gap
- Flagged for deeper investigation — not for this presentation

### 10g. Frequency Buckets

| Bucket | Clients | Top Campaign | % | 2nd Campaign | % |
|--------|---------|-------------|---|-------------|---|
| 01 (single) | 915K | VUT | 60% | VAW | 19% |
| 02 | 717K | VDA | 29% | VUT | 27% |
| 03 | 299K | VCN | 64% | VUI | 21% |
| 04-05 | 533K | VCN | 56% | VDA | 21% |
| 06-10 | 873K | VCN | 66% | VDA | 23% |
| 11+ | 986K | VCN | 72% | VDA | 26% |

### 10h. Transition Classification
- Self-loop: 86.5%, Lateral: 12.0%, Forward: 0.8%, Backward: 0.3%
- User verdict: "nothing burger" — drop from presentation

---

## 11. Card Type Distribution

**VVD cards 2025-2026: 1,298,926 total. NULL RATE = 0%**

| VISA_DR_CRD_BRND_CD | All VVD | % | Experiment | % |
|-----|---------|---|-----------|---|
| 03 | 605,364 | 46.60% | 413,314 | 47.53% |
| 01 | 541,132 | 41.66% | 345,627 | 39.75% |
| 04 | 152,430 | 11.74% | 110,654 | 12.72% |

**Time series:** Code 03 (Digital) first appears 2022-06. Dominates from 2025 onwards.
- Pre-2022-06: only codes 01 and 04
- 2025 onwards: ~60-70% code 03
- Grand total 2020-2026: 349K code 01 + 699K code 03 + 126K code 04 = 1.07M

---

## 12. Revenue / NIBT

**$78.21 per incremental client** (confirmed from RBC reference document)

| Campaign | Incremental | Revenue | Claimable? |
|----------|-----------|---------|-----------|
| VAW | 10,816 | $845,903 | Footnoted (provisioning ≠ new client) |
| VDT | 6,138 | $480,033 | Arguable (activation) |
| VCN | 3,591 | $280,852 | Yes (acquisition, SRM caveat) |
| VDA | 2,793 | $218,359 | Yes (acquisition, SRM caveat) |
| VUI | — | N/A | No (not significant) |
| VUT | — | N/A | No (not significant) |

Conservative (VCN+VDA): **$499K**. With VDT: **$979K**. With VAW: **$1.83M**.

---

## 13. VUI Previous Recommendations (Oct 2025 NBA M&A slide)
- N=154K, Control 28.63%, Lift 0.37%, NOT significant
- Rec 1: Expand target population (81% excluded because activation >61 days ago)
- Rec 2: Increase control from 5% to 15% (min detectable lift drops from 1.29% to 0.78%)
- Rec 3: Relax 365-day resting rule to 30 days
- STATUS: Unknown if any were implemented
