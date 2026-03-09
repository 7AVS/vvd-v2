# VVD Experiment Design Overview

Six independent A/B tests measuring the causal effect of targeted communications on Virtual Visa Debit card outcomes. Each campaign operates independently with its own control group.

---

## Acquisition

### VCN — Contextual Notification
**Monthly trigger** | MB (mobile push) | 95/5 split | 30-day window

- **Segments**: Age of Majority (PVCNAG01), Student 14+ (PVCNAG02)
- **Cohorts**: 12 (Feb 2025 – Feb 2026). ~2.0M action / ~109K control
- **Target**: Non-VVD-holders. Static control group (SRF-based)

### VDA — Seasonal Acquisition
**Seasonal batch (~2x/yr)** | EM+IM / IM (Banner/O&O) | 95/5 split | 90-day window

- **Channel arms**: PVDAAG03 (Email + Banner, 250K cap), PVDAAG04 (Banner/O&O only, no cap)
- **Cohorts**: 2 (Jul 2025, Nov 2025). ~895K action / ~78K control
- **Target**: Non-VVD-holders. Tenure-based prioritization (≤8 years for email)

---

## Activation

### VDT — Activation Trigger
**Activity trigger (day 7 + day 15 reminder)** | Email (DO fallback for non-EM-eligible) | 90/10 split | 30-day window

- **Report groups**: 6 — Student/Non-Student × Activation/Reminder × EM/DO
- **Cohorts**: 14 (Jan 2025 – Feb 2026). ~130K action / ~14K control
- **Target**: ~5% of VVD cards that don't auto-activate. Static control (SRF-based)

---

## Usage

### VUI — Usage Trigger
**Activity trigger** | EM+IM / EM / IM | 95/5 split | 30-day window

- **Report groups**: 4 active — AoM × (EM+IM, EM, IM) + Student EM
- **Cohorts**: 13 (Feb 2025 – Feb 2026). ~149K action / ~8K control
- **Target**: Cardholders who activated in last 60 days but haven't transacted

---

## Wallet Provisioning

### VUT — Tokenization
**One-time batch (Jun 2025)** | Email | 95/5 split | 90-day window

- **Report group**: Single — PVUTAG01 (Age of Majority, EM only)
- **Cohorts**: 1. ~785K action / ~41K control
- **Target**: Existing VVD users to add card to digital wallet

### VAW — Add To Wallet
**Monthly** | In-app (iOS only) | 80/20 split | 30-day window

- **Report group**: Single — PVAWAG01 (IM)
- **Cohorts**: 10 (Apr 2025 – Feb 2026). ~402K action / ~100K control
- **Target**: Active VVD users (purchase in last 60 days), mobile-engaged, never provisioned

---

**Notes**

1. Action group receives communication. Control group does not (organic behavior).
2. No formal orchestration between campaigns. Each operates independently.
3. Static control groups (VCN, VDT) use client SRF for persistent holdout.
