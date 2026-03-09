# VVD Experiment Design Overview

Six independent A/B tests measuring the causal effect of targeted communications on Virtual Visa Debit (VVD) card outcomes. Each campaign operates independently — no formal orchestration between them. Action group receives communication; Control group does not (organic behavior only).

Measurement period: January 2025 – February 2026.

---

## Acquisition

### VCN — Contextual Notification
**Monthly trigger** | MB (mobile push) | 95/5 split | 30-day window

VCN is a monthly screening campaign that identifies RBC clients who do not hold a VVD card and sends them a mobile push notification encouraging card acquisition. The target population is filtered through a decision tree: clients must have an active PBA (personal banking account), be active in mobile banking, have an opened DDA (demand deposit account), and meet spending thresholds — chequing money out >$500/month and >10 transactions in the past 6 months (high debit usage), while also being a low credit card user (<10 monthly transactions). Newcomers and students 14+ using the Student Banking app are also included.

The population is segmented into two report groups: PVCNAG01 (Age of Majority — standard marketing) and PVCNAG02 (Student 14+ — operational). Both receive MB (mobile push). The control group is static, based on the last 2 digits of client SRF (95-99), designed to measure cumulative impact of repeated VCN messages over time.

- **Scale**: 12 monthly cohorts (Feb 2025 – Feb 2026). ~1.1M clients targeted per cohort. ~2.0M unique action clients, ~109K control across the period
- **Note**: Persistent SRM (sample ratio mismatch) observed across all cohorts — 94.7/5.3 vs designed 95/5

---

### VDA — Seasonal Acquisition
**Seasonal batch (~2x/yr)** | EM+IM / IM (Banner/O&O) | 95/5 split | 90-day window

VDA is a seasonal acquisition campaign deployed approximately twice per year during high-engagement periods (summer and holiday). It targets RBC clients with an active PBA who are active in OLB (online banking) and do not hold a VVD card. The targeting criteria include: high debit card usage (>$1,000/month OR >10 transactions in past 6 months), low credit card usage ($500 or less/month OR <6 transactions), and newcomers. Several criteria were removed during revisions: ~~ATM users~~, ~~IOP users~~, ~~PayPal users~~, and an ~~IOP decommissioning exclusion~~ were all struck through in the config document.

The campaign runs a two-arm channel test. PVDAAG03 receives Email + Banner/Ad-serve/O&O with a 250,000 email cap, prioritized by tenure (≤8 years). PVDAAG04 receives Banner/Ad-serve/O&O only (no email) with no volume cap. The split is determined by email eligibility and tenure — clients eligible for email with tenure ≤8 years go to PVDAAG03; the rest go to PVDAAG04 with tenure up to 25 years.

HSBC client isolation is noted in the Test and Learn section — depending on approval to market to new HSBC clients, the campaign may only include shared HSBC clients.

- **Scale**: 2 seasonal waves (Jul 2025, Nov 2025). ~755K deployments per wave. ~895K unique action clients, ~78K control
- **Note**: SRM observed — 92/8 vs designed 95/5

---

## Activation

### VDT — Activation Trigger
**Activity trigger (day 7 + day 15 reminder)** | Email / DO fallback | 90/10 split | 30-day window

VDT is a triggered campaign that fires when a VVD card has been issued but not activated. The first email is sent 7 calendar days after card issuance. If the client still hasn't activated by day 15, a reminder email is sent. The target population includes all RBC clients (including students aged 13-17) who have 1 or more non-activated VVD card issued within the last 7 days — this includes replacements for lost, stolen, or expired cards.

Approximately 95% of VVD cards auto-activate at issuance. This campaign targets the remaining ~5% that require manual activation, making the addressable market small but the potential lift high.

The population is segmented by age (Student 13-17 vs Non-Student >17) and email eligibility. There are 6 report groups:
- **Activation wave (day 7)**: PVDTAG01 (Student, EM), PVDTAG02 (Student, DO fallback), PVDTAG03 (Non-Student, EM), PVDTAG04 (Non-Student, DO fallback)
- **Reminder wave (day 15)**: PVDTAG11 (Student, EM), PVDTAG13 (Non-Student, EM)

DO (Digital Online) is NOT a deliberate channel test — it is the fallback for clients who are not email-eligible. The reminder wave has no DO fallback; non-EM-eligible clients are simply excluded from reminders. The control group is static (SRF last 2 digits 90-99, 10% of population), shared between activation and reminder waves.

The original success definition — ~~10% increase in activation with first email, 5% with second email~~ — was struck through in the config document and no replacement was specified.

- **Scale**: 14 monthly cohorts (Jan 2025 – Feb 2026). ~51K triggers per month. ~130K unique action clients, ~14K control
- **Measurement**: RESP_END: DS+30 (30 days from decision date)

---

## Usage

### VUI — Usage Trigger
**Activity trigger** | EM+IM / EM / IM | 95/5 split | 30-day window

VUI is a triggered campaign targeting existing VVD cardholders who activated their digital card in mobile within the last 60 days but have not yet made any purchase. Clients must also be active mobile users (logged in within last 90 days). The decision tree applies several additional filters before eligibility: the client must be a low credit card user, a high debit user, and have taken out >$100 in foreign currency in the past 2 years — making this a heavily restrictive targeting funnel.

The population is segmented into 4 active report groups:
- PVUIAG01: Age of Majority, Email + Mobile Banner eligible (EM_IM)
- PVUIAG02: Age of Majority, Email only eligible (EM)
- PVUIAG03: Age of Majority, Mobile Banner only + email dedup overflow (IM)
- PVUIAG04: Student 14+, Email + Mobile Banner eligible (EM)

Two additional student segments — ~~PVUIAG05 (Student EM only)~~ and ~~PVUIAG06 (Student IM only)~~ — were crossed out in the config document.

The stated success targets are: increase monthly transactions to 11.5 trxs/mth (from current 10.8) and increase average transaction amount to $55 (from current $45).

An October 2025 deep dive recommended three changes: (1) expand target population — 81% of clients excluded because activation was 61+ days ago, (2) increase control from 5% to 15% to lower minimum detectable lift from 1.29% to 0.78%, (3) relax the 365-day resting rule to allow more re-engagement. Implementation status of these recommendations is unknown.

- **Scale**: 13 monthly cohorts (Feb 2025 – Feb 2026). ~28K triggers per month. ~149K unique action clients, ~8K control (very small — ~580-720 per cohort)
- **Note**: VUI action codes use the PVUT prefix (PVUT01AA, etc.), creating naming overlap with the VUT campaign

---

## Wallet Provisioning

### VUT — Tokenization
**One-time batch (Jun 2025)** | Email | 95/5 split | 90-day window

VUT is a single-deployment batch campaign sent in June 2025 targeting existing VVD cardholders to add their card to a digital wallet. The targeting criteria require the client to meet any of three conditions: (1) made at least 1 VVD transaction in the last 30 days, (2) be a low credit card user AND high debit user, or (3) have taken out >$100 in foreign currency in the past 2 years. Clients targeted by VUI in the last 90 days are excluded (resting rule).

Only one report group remains active: PVUTAG01 (Age of Majority, Email eligible). The campaign was originally designed with 6 report groups across Student/Non-Student segments and EM/IM channels, but 5 were removed during revisions: ~~PVUTAG02 (Student EM)~~, ~~PVUTAG03 (AoM IM)~~, ~~PVUTAG04 (Student EM+IM)~~, ~~PVUTAG05 (Student EM)~~, ~~PVUTAG06 (Student IM)~~. All student segments and all in-app channels were eliminated.

The stated success targets are ambitious: 18% incremental transaction volume and 17% incremental revenue, both measured over 5 years.

Campaign period: June 9, 2025 – September 9, 2025. One-time decisioning (May 2025 build).

- **Scale**: Single cohort. ~785K action clients, ~41K control
- **Note**: The SAS success definition has a known bug — the `any_wallet` flag references the wrong alias (Success02 instead of Success03), and uses a -30 day pre-treatment window that counts activity 30 days before treatment start

---

### VAW — Add To Wallet
**Monthly** | In-app (iOS only) | 80/20 split | 30-day window

VAW is a monthly campaign targeting existing VVD cardholders to add their card to Apple Pay. It uses in-app messaging (IM channel) and is restricted to iOS users only (handled by MarTech, CIDM not required). The targeting criteria are specific: clients must be active VVD users (at least 1 purchase in the last 60 days), have activated a digital or hybrid VVD card in mobile, have NEVER added VVD to mobile wallet before, be low credit card users ($100 or less/month OR <2 transactions in past 6 months), and be active mobile users (logged in at least 1 time in the last 30 days).

Single report group: PVAWAG01 (IM). The control group is 5% — though the config doc says 5%, the actual deployment data shows an 80/20 action/control split. The stated success targets: 25% of active VVD users provisioning card to wallet, increased in-app purchases, and increased tap-in-store usage internationally.

Campaign period: April 14, 2025 – March 31, 2026. Monthly decisioning starting April 10, 2025.

VAW and VUT target the same outcome (wallet provisioning) but differ fundamentally in design: VAW uses in-app messaging to a targeted, mobile-engaged, iOS-only audience with monthly refresh and already-provisioned exclusion. VUT used a single email blast to a broad audience with no platform restriction.

- **Scale**: 10 monthly cohorts (Apr 2025 – Feb 2026, missing May 2025). ~402K action clients, ~100K control

---

## Design Notes

1. **Action group** receives communication. **Control group** does not — organic behavior only.
2. **No formal orchestration** between campaigns. Each operates independently with its own targeting, timing, and control group.
3. **Static control groups** (VCN, VDT) use client SRF last digits for persistent holdout, enabling cumulative impact measurement across repeated contacts.
4. **VUI → VUT resting rule**: VUT excludes clients targeted by VUI in the last 90 days. This is the only explicit cross-campaign dependency.
5. **Student segments** were removed from VUT (all 5 groups) and VUI (2 groups) during config revisions, but remain active in VCN (AG02), VDT (AG01, AG11), and VUI (AG04).

---

## Presenter Notes

### VDT Double-Counting (if asked)
VDT has two waves sharing a single static control group: activation email (day 7) and reminder email (day 15). When rolling up to MNE level, the same client can appear in both waves, inflating raw counts. However, this double-counting is **symmetric** — both Action and Control groups are equally inflated — so success **rates** and **lift** remain unbiased. Per-RPT_GRP_CD analysis is clean (no overlap). Bottom line: MNE-level rollup is valid for rates and lift; just don't sum raw client counts across waves.
