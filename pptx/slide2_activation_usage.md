# Activation Delivers Highest Lift in Portfolio — Usage Shows No Detectable Effect

---

## VDT — Activation Trigger
**Activity trigger (day 7 + day 15 reminder)** | Email / DO fallback | 90/10 split | 30-day window

Highest absolute lift in the portfolio: +4.65pp overall (7.6% relative), with all cohorts significant at 99.9%. Recent cohorts are strengthening — Dec +3.51pp, Jan +4.57pp, Feb +4.85pp — suggesting the campaign is not fatiguing.

Scale: 14 monthly cohorts (Jan 2025 -- Feb 2026). ~51K triggers/month. ~130K unique action clients, ~14K control.

### Channel Breakdown

This is the key finding. Email drives 100% of the signal. The DO fallback channel has zero or negative lift.

| RPT_GRP_CD | Segment | Wave | Channel | Clients | Lift | Sig |
|--|--|--|--|--|--|--|
| PVDTAG03 | Non-Student | Activation (day 7) | EM | 99,951 | +4.70pp | 99.9% |
| PVDTAG13 | Non-Student | Reminder (day 15) | EM | 56,850 | +4.87pp | 99.9% |
| PVDTAG11 | Student | Reminder (day 15) | EM | 5,175 | +5.38pp | 99% |
| PVDTAG01 | Student | Activation (day 7) | EM | 10,289 | +3.03pp | 95% |
| PVDTAG04 | Non-Student | Activation (day 7) | DO | 16,325 | -0.41pp | No |
| PVDTAG02 | Student | Activation (day 7) | DO | 2,629 | -3.06pp | No |

### Key Insights

- **Email drives 100% of the signal.** DO fallback has zero or negative lift.
- **Reminder email (day 15) outperforms activation email (day 7)** — clients need the second nudge. PVDTAG11 (+5.38pp) is the single strongest segment in the portfolio.
- **95% of VVD cards auto-activate at issuance.** VDT targets the remaining ~5%. As digital card types grow (Card Type 03 now at 47%), the addressable market is shrinking.
- **NIBT**: $480K (6,139 incremental x $78.21) — but $78.21 is an acquisition proxy. Activation is not new client acquisition. Arguable fit.
- **Recommendation**: Continue short-term (strong lift). Monitor shrinking TAM. Quantify when Card Type 03 makes VDT obsolete.

---

## VUI — Usage Trigger
**Activity trigger** | EM+IM / EM / IM | 95/5 split | 30-day window

Overall lift: +0.78pp (2.4% relative) — NOT significant (p=0.054). No individual cohort reaches significance. Best attempts: 2025-04 (+3.64pp, p=0.069), 2026-02 (+3.61pp, p=0.073). Baseline usage sits at 33% — 1 in 3 clients transact organically without any messaging.

Scale: 13 monthly cohorts (Feb 2025 -- Feb 2026). ~28K triggers/month. ~149K unique action clients, ~8K control (~580-720 per cohort).

### Channel Breakdown

Nothing reaches significance across any channel.

| RPT_GRP_CD | Segment | Channel | Clients | Lift | p-value |
|--|--|--|--|--|--|
| PVUIAG01 | AoM | EM_IM | 102,572 | +0.62pp | 0.337 |
| PVUIAG03 | AoM | IM | 31,979 | +1.26pp | 0.288 |
| PVUIAG04 | Student 14+ | EM | 13,066 | +2.64pp | 0.171 |
| PVUIAG02 | AoM | EM | 1,205 | -6.54pp | 0.321 |

### Key Insights

- **Underpowered by design, not necessarily broken.** ~600 control clients per cohort is far too small. Minimum detectable lift is 1.29pp.
- **Heavily restrictive targeting funnel**: must be <60 days since activation, low CC user, high debit, foreign currency user. 81% of eligible clients are excluded.
- **Oct 2025 deep dive recommended 3 changes**: (1) expand 61-day activation window, (2) increase control from 5% to 15%, (3) relax 365-day resting rule to 30 days. Implementation status unknown.
- **Same-day email duplicates**: 233K VUI-to-VUI transitions at 0.0-day gap — system bug, not strategy.
- **NIBT**: $0 — no significance, no revenue claim.
- **Recommendation**: Cannot conclude campaign doesn't work. Implement Oct 2025 recommendations, re-measure for 1 quarter. If still not significant, consider discontinuation or pivot to IM-only.

---

## Design Notes

1. VDT's two waves (activation + reminder) share a static control group (SRF 90-99). Double-counting at MNE rollup is symmetric — rates and lift are unbiased.
2. VUI action codes use the PVUT prefix (PVUT01AA, etc.), creating naming overlap with the VUT campaign. Distinguish by MNE, not action code prefix.
3. VDT's DO channel is NOT a deliberate channel test — it is the fallback for clients who are not email-eligible.

---

## Presenter Notes

### VDT — Why is lift so high?
Email is effective for activation triggers because the action is binary (activate or don't) and time-sensitive (card just issued). The reminder at day 15 works better than day 7 because it catches clients who intended to activate but forgot. The DO channel fails because it lacks the urgency and personalization of email.

### VUI — "Doesn't work" vs "Can't tell"
Be precise: we cannot say VUI doesn't work. We can say we don't have enough statistical power to detect an effect. The Oct 2025 recommendations would fix this. If someone asks "should we kill VUI?" — the answer is "not yet, but we need to fix the measurement design first."

### VDT NIBT Caveat
If challenged on the $480K: "We're applying the $78.21 per-client acquisition value as a proxy. These are not new clients — they already have the card. A more precise model would use post-activation spend differential from deep analysis. We're flagging this as an area for refinement."
