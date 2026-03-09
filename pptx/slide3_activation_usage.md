# Activation Delivers Highest Lift in Portfolio — Usage Shows No Detectable Effect

---

## VDT — Activation Trigger
**Activity trigger (day 7 + day 15 reminder)** | Email / DO fallback | 90/10 split | 30-day window

Highest absolute lift in portfolio: +4.65pp overall (7.6% relative), significant at 99.9%.

### Email Works, DO Fails

| Channel | RPT_GRP_CDs | Lift Range | Significant |
|---------|------------|------------|-------------|
| Email (activation + reminder) | AG01, AG03, AG11, AG13 | +3.03 to +5.38pp | All yes |
| DO fallback | AG02, AG04 | -3.06 to -0.41pp | None |

100% of VDT's signal comes from email. DO (Digital Online) is the fallback for non-email-eligible clients — it has zero or negative lift.

The reminder email (day 15) outperforms the activation email (day 7). PVDTAG11 (Student reminder) at +5.38pp is the single strongest segment in the entire portfolio. Clients need the second nudge.

### Shrinking Addressable Market

95% of VVD cards are digital and auto-activate at issuance. VDT targets the remaining ~5% that require manual activation. Card Type 03 (digital) has grown from 0% in 2022-06 to 47% of the experiment population in 2025-2026. As digital card penetration increases, VDT's addressable market shrinks.

NIBT is not applicable — activation ≠ new client acquisition. These clients already have the card.

**Recommendation**: Continue short-term (strong lift, large effect size). Model the Card Type 03 growth trajectory. When auto-activation reaches 70%+, VDT becomes obsolete. Consider whether the activation nudge creates lasting engagement or merely accelerates an inevitable activation.

---

## VUI — Usage Trigger
**Activity trigger** | EM+IM / EM / IM | 95/5 split | 30-day window

Overall: +0.78pp lift (2.4% relative) — NOT significant (p=0.054). No cohort reaches significance.

### Underpowered, Not Failed

VUI's control group is ~580-720 clients per cohort — far too small. The minimum detectable lift at this sample size is 1.29pp. If the true effect is below that, VUI will never detect it regardless of how many cohorts run.

The baseline usage rate is 33% — one in three clients transact organically without any messaging. Moving a high-baseline metric requires either massive sample sizes or very large effect sizes.

### October 2025 Deep Dive — Three Recommendations

An internal deep dive (Oct 2025) diagnosed the power problem and recommended three changes:

1. **Expand target population** — 81% of eligible clients were excluded because their activation was >61 days ago
2. **Increase control from 5% to 15%** — reduces minimum detectable lift from 1.29% to 0.78%
3. **Relax 365-day resting rule to 30 days** — allows more re-engagement

Implementation status of all three recommendations is **unknown**.

### System Bug: Same-Day Email Duplicates

233K VUI→VUI transitions occur at 0.0-day gap — clients receiving multiple usage trigger emails on the same day. This is a system bug (multiple qualifying transactions = multiple emails), not a strategy decision. This creates email fatigue risk for a campaign with no measurable effect.

NIBT is not applicable — not significant.

**Recommendation**: Cannot conclude the campaign doesn't work — the measurement design prevents detection. Implement the Oct 2025 recommendations, re-measure for one quarter. If still not significant at p<0.10, consider discontinuation or pivot to in-app only channel.

---

## Design Notes

1. VDT's two waves share a static control group (SRF 90-99). Double-counting at MNE rollup is symmetric — rates and lift are unbiased.
2. VUI action codes use the PVUT prefix, creating naming overlap with VUT. Distinguish by MNE column.
3. VDT's DO channel is a fallback, not a deliberate channel test.
