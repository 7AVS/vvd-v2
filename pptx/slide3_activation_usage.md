# Activation & Usage: VDT Delivers +4.65pp — Highest Lift in Portfolio | VUI Underpowered, No Detectable Effect

---

## LEFT — VDT (Activation Trigger)

### Plot Title
**6.1K Incremental | +4.65pp Lift** — Highest in Portfolio, Recent Waves Strengthening (Dec–Feb: +3.5 to +4.9pp), Email Drives 100% of Signal

NO NIBT — activation is not new client acquisition.

### Vintage Curve
*(placeholder — user builds the chart)*

### One Campaign, Two Nudges — The Reminder Is the Real Driver

VDT is a single two-stage campaign (confirmed by config decision tree). Clients who don't activate after the first email (day 7) automatically receive a reminder (day 15). AG11/AG13 are a subset of AG01/AG03 — same clients, second attempt.

**The reminder re-accelerates the curve.** On the MNE vintage curve, the action-control gap opens in the first 2 weeks (first email), slows around day 8, then widens again after day 8 when the reminder fires. Control has no such inflection — just steady organic activation.

Wave-level action-control gaps (cohort 2026-02):

| Wave | Action Day 30 | Control Day 30 | Gap |
|------|--------------|----------------|-----|
| Activation (AG01, Student EM) | ~70% | ~65% | ~4.7pp |
| Reminder (AG11, Student EM) | ~46% | ~37% | **~8.9pp** |
| Activation (AG03, Non-Student EM) | ~65% | ~61% | ~3.7pp |
| Reminder (AG13, Non-Student EM) | ~46% | ~37% | **~8.9pp** |

The reminder gap (~9pp) is nearly 2x the activation gap (~4pp). Clients who don't respond to the first nudge respond even more strongly to the second. This is the most actionable finding in VDT: the campaign's value is in the follow-up, not the first contact.

**DO fallback is a dead end.** AG02/AG04 (Digital Online) have negative lift (-3.1pp, -0.4pp) and never receive a reminder. Non-email-eligible clients get one failed attempt and nothing more.

**Shrinking addressable market.** 95% of VVD cards auto-activate at issuance. VDT targets the remaining ~5%. As Card Type 03 (digital) grows from 47% today, this tail shrinks.

**Recommendation**: The reminder is the real value driver — protect it. Investigate whether a third nudge (day 21?) could further lift the remaining non-activator tail. Model Card Type 03 growth for sunset planning. Consider dropping DO fallback (zero value, no reminder path).

---

## RIGHT — VUI (Usage Trigger)

### Plot Title
**No Significant Effect | +0.78pp Lift (p=0.054)** — No Wave Reaches Significance, Baseline Usage at 33%, ~600 Control Clients Per Cohort

NO NIBT — not significant.

### Vintage Curve
*(placeholder — user builds the chart)*

### Underpowered by Design, Not Necessarily Failed

VUI's control group is ~600 clients per cohort. With that sample size, a cohort needs roughly **+3.9pp lift** to reach statistical significance. Even the strongest cohort (Feb 2026, +3.61pp) falls just short at p=0.073.

Baseline usage is 33% — one in three clients transact organically without any messaging. The campaign is trying to move a metric that already happens at high rates.

Increasing control from 5% to 15% would bring the detectable threshold down to ~0.78pp (per Oct 2025 analysis) — well within the range of observed lifts.

October 2025 deep dive recommended three changes:
1. **Expand target population** — 81% of eligible clients are excluded because their activation was >61 days ago
2. **Increase control from 5% to 15%** — brings detectable threshold from ~3.9pp to ~0.78pp
3. **Relax 365-day resting rule to 30 days** — allows more re-engagement

Implementation status of all three: **Unknown**.

233K same-day email duplicates detected — a system bug where multiple qualifying transactions trigger multiple emails to the same client on the same day.

**Recommendation**: The measurement design cannot detect effects below ~3.9pp per cohort. Increasing control to 15% is the single highest-impact fix. Implement Oct 2025 recommendations, re-measure one quarter. If still not significant with adequate power, consider discontinuation or pivot to in-app only.
