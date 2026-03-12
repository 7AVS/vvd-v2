# VVD Presentation Review — Errors, Weaknesses & Narrative

## RED FLAGS — Fix Before Presenting

### 1. Numbers That Don't Match Source of Truth

| Slide | What You Show | Source of Truth | Action |
|-------|--------------|-----------------|--------|
| Slide 3 — VDT overall lift | +4.25pp | **+4.65pp** (campaign_summary.csv) | Fix. You even say "4.65pp" in the body text of the same slide — contradicts your own headline |
| Slide 3 — VUI overall lift | +1.12pp | **+0.78pp** (campaign_summary.csv, p=0.2053) | Fix. Overstating a non-significant result is the worst place to have an error |
| Slide 3 — VDT email fallback | -2.44pp | **+1.13pp** aggregate in source, but individual DO segments (PVDTAG02/04) are negative | Verify which granularity you intended. If it's Q1-specific or RPT_GRP-specific, label it precisely |
| Slide 4 — VUT rates | 8.19% vs 8.12% | **13.86% vs 13.84%** (campaign_summary.csv) | Verify — you may be showing wallet provisioning rate vs overall success rate. If intentional, label what the metric actually is |

### 2. Typo
Slide 3, VDT section — "significancy" → **"significance"**

### 3. "Q1 2026: Nov–Jan"
Calendar Q1 is Jan–Mar. If this is fiscal Q1 (RBC fiscal year starts Nov 1), fine, but label it "Fiscal Q1" to avoid confusion.

---

## YELLOW FLAGS — Weaknesses That Could Bite You

### 4. SRM Is Completely Absent
VCN (94.7/5.3 vs 95/5) and VDA (92/8 vs 95/5) both have severe SRM flags (p=0.0000). If anyone in the room knows what SRM is, they'll ask — and discovering you omitted it looks worse than disclosing it. At minimum, have a ready answer. Better: add a small footnote like "SRM noted for VCN/VDA; lift direction confirmed across all cohorts."

### 5. VDT's $480K NIBT Is Silently Excluded
Your $499K headline is VCN+VDA only. Smart — activation ≠ new client. But someone will do the math: 6,138 × $78.21 = $480K. Have your answer ready: "NIBT applies to net-new client acquisition only. VDT activates existing cards, so revenue attribution requires a different model."

### 6. VDT Headline Inconsistency Within Slide 3
The title says +4.25pp, the body says 4.65pp, the channel table says 4.09pp (email) and -2.44pp (fallback). Three different numbers for the "same" campaign. A skeptic reads this as: "which one is it?" Align on one primary number and label the others clearly (e.g., "email-only: 4.09pp").

### 7. No SRM Caveat, but VCN/VDA Lift Presented as Definitive
The $499K NIBT headline is your biggest claim. It rests entirely on two campaigns with broken randomization. A sophisticated audience member could invalidate your whole story.

### 8. No Closing/Summary Slide
You go campaign-by-campaign but never pull up to portfolio-level. A 10-minute spotlight needs a "so what" moment — total value, rank-ordered recommendations, what you're doing next.

### 9. VAW Android Recommendation
"Consider Android (Google Pay)" — make sure you know if there's a technical/business reason this hasn't been done. Otherwise you're inviting: "Why haven't we already?"

---

## MINOR NOTES

- "Virtual Vista Debit" in the title — confirm this is the correct product name (project docs say "Virtual Visa Debit")
- The "next best action" watermark in bottom-right of every slide — intentional branding or leftover template artifact?
- Vintage curves are hard to read at projection size — the cohort annotations may be too small in a meeting room

---

## Presentation Narrative — 10-Minute Talk Track

### Opening (30 sec)

> "We measured six VVD campaigns across the full customer lifecycle — acquisition, activation, usage, and wallet provisioning. Here's what works, what doesn't, and where we're leaving money on the table."

### Slide 1 — The Portfolio (1 min)

> "Six campaigns, four funnel stages. Each has a different channel, cadence, and target. The key insight from this measurement is that **design matters more than volume** — our highest-volume campaign is our least efficient, and our smallest campaign delivers the best ROI per deployment."

Set up the tension. Don't walk through every box — they can read.

### Slide 2 — Acquisition: $499K NIBT, Efficiency Varies 8x (3 min)

> **VCN** (left side): "VCN generates consistent, significant lift every month. But it takes 4,400 mobile banner impressions to produce one incremental cardholder. The median client sees 7 notifications. After the first contact, response drops 48%. By the 10th, it's down 82%. Our recommendation: frequency cap at 3–5 contacts with a resting period."

> **VDA** (right side): "VDA runs twice a year — seasonal batch. Same goal, 8x more efficient: 541 deployments per incremental client. And we have a natural channel test: email+in-app delivers 4.6x the lift of in-app alone. Recommendation: maintain dual-channel, consider adding a third seasonal wave."

> **The portfolio headline**: "$499K in net incremental business value from these two campaigns alone."

*If asked about SRM*: "We flagged sample ratio mismatch on both — VCN at 94.7/5.3, VDA at 92/8 versus the 95/5 design. The lift direction is consistent across all cohorts, which gives us confidence the signal is real, but the exact magnitude could be biased. We're investigating root cause."

### Slide 3 — Activation & Usage: One Winner, One Underpowered (2.5 min)

> **VDT**: "The strongest per-client effect in the portfolio. The two-stage email design — day 7 reminder plus day 15 follow-up — delivers +4.65pp lift, significant in 10 of 12 months. Recent waves are strengthening. This is the campaign doing exactly what it should."

> **VUI**: "No detectable effect — but that's a measurement problem, not necessarily a campaign problem. With only ~600 control clients per cohort, we need +3.2pp lift just to reach significance. Even VUI's best month (+3.6pp) falls short. Our recommendation: expand the target population and increase control to 15%. That drops the detection threshold to ~1pp."

*Key point*: Frame VUI as "underpowered by design" — you're not saying the campaign failed, you're saying the measurement can't detect success at this sample size. Big difference politically.

### Slide 4 — Wallet Provisioning: The Clearest A/B in the Portfolio (2.5 min)

> **The contrast**: "Same goal — get clients to add VVD to their digital wallet. Opposite results. VUT sent one email blast to 785K clients. Zero measurable effect. VAW sends one in-app message to iOS users. +2.62pp lift, all 8 cohorts significant, 38 deployments per incremental — the best efficiency in the entire portfolio."

> **Why the difference**: "VUT targeted stale clients via email. VAW targets recently-active mobile users at the moment they're in the app. Precision beats volume. One in-app message outperforms 785K emails."

> **Recommendation**: "Discontinue or fundamentally redesign VUT. Expand VAW — consider Android/Google Pay equivalent."

### Close (30 sec)

> "Three takeaways: One — frequency caps on VCN, we're over-contacting and seeing diminishing returns. Two — VUI needs a measurement redesign before we can evaluate it. Three — VAW is the most efficient campaign in the portfolio and should be expanded. Total confirmed incremental value: $499K from acquisition alone, with activation and provisioning adding thousands of incremental clients."

---

## Anticipated Questions & Ready Answers

| Question | Answer |
|----------|--------|
| "Why not include VDT in the NIBT?" | "NIBT model values net-new clients. VDT activates existing cards — different revenue model needed." |
| "Is VCN worth keeping if it's so inefficient?" | "Zero marginal cost channel — the question isn't whether to keep it, but whether to cap frequency. First contact converts at 1.25%, seventh at near-zero." |
| "Can we trust the numbers given SRM?" | "Lift direction is consistent across all cohorts and time periods. SRM means the exact magnitude may be biased, but the signal is real. Root cause investigation is underway." |
| "Why is VUI control so small?" | "95/5 split with ~28K deployments/cohort = ~1,400 control. After applying eligibility filters, ~600 remain. Increasing to 85/15 is our top recommendation." |
| "What's next?" | "Three actions: implement VCN frequency cap, redesign VUI measurement (expand population + 15% control), explore VAW Android expansion." |

---

## Bottom Line

Fix the four number discrepancies and the typo before you present. Have your SRM answer rehearsed. The story itself is strong — **design beats volume, precision beats mass outreach**. That's a clear, memorable message for a 10-minute slot.
