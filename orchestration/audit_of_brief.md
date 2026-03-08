# Audit of the VVD Orchestration Brief

**Date:** 2026-03-07
**Auditor:** Director of Campaign Orchestration & Digital Strategy
**Document under review:** `orchestration_brief.md`
**Supporting reference:** Independent audit (`audit_report.md`), campaign stories (`campaign_stories.md`), project context

---

## Overall Assessment

The brief is well-structured, channel-aware, and significantly more honest than most orchestration documents I see at this stage. The VUT vs VAW analysis is genuinely strong and presentation-ready. However, three weaknesses could undermine credibility with leadership: (1) the VCN cap debate hedges too much and lands on a number (5) without rigorous justification, (2) the "already succeeded" waste section is the brief's most powerful argument but is entirely estimated with no computed numbers, and (3) the brief systematically avoids engaging with the SRM problem's implications for the revenue story. If presented as-is, the biggest risk is a senior leader asking "how much of that $499K is real if SRM is broken?" and getting no answer.

---

## Section-by-Section Critique

### Executive Summary

**Brief claims:** "80.5% of volume goes to VCN (0.47% success)" and "VAW — with 2.1% of volume and a single contact per client — delivers 10,816 incremental clients at +2.62pp lift."

**Assessment:** Strong, with one misleading comparison.

**Why:** The framing juxtaposes VCN's 0.47% success rate against VAW's implied efficiency. But these are not comparable metrics. VCN's 0.47% is the raw success rate (action group clients who acquired a card). VAW's 5.87% is its raw success rate. The 0.47% vs 5.87% comparison involves different funnel stages, different denominators, different baselines. The executive summary should make this explicit rather than letting the reader infer comparability.

The claim "zero cross-campaign coordination" is asserted but only partially demonstrated. The transition matrix shows self-loops and same-stage lateral movement, but the brief never checks whether the campaign platform has any suppression rules configured. It infers "no coordination" from outcome data, which is reasonable but not definitive. The campaigns could have suppression rules that simply don't bind for most clients.

**What's missing:** The executive summary does not mention the SRM problem. For a document going to leadership, burying the fact that the two acquisition campaigns both have broken randomization until the reader reaches the Assumptions section (Section 2b, point 5) is a mistake. It should be flagged up front.

---

### Section 1a: The Portfolio Is Structurally Inverted

**Brief claims:** "VAW produces 3x the incremental clients of VCN with 38x fewer deployments."

**Assessment:** Strong claim, but the "structurally inverted" framing is too aggressive.

**Why:** The math checks out: VAW = 10,816 incremental clients from ~412K deployments (38.1 per incremental). VCN = 3,591 from ~15.8M deployments (4,403 per incremental). The 116x efficiency gap is real. But calling the portfolio "structurally inverted" implies the fix is to shift volume from VCN to VAW. The brief later acknowledges (correctly) that VCN is near-zero cost and VAW may not scale — but the "structurally inverted" headline, which is what leadership will remember, does not carry these caveats.

The table mixes statistically significant and non-significant campaigns without visual distinction. VUT and VUI are marked "(not sig)" in parentheses but sit in the same table as campaigns with p<0.001. A reader skimming the table could miss the parentheticals.

**What's missing:** The table shows VDT at 61.05% success rate, which is by far the highest. But VDT's base rate (control) is 56.40%, so the absolute lift is 4.65pp. The raw success rate without context makes VDT look like the star when in fact VAW's 2.62pp lift on a 3.25% baseline is a far more impressive relative lift (80.7% vs 8.2%). The brief should note this — it strengthens the VAW story, which is the brief's centerpiece.

---

### Section 1b: Channel Intrusiveness Hierarchy

**Brief claims:** "This hierarchy changes how we evaluate over-contacting."

**Assessment:** Strong conceptual framework. The hierarchy is logical and well-articulated.

**Why:** The five-tier channel hierarchy (MB < IM < EM < EM_IM < DO) is sensible and the descriptions are accurate. This is probably the brief's single most important contribution — it reframes the entire overcontacting narrative from "too many contacts" to "too many intrusive contacts."

**What's missing:** The hierarchy is asserted, not measured. The brief presents no data on unsubscribe rates, email open rate decay, or app engagement metrics. It is entirely theoretical. A leadership audience will ask: "Do we have evidence that email fatigue is actually happening, or is this just a framework?" The answer is: we have 2.2M rows of email engagement data from Run 3 that we haven't analyzed for this purpose. The brief lists this in Section 2a as a gap but doesn't flag how close we are to having the evidence — the data exists, it just hasn't been correlated with success outcomes.

---

### Section 1c: Over-Contacting — The Nuanced View

**Brief claims:** "The 4-5 bucket's high rate is NOT evidence that more contacts help. It reflects campaign composition."

**Assessment:** Plausible but explicitly unproven.

**Why:** The brief acknowledges this directly: "This is testable — break the bucket by campaign — but we haven't done it yet." This is honest, but it leaves the brief making a compositional claim without the composition data. If a skeptic asks "how do you know it's VDT clustering?" the answer is "we believe it but haven't verified." This is listed as a P1 analysis in Section 6a, which is appropriate, but the body text states it as fact rather than hypothesis.

The VCN diminishing returns curve (1.25% -> 0.23% over 10 contacts) is the brief's strongest quantitative evidence. These numbers are specific, directional, and unambiguous.

**What's missing:** The brief does not discuss whether the VCN diminishing returns reflect true diminishing returns or survivorship bias. Clients who reach contact 10 are, by definition, clients who did NOT convert on contacts 1-9. They may be structurally lower-intent. The declining conversion rate could reflect selection into a progressively harder-to-convert population, not fatigue from repeated contact. This distinction matters for the cap recommendation: if it's true fatigue, a cap helps; if it's population selection, a cap merely stops contacting people who wouldn't have converted anyway, which is still waste but for a different reason. The policy response is the same (cap), but the causal story is different, and a sophisticated audience will notice.

---

### Section 1c (continued): The VCN Cap Debate

**Brief claims:** The brief presents 5 arguments for capping and 5 against, then resolves to cap at 5.

**Assessment:** The debate is well-structured but the resolution is weak.

**Why:** The pro-cap arguments are strong: diminishing returns, experimentation slot cost, SRM noise, analytical distortion, opportunity cost of app real estate. The anti-cap arguments are also legitimate: zero cost, minimal annoyance, still converting, no evidence of harm, normalized behavior.

The problem is the resolution. "A cap at 5 (not 3) may be the pragmatic compromise — it captures 74% of the diminishing returns" is not justified. Where does 74% come from? The brief shows conversion rates at contacts 1, 2, 3, 5, and 10. It does not show what percentage of total VCN conversions come from contacts 1-5 vs 6-14. "74% of the diminishing returns" is not the same as "74% of conversions." The brief conflates the two. If most conversions happen at contact 1 (which the 1.25% rate vs 0.33% at contact 5 suggests), then capping at 3 might capture 85%+ of conversions. Capping at 5 might capture 90%+. The brief does not compute this.

The independent audit recommends 6, not 5, using the same qualitative reasoning. Neither document shows the actual conversion distribution that would settle this. This is a number that leadership will fixate on ("why 5 and not 3 or 7?"), and the brief cannot defend it rigorously.

**What's missing:** A cumulative conversion curve. Show: of all VCN successes, what percentage occurred at contact 1? Contacts 1-2? 1-3? 1-5? 1-10? This is trivially computable from result_df and would immediately settle the cap debate with data instead of judgment.

---

### Section 1d: VUT vs VAW

**Brief claims:** "Two campaigns targeting the same outcome with a 130x efficiency gap, entirely explained by five targeting differences."

**Assessment:** This is the brief's strongest section. The analysis is specific, the comparison table is clear, and the recommendation flows directly from the evidence.

**Why:** The five-dimension comparison (recency, platform, provisioning exclusion, channel, mobile engagement) is concrete and actionable. Each dimension has a clear mechanism. The recommendation — discontinue VUT and redirect to VAW — is the only logical conclusion.

Two concerns:

1. The brief claims the gap is "130x" but uses different metrics to calculate it: VUT lift of +0.02pp vs VAW lift of +2.62pp. That's a 131x ratio, which checks out. But the brief sometimes uses "130x efficiency gap" and sometimes "116x" (in the appendix, comparing deployments per incremental client: 4,403/38 for VCN/VAW). These are different ratios measuring different things. The 130x is the lift ratio for VUT/VAW specifically. The 116x is the deployment-efficiency ratio for VCN/VAW. The brief should not mix them.

2. The expansion potential estimate — "If even half of VUT's audience meets VAW's tighter targeting criteria... that's an additional ~10K incremental provisioning clients" — is aggressive. VAW's tighter targeting criteria (iOS only, 6-day recency, active mobile, not already provisioned) would likely filter out significantly more than 50% of VUT's audience. VUT targets all platforms and uses a 30-day window. iOS is roughly 50% of the market, but 6-day recency from a 30-day pool might be 20-30%. The overlap could be more like 10-15%, not 50%. At 15%, that's ~3K incremental, not ~10K. The brief should present a range.

**What's missing:** The brief does not mention whether VUT and VAW share any audience currently. The `adhoc_vut_vaw_overlap.py` script was written for exactly this purpose but hasn't been run. This is acknowledged elsewhere in the project memory but not in the brief itself. If 60% of VUT clients were also in VAW, the "redirect" story changes — those clients already got the better treatment.

---

### Section 1e: There Is No Designed Funnel

**Brief claims:** "Only 0.9% of transitions cross funnel stages."

**Assessment:** Misleading framing.

**Why:** The 0.9% number is correct but uses all transitions as the denominator, including VCN self-loops (13.1M). VCN self-loops are not transitions in any meaningful sense — they are the same campaign repeating. If you exclude self-loops, the denominator drops to ~3.1M, and cross-stage transitions become ~3% of non-self-loop transitions. Still low, but 3x the impression of 0.9%.

More importantly, the brief assumes funnel progression should happen through the campaign system. But clients acquire cards, activate them, and start using them without campaign intervention — that's what the control group proves. The "funnel" may be working fine at the client level even if campaigns don't orchestrate it. The brief conflates "campaigns don't coordinate" with "clients don't progress," which are different claims.

The backward transitions (VUT->VCN = 9,878) are genuinely problematic and the brief is right to flag them. But the volume is small relative to the 16.2M total transitions. The brief calls them "waste by definition," which is fair only if we're sure the VCN deployment targeted these clients for VCN-specific reasons (and not just because VCN targets all non-holders monthly, which would include provisioning-stage clients who have a card but are being re-screened for some reason). The brief does not investigate this.

**What's missing:** The brief should acknowledge that a triggered campaign system (which VCN, VDT, VUI, VUT are) naturally produces self-loops and same-stage patterns because eligibility criteria don't change between triggers. A client who qualifies for VCN in January and doesn't convert will qualify again in February. That's not a coordination failure — it's how trigger-based campaigns work. The issue is not that self-loops exist but that there's no exit condition (success suppression) or lifecycle routing.

---

### Section 1f: Same-Day Deployments

**Brief claims:** "VUI->VUI: 233K transitions, avg gap 0.0 days... This is not a strategy decision — it's a system behavior."

**Assessment:** Strong. This is clearly a bug, clearly wasteful, and clearly actionable.

**Why:** Same-day email duplicates are indefensible regardless of framework. The brief is correct that "No business case for sending a 'use your card' message multiple times on the same day" via email. The volume (233K transitions, 116K clients) is meaningful.

The VDT characterization is less clear. The brief says "589K transitions, avg gap 2.3 days" and calls the reminder windows "too aggressive." But the MEMORY.md notes that VDT uses a 7-day + 7-day reminder cadence by design. If avg gap is 2.3 days, some of these "transitions" may be within the same reminder cycle, not separate re-triggers. The brief should distinguish between designed reminders and unintended re-triggers.

---

### Section 1g: Two Campaigns Should Not Exist

**Brief claims:** VUT and VUI "should not exist in their current form."

**Assessment:** VUT is airtight. VUI is premature.

**Why:** The VUT case is strong: zero lift, p=0.91, v1 showed negative ROI, root cause identified, superior alternative exists. This recommendation would survive any challenge.

VUI is weaker. The brief says "no individual cohort reaches significance" and the 32.89% baseline means "clients are already using their cards without prompting." But the brief also notes (in Proposal 5) that "VUI might have a real but small effect that our sample can't detect at 95/5 split." If VUI has a true effect of 0.5-1.0pp, it's real value being missed because of underpowered design. The October 2025 recommendations (expand population, increase control to 15%) were specifically designed to fix this. The brief recommends pausing before checking whether those changes were implemented. That's backwards — check first, then decide.

**What's missing:** The brief does not mention that VUI's 1,232 "incremental clients" (even if not significant) could represent real activity. The 95/5 split means the control group is tiny (N~8K for VUI based on 159K total clients at 5%), giving very low power. With VDA's actual 92/8 split showing SRM, there's precedent for the designed splits not matching reality. The brief should compute the minimum detectable effect at VUI's current sample sizes to show what lift would be needed for significance.

---

### Section 2a: What We Cannot Tell

**Assessment:** Honest and well-structured. The gaps are real.

**One issue:** The brief lists "Revenue per deployment" as a gap and says "Cannot compute ROI without cost data." But for MB (VCN), the brief itself argues the cost is zero. For email campaigns, industry benchmarks for cost per email ($0.02-0.05) are cited in the independent audit's channel framework. The brief could have provided rough ROMI estimates using benchmark costs rather than throwing up its hands entirely.

---

### Section 2b: Assumptions We Are Making

**Brief claims:** Six explicit assumptions.

**Assessment:** This is unusually good practice. Most briefs hide their assumptions.

**Two weak spots:**

Assumption 4 ("the 4-5 contact bucket's high success rate is compositional") should not be labeled an assumption. It's either true or false, and it's testable with data already in hand. It should be in the "must compute before presenting" list, not the assumptions list.

Assumption 6 ("VAW's targeting explains its performance, not just its audience quality") contains its own rebuttal: "the control group has the same targeting criteria, so the lift is real." If the control group has the same targeting, then by definition the lift cannot be explained by audience quality alone. This is not an assumption — it's a fact of the experimental design. The brief should state it with more confidence.

---

### Section 3: Key Findings Summary

**Assessment:** Solid summary. No new claims, just consolidation.

**One issue:** Section 3a claims "If VCN were capped at 5 contacts per client, ~9M deployments eliminated while preserving the low-cost tail." The ~9M figure implies that 9M of VCN's 15.8M deployments are contacts 6+. This means the median client getting 7 contacts and the top tail getting 14 creates a distribution where 57% of deployments are contact 6 or higher. This is plausible given the stated median of 7, but the brief should show the actual distribution (how many deployments are at each contact number) rather than asking the reader to trust the estimate.

---

### Section 4: "Already Succeeded" Waste

**Brief claims:** "300K-800K provably wasted VCN deployments" and "400K+ wasted VDT deployments."

**Assessment:** Unsupported estimates presented as the brief's most powerful argument.

**Why:** The brief calls this "the single most impactful number for the presentation" but the number doesn't exist yet. The estimates rely on assumptions about when the average successful client succeeds (contact ~3 for VCN) that are not backed by data. The PySpark code is provided but hasn't been run. The brief is essentially saying "we think this number will be impressive but we haven't checked."

This is the brief's biggest vulnerability. If the actual number is lower than estimated (say, 100K VCN wasted deployments instead of 300K-800K), the argument loses much of its force. If it's higher, the brief understated it. Either way, presenting an uncomputed estimate as the centerpiece of a leadership deck is a risk.

The PySpark code itself looks correct — the window function approach is appropriate. But the code computes post-first-success deployments, which is the right metric only if success is irreversible (true for acquisition, true for activation, questionable for usage).

**What's missing:** The code should also flag what percentage of post-success contacts are VCN (MB) vs VDT (email). If 90% of post-success waste is VCN banners, the severity is low. If 40% is VDT emails, the severity is high. The aggregate number without the channel split is less useful.

---

### Section 5: Proposed Orchestration Improvements

See "Recommendations Assessment" below for detailed critique of all 7 proposals.

---

### Section 6: What Would Make This Presentation-Ready

**Assessment:** This is the most operationally useful section.

**One concern:** The "must-have" vs "nice-to-have" prioritization is correct, but the brief does not estimate the time required for the must-haves. P0 (already-succeeded waste) is labeled "Medium" PySpark complexity. P1 (frequency bucket by campaign) is "Low." But in a Lumina environment with YARN sessions that can corrupt on long queries, "Medium" could mean 30 minutes or 4 hours. The brief should give time estimates so the reader can decide what to run.

---

### Section 7: Sankey Visualization

**Brief claims:** "Probably not for the 10-minute presentation."

**Assessment:** Correct recommendation, well-justified. The brief saves the reader time by explaining why the obvious visual won't work. The suggested alternative (simplified flow diagram in PowerPoint) is pragmatic.

---

### Section 8: The Narrative Arc

**Assessment:** Strong presentation structure. The VUT vs VAW hero moment at minute 3-6 is the right choice — it's concrete, surprising, and immediately actionable.

**One issue:** The opening line — "We run 6 VVD campaigns across 4 channels" — should be "across 5 channel types" (MB, IM, EM, EM_IM, DO) to match the brief's own hierarchy in Section 1b. Minor, but a director will notice the inconsistency.

The narrative does not include a slide or moment for the revenue story. The brief's Section 5 summary table shows ~10.8M-11.6M deployments eliminated, but the presentation narrative never mentions the $499K-$979K revenue baseline or the incremental value from VAW expansion. Leadership will want a dollar figure. The narrative arc implicitly assumes the audience cares about deployments and efficiency, but budget holders care about revenue at risk and revenue opportunity.

---

## Claims That Don't Survive Scrutiny

**1. "74% of the diminishing returns" justifying the cap-at-5.**
No cumulative conversion curve is presented. The 74% figure appears without derivation. The cap number is the brief's most consequential recommendation for VCN, and it's not backed by the data that would settle it.

**2. "300K-800K provably wasted VCN deployments" (Section 4).**
The word "provably" is doing heavy lifting. The range is 2.7x wide (300K to 800K), derived from assumptions about average success timing, and the computation hasn't been run. "Estimated" or "projected" would be appropriate. "Provably" is not.

**3. "If even half of VUT's audience meets VAW's tighter targeting criteria... ~10K additional incremental provisioning clients" (Section 1d).**
The 50% overlap estimate is likely too high. VAW requires iOS + 6-day recency + active mobile + not provisioned. VUT's audience (all platforms, 30-day recency, no engagement gate) would lose ~50% on iOS alone, another 50-70% on the 6-day recency filter, and more on the engagement gate. A realistic estimate might be 10-20% overlap, yielding 2K-4K incremental clients.

**4. "Only 0.9% of transitions cross funnel stages" (Section 1e).**
True by the math, misleading by the framing. The 13.1M VCN self-loops inflate the denominator. Excluding self-loops, the figure is approximately 3%. Still low, but the 0.9% headline overstates the case.

**5. The VDA success rate of 1.36% in the portfolio table (Section 1a).**
VDA has SRM with chi-sq = 18,439 and an observed split of 92/8 vs designed 95/5. This is the most severe SRM in the portfolio, far worse than VCN's. The brief mentions this in passing (Section 2b, point 5) but the portfolio table in Section 1a presents VDA's numbers without any caveat. The 2,793 incremental clients and $218K revenue claim both rest on a campaign where 60% more clients ended up in control than designed. That's a randomization failure, not a rounding error.

---

## Claims That Are Stronger Than the Brief Realizes

**1. VAW's relative lift is 80.7% — the best in the portfolio.**
The brief focuses on absolute lift (+2.62pp) and incremental clients (10,816). But the relative lift (80.7% improvement over control) is the most impressive number in the entire program. VDT's +4.65pp sounds bigger, but on a 56.4% baseline it's only 8.2% relative lift. VAW literally doubles its clients' provisioning rate relative to what would have happened organically. The brief undersells this.

**2. The channel intrusiveness framework could apply beyond VVD.**
The brief treats the MB < IM < EM < EM_IM < DO hierarchy as a VVD-specific insight. It's not. This framework is applicable to every marketing campaign in the organization. If leadership adopts channel-aware contact policies for VVD, it sets a precedent for all programs. The brief misses this strategic leverage.

**3. The SRM direction (more control than expected) is actually helpful for credibility.**
The brief says "both skew in the same direction: control is larger than designed." This means our lift estimates are likely conservative (the control group outperformed its designed size, which typically means the control baseline is more precisely measured). The brief notes this in passing but doesn't emphasize that this SRM direction is the best-case failure mode. A leadership audience will worry less about SRM if told "the error, if any, makes our numbers conservative."

**4. VDT's 90/10 split is evidence of good design.**
The brief doesn't highlight that VDT is the only campaign using a 90/10 split instead of 95/5. This gives VDT 2x the control group size, which is why it has clean SRM (p=0.79) and strong per-cohort significance. This is evidence that the program knows how to design experiments properly — it just doesn't do it consistently. This strengthens the VUI recommendation to increase its control size.

---

## Recommendations Assessment

### Proposal 1: Cap VCN at 5 Contacts Per Client

**Verdict:** Directionally right, number unjustified.

The case for A cap is strong. The case for THIS cap is weak. The brief acknowledges the independent audit recommends 6. The brief's own data shows conversion at contact 5 is 0.33% and at contact 10 is 0.23% — a difference of 0.10pp over 5 additional contacts. The yield curve from 5 to 10 is nearly flat, meaning contacts 6-10 add almost nothing. But contacts 3-5 are also nearly flat (0.50% to 0.33%). The "cap at 5 vs 3 vs 6" debate cannot be resolved without computing: of all VCN conversions, what cumulative percentage occurs at each contact number?

**Risk:** A cap at 5 removes 57% of VCN volume. If the product owner pushes back, there is no quantitative defense beyond "diminishing returns." Compute the cumulative conversion distribution first.

### Proposal 2: Discontinue VUT, Redirect to VAW

**Verdict:** Strongest recommendation in the brief. Well-supported, specific, actionable.

The only risk is the expansion estimate (50% of VUT audience eligible for VAW). This should be presented as a range (10-50%) rather than a point estimate. The recommendation itself — discontinue VUT — requires no expansion estimate. Even if zero VUT clients redirect to VAW, discontinuing a zero-lift email campaign is pure savings.

### Proposal 3: Fix VUI Same-Day Email Duplicates

**Verdict:** No-brainer. This is a bug fix, not a strategic recommendation. Should be framed as such.

233K duplicate same-day emails is a system defect. The fact that it appears as "Proposal 3" of 7 strategic recommendations undersells the urgency. This should be separated from the strategic proposals and presented as: "This is a bug. Fix it immediately. While we investigate VUI's overall effectiveness, at minimum stop sending the same email twice on the same day."

### Proposal 4: Suppress Post-Success Same-Campaign Contacts

**Verdict:** Conceptually strong, quantitatively empty.

The brief estimates 300K-800K VCN and 400K+ VDT wasted deployments but hasn't computed either number. The recommendation depends on the magnitude. If VDT post-success waste is 400K emails, this is urgent. If it's 40K, it's a nice-to-have. Run the PySpark code before presenting.

**Implementation risk the brief misses:** Post-success suppression requires near-real-time success detection. If success data arrives in a daily batch but VDT triggers fire intra-day, there will be a 24-hour gap where post-success clients still receive emails. The brief says "Real-time (or daily batch)" but doesn't address this lag.

### Proposal 5: Evaluate VUI for Pause/Redesign

**Verdict:** Too timid, given the email channel cost.

The brief recommends "evaluate" and suggests a 50/50 test. But VUI is currently sending EM_IM (dual-channel, the most intrusive combination) for zero measurable lift. The brief's own framework ranks EM_IM as the highest-severity channel. Continuing to send EM_IM while "evaluating" contradicts the brief's channel framework. The recommendation should be: pause EM_IM immediately, run a 50/50 IM-only test for one quarter.

The 50/50 test suggestion is sound, but the brief doesn't compute what effect size it could detect. At VUI's current population (~159K clients) with a 50/50 split, the minimum detectable effect at 80% power and 95% confidence would be roughly 0.4-0.5pp. If VUI's true effect is 0.78pp (the current point estimate), a 50/50 split should detect it. This is worth stating — it makes the recommendation more concrete.

### Proposal 6: Build a Cross-Campaign Suppression Layer

**Verdict:** Strategically correct but operationally ambitious.

The brief describes this in three sentences. Building a shared suppression table that checks success outcomes across campaigns requires: (a) a single source of truth for success by client and campaign, (b) integration with the campaign platform's targeting engine, (c) refresh cadence fast enough to catch triggered campaigns. This is a multi-month engineering project, not a "proposal." The brief should separate the concept (which is right) from the implementation (which needs scoping).

### Proposal 7: Channel-Aware Orchestration Rules

**Verdict:** The most strategically important proposal, presented last and with insufficient emphasis.

The channel-specific frequency caps are well-designed. But the implementation section is a simple table with no discussion of: who owns the rules engine, whether the campaign platform supports channel-level counters, or how conflicts are resolved (a client hits both an EM cap and a campaign-level priority — which wins?). The brief should flag this as the anchor for the 6-12 month roadmap.

---

## The Brief's Blind Spots

**1. The revenue story is absent from the recommendations.**
The brief quantifies deployments eliminated (~10.8M-11.6M) but never answers: "What revenue are we putting at risk?" The independent audit estimates -$14K from the VCN soft cap. The brief should present a revenue impact column for each proposal. Leadership will ask "what do we lose?" before caring about "what do we save?"

**2. No discussion of organizational dynamics.**
The brief assumes recommendations can be implemented based on evidence alone. In practice, VUT and VUI have campaign owners. VCN has a product owner whose "free channel, still converting" argument the brief acknowledges. The brief should include a stakeholder impact section: who will resist which recommendation, and what data would address their objections.

**3. The control group quality question is unaddressed.**
The brief notes SRM in VCN and VDA but does not ask: are the control groups representative? If the SRM mechanism is related to client characteristics (e.g., certain client segments are systematically excluded from action), then the control group is biased and all lift estimates are wrong. The brief should acknowledge this as a scenario and state what would change if lift estimates were off by, say, 20%.

**4. No temporal analysis.**
The brief treats 14 months of data as a single period. But VDA is seasonal, VAW launched in April 2025, and VUI may have changed configuration in October 2025. Campaign performance may vary significantly by quarter. The brief's numbers are averages that could mask deterioration or improvement. A Q-by-Q trend for each campaign's lift would strengthen or weaken several claims.

**5. The brief never mentions client lifetime value or retention.**
All analysis focuses on immediate campaign outcomes (did the client acquire/activate/provision?). There is no discussion of whether campaign-acquired clients retain or churn. The independent audit mentions that VCN control acquirers spend MORE per client ($1,459) than action acquirers ($1,340). If true, this is a significant finding that undermines VCN's revenue story — the campaign may be acquiring lower-quality clients. The brief should engage with this.

**6. No sensitivity analysis on the VAW expansion.**
VAW is the hero of the brief. If VAW's lift decays when expanded (the independent audit warns about this), the entire "redirect VUT to VAW" story weakens. The brief should present scenarios: what if VAW's lift drops to 1.5pp? To 1.0pp? At what point does expansion stop being worthwhile?

---

## Verdict

**Revise Sections 1c, 1e, and 4 before presenting. Everything else is presentable with minor edits.**

Specifically:

1. **Section 1c (VCN cap):** Compute the cumulative conversion distribution before choosing a cap number. Replace "74% of the diminishing returns" with actual data. This is the difference between a defensible recommendation and an opinion.

2. **Section 1e (funnel flow):** Reframe the 0.9% number by excluding self-loops from the denominator, or present both denominators. Acknowledge that trigger-based campaigns naturally produce self-loops and that the real issue is the absence of exit conditions, not the presence of repetition.

3. **Section 4 (already-succeeded waste):** Run the PySpark code before presenting. If you present estimates, label them as estimates with explicit assumptions. Do not use the word "provably" for an uncomputed number.

4. **Executive summary:** Add one sentence on SRM. Something like: "VCN and VDA both show SRM warnings — lift estimates should be treated as directional, and a root cause investigation is needed before budget reallocation."

5. **Add a revenue impact column** to the Proposal summary table in Section 5. Even rough estimates ($0 at risk, -$14K at risk, etc.) give leadership a way to evaluate the proposals.

The VUT vs VAW analysis (Section 1d) is ready for leadership as-is. It is specific, well-evidenced, and actionable. Lead with it.

---

*End of audit. The brief is 80% ready. The 20% that needs work is concentrated in three areas: the VCN cap justification, the uncomputed waste numbers, and the revenue framing. Fix those, and this is a strong piece of analysis.*
