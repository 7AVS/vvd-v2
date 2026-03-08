# VVD Campaign Program — Orchestration Audit Report

**Date:** 2026-03-07
**Auditor:** Director of Campaign Orchestration & Digital Strategy
**Data Period:** 2025-01-02 to 2026-03-06 (14 months)
**Source:** VVD v2 Pipeline Run 3 (20.8M deployments, 3.56M unique clients)

---

## Executive Assessment

**Rating: AMBER — Selectively Effective, Structurally Uncoordinated**

Four of six campaigns produce statistically significant lift. The program generates conservatively $499K–$979K in incremental revenue and has strong measurement infrastructure. However, the six campaigns operate as independent experiments with no shared contact policy, no funnel coordination, and no post-success suppression.

The headline number — 80% of volume going to VCN — requires nuance. VCN uses mobile banking notifications (MB), a passive in-app banner at effectively $0 per contact. This is not the same problem as VUT blasting 785K clients with zero-lift emails. The real waste in this program is not volume — it's misallocation: email-heavy campaigns (VUT, VUI, VDT) consuming inbox attention for zero or uncertain return, while the one campaign with the tightest targeting and best lift (VAW) gets only 2.1% of total volume.

The most actionable finding in this audit is the VUT vs VAW comparison. Both target wallet provisioning. VUT fails because it targets broadly (30-day recency, all platforms, email-only). VAW succeeds because it targets narrowly (6-day recency, iOS only, in-app, active mobile users). This is not a mystery — it's a targeting quality problem with a known fix.

---

## What's Working

**1. VAW is a best-in-class campaign — and we now know exactly why.**
+2.62pp absolute lift (80.7% relative), significant at 99.9% across every cohort, from a single-contact IM-only deployment consuming just 2.1% of total volume. 10,816 incremental clients. Clean SRM (p=0.99). VAW's targeting is the tightest in the portfolio: purchase within 6 days, activated digital/hybrid card, iOS only, active mobile user (login within 30 days), never previously provisioned. Every criterion selects for intent and channel fit. This is what efficient campaign design looks like.

**2. VDT delivers the highest absolute lift in the portfolio.**
+4.65pp on a 56.4% control baseline, producing 6,138 incremental activations. The 90/10 test/control split gives strong statistical power. Clean SRM (p=0.79). The 7-day reminder cadence (2.3d avg self-loop gap) is an intentional design pattern, not waste. Channel is 97% email / 3% display — appropriate for activation reminders where email serves as a task prompt.

**3. VCN and VDA produce real incremental acquisitions.**
VCN: 3,591 incremental clients at $78.21 = $280K. VDA: 2,793 incremental clients = $218K. Both statistically significant at 99.9%. Even with SRM caveats (controls larger than expected, which if anything makes the lift estimate conservative), these are defensible revenue claims.

**4. The measurement infrastructure is solid.**
Proper A/B testing with holdout controls. Vintage curve methodology. Chi-square SRM detection. Cohort-level significance testing. This is better than most campaign programs I've audited — the analytics capability is not the problem.

---

## Critical Issues (Priority Order)

### Issue 1: VUT Produces Zero Incremental Value — Root Cause Now Identified

**Evidence:**
- +0.02pp lift, p=0.9094. Not significant. Single cohort, single contact, 785K clients.
- Control rate (13.84%) is statistically identical to action rate (13.86%)
- v1 showed negative lift (-0.2%) and negative ROI (-$12K, -18X ROMI)
- Channel: 100% email — reaches users who may not be actively engaged with mobile banking
- VAW targets the identical outcome (wallet provisioning) and produces +2.62pp significant lift

**Root cause — VUT vs VAW side-by-side:**

| Dimension | VUT (fails) | VAW (works) |
|-----------|-------------|-------------|
| Recency | Purchase in last 30 days | Purchase in last 6 days |
| Platform | All platforms | iOS only (Apple Pay) |
| Channel | Email (100%) | In-app message (100%) |
| Mobile engagement | No requirement | Login in last 30 days |
| Already-provisioned exclusion | Unclear | Explicit |
| Card type | Digital or physical | Digital or hybrid only |
| Additional filters | Foreign currency history | — |

VUT fails for five compounding reasons:
1. **Recency decay.** 30-day purchase window includes clients whose intent has already cooled. VAW's 6-day window catches clients while the card is still top-of-mind.
2. **Platform mismatch.** VUT targets all platforms including Android and non-mobile users. Wallet provisioning is disproportionately an Apple Pay action — VAW's iOS restriction selects for the ecosystem where provisioning is frictionless.
3. **Channel mismatch.** Email reaches inboxes, not banking sessions. VAW's in-app message catches users who are already in the app — the provisioning action is one tap away. VUT's email requires the user to read, click through, open the app, find the wallet feature, and provision.
4. **No engagement gate.** VUT has no mobile activity requirement. It may be emailing users who haven't opened the app in months. VAW requires a login within 30 days — guaranteeing the audience is reachable via mobile.
5. **No already-provisioned exclusion.** VAW explicitly excludes clients who have already added VVD to their wallet. VUT's criteria don't mention this — it may be emailing clients who have already done the thing it's asking them to do.

**Recommendation:**
Discontinue VUT. Migrate its audience to VAW-style targeting with three modifications: (a) tighten recency to 7 days, (b) require active mobile session in last 14 days, (c) restrict to iOS initially, expand to Android only after confirming Google Pay provisioning flow is equally frictionless. If organizational constraints prevent full discontinuation, convert VUT's 95/5 split to 50/50 for one final quarter, then shut down.

**Effort:** Low. This is a targeting rule change. The campaign infrastructure already exists in VAW.

---

### Issue 2: VCN Volume — High but Not Necessarily Wasteful

**Evidence:**
- VCN = 15.8M deployments (80.5% of total), 2.0M clients, avg 7.9 contacts per client
- Contact 1 converts at 1.25%. Contact 10 converts at 0.23% — an 82% decline
- 13.1M VCN-to-VCN self-loop transitions at 33.9d avg gap
- VCN P90 = 14 contacts. Max = 14 (hard cap exists but is set too high)
- Channel: 100% MB (mobile banking notification) — passive in-app banner, ~$0 per contact

**The counterargument (and why it's partially valid):**
A product owner will argue: "VCN is a free mobile notification. It doesn't email anyone, doesn't send SMS, doesn't interrupt the user's session. It's a banner they see when they open their banking app. The marginal cost of contact #10 is literally zero. Why cap it?"

This argument has merit. MB notifications are the least intrusive channel in the portfolio. There is no inbox pollution, no unsubscribe risk, no spam fatigue that bleeds into other campaigns. The analogy is closer to a persistent banner ad than to an email campaign. Capping VCN at 3 contacts would eliminate ~10.7M deployments that cost approximately nothing to deliver.

**Where the counterargument breaks down:**
1. **Opportunity cost of the notification slot.** Even if MB is free, the banner real estate is finite. A client seeing their 12th VCN acquisition message could instead be seeing a VDT activation prompt or a VAW provisioning nudge — campaigns with higher per-contact conversion rates.
2. **Notification fatigue is real even for passive channels.** Users who see the same banner 14 times without acting may develop banner blindness that extends to future campaigns using the same channel. We cannot measure this directly, but the 82% conversion decline from contact 1 to contact 10 is consistent with it.
3. **Already-succeeded clients.** The more pressing issue is not frequency but targeting: some share of VCN's 2.0M audience has already acquired a VVD card. These clients are being shown an acquisition banner for a product they already hold. This is waste regardless of channel cost.

**Revised recommendation:**
Do NOT hard-cap VCN at 3 contacts. Instead:
1. **Implement post-success suppression immediately.** Remove clients who already hold a VVD card. This is the highest-impact, zero-controversy change.
2. **Set a soft cap at 6 contacts per rolling 12 months** (not 3). This preserves the low-cost, high-volume nature of MB while cutting the truly exhausted tail (contacts 7-14 where conversion is near zero).
3. **Repurpose freed notification slots.** When a client hits the VCN cap, rotate the MB slot to the next lifecycle-appropriate campaign (VDT if they've acquired, VAW if they've activated). This turns wasted impressions into funnel progression.

**Effort:** Low-Medium. Post-success suppression is a targeting rule. Slot rotation requires a basic decisioning layer.

---

### Issue 3: VUI Shows No Statistical Effect

**Evidence:**
- +0.78pp lift overall, NOT significant
- Zero individual cohorts reach significance
- 32.89% control baseline — high natural usage rate limits addressable lift
- Channel: EM_IM 80% + EM 12% + IM 8% — the most aggressive channel mix in the portfolio
- VUI same-day self-loops: 233K transitions at 0.0d avg gap (dual-channel same-day sends)

**Why it matters:**
159K clients and 393K deployments producing no detectable effect. The EM_IM channel (email + in-app simultaneously) is the most intrusive combination in the portfolio — it is the only campaign that hits clients in both their inbox and their banking app at the same time. Unlike VCN's passive MB banners, VUI's emails have real costs: inbox fatigue, unsubscribe risk, potential CAN-SPAM implications at scale. The 32.89% baseline means roughly 1 in 3 clients would use their card regardless — the campaign is targeting the already-converted.

**Recommendation:**
Pause VUI for one quarter while redesigning. Specific changes:
1. **Exclude active users.** Remove clients with any VVD transaction in the past 30 days. Target dormant cardholders only (60+ days no transaction).
2. **Drop dual-channel delivery.** The EM_IM simultaneous send is the most aggressive contact pattern in the program and produces no measurable lift. Switch to IM-only (following VAW's success pattern) or EM-only, not both.
3. **Implement the October 2025 recommendations** (expand population, increase control to 15%, relax 365-day resting rule) if not already done.

**Effort:** Medium. Requires targeting rule changes and channel reconfiguration.

---

### Issue 4: SRM in VCN and VDA Undermines Confidence

**Evidence:**
- VCN: 94.7/5.3 observed vs 95/5 expected (chi-sq=342.54, p=0.0000)
- VDA: 92.0/8.0 observed vs 95/5 expected (chi-sq=18,439.91, p=0.0000)
- Both skew in the same direction: control is larger than designed
- Persistent across all VCN cohorts, both VDA cohorts

**Why it matters:**
SRM means the randomization mechanism is not working as designed. The direction (more control than expected) is the "better" failure mode — it makes lift estimates conservative rather than inflated. But SRM flags a systematic process issue that could also affect unmeasured dimensions.

**Recommendation:**
Investigate the randomization mechanism with the campaign engineering team. Specifically: (1) Is there a suppression layer between the randomizer and delivery that preferentially drops action-group clients? (2) Does the randomization happen before or after eligibility filtering? (3) Are there delivery failures that disproportionately affect action clients (e.g., notification delivery failures for VCN's MB channel)?

**Effort:** Medium. Diagnostic task requiring campaign engineering involvement.

---

### Issue 5: 2.15M Overlapping Transitions With No Coordination Logic

**Evidence:**
- 2.15M transitions with gap < 25 days, affecting 727K clients (20.4% of the action universe)
- 752K same-day transitions (221K clients)
- VUT-to-VAW: 151K transitions at 63.9d gap — two campaigns targeting the same outcome sequentially
- VAW-to-VUT: 41K transitions at 7.5d gap — same goal, reverse order, one week apart
- VCN-to-VDA: 848K transitions at 32.2d gap — two acquisition campaigns back-to-back
- VDA-to-VCN: 787K transitions at 28.2d gap — same pair, other direction

**Why it matters:**
Clients receive acquisition campaign A followed by acquisition campaign B within a month, with no awareness of whether they already acquired. Wallet provisioning clients get VUT (which doesn't work) and then VAW (which does) — or vice versa — with no logic governing the sequence. This is six campaigns firing independently at overlapping audiences.

**Recommendation:**
Implement three suppression rules:
1. **Post-success suppression:** If a client succeeds at a campaign's goal, suppress all campaigns targeting that same goal for 90 days.
2. **Cross-campaign cooling (channel-aware):** After an email deployment (EM or EM_IM), suppress other email campaigns targeting the same client for 7 days. After an MB or IM deployment, suppress same-channel campaigns for 3 days (lower intrusiveness = shorter cooling).
3. **VUT/VAW deconfliction:** Solved by discontinuing VUT. If both are kept, never send both to the same client.

**Effort:** Medium-High. Requires a shared suppression table or campaign-aware decisioning layer.

---

## Channel Intrusiveness Framework

The program uses four distinct channels with fundamentally different cost and intrusiveness profiles. Any contact policy must be channel-aware.

| Channel | Campaigns | Intrusiveness | Cost/Contact | Fatigue Risk | Recommended Cap |
|---------|-----------|:------------:|:------------:|:------------:|:---------------:|
| MB (mobile notification) | VCN | Low | ~$0 | Banner blindness | 6/12mo |
| IM (in-app message) | VAW, VDA (67%) | Low-Medium | ~$0 | Moderate | 4/12mo |
| EM (email) | VUT, VDT (97%), VUI (12%) | High | ~$0.02-0.05 | Unsubscribe, spam | 2-3/12mo |
| EM_IM (dual channel) | VUI (80%), VDA (33%) | Highest | ~$0.02-0.05 | Dual fatigue | 2/12mo |
| DO (display/other) | VDT (3%) | Medium | Variable | Ad blindness | 4/12mo |

**Key insight:** A single "max contacts per client" policy is wrong for this program. A client who receives 10 MB notifications and 2 emails has a very different experience from a client who receives 10 emails and 2 MB notifications. The former barely notices; the latter is annoyed.

**Channel-aware contact policy (recommended):**
- MB: 6 contacts per 12-month rolling window. Post-success suppression. Slot rotation to next funnel stage at cap.
- IM: 4 contacts per 12-month rolling window. 3-day minimum gap between deployments.
- EM: 3 contacts per 12-month rolling window across all email-using campaigns (VUT, VDT, VUI). 7-day minimum gap. After 2 unanswered emails, suppress for 90 days.
- EM_IM: 2 contacts per 12-month rolling window. This is the most aggressive pattern — treat it as consuming both an EM and an IM slot.
- Program-level cap: 8 total contacts per client per 30-day rolling window (all channels combined). This is a safety net, not the primary control — channel-level caps should bind first.

---

## Orchestration Gaps

### 1. No Funnel Awareness
The six campaigns map to four lifecycle stages (Acquisition - Activation - Usage - Provisioning) but operate independently. A client who acquires a VVD card via VCN continues receiving VCN acquisition messages. There is no handoff: "this client acquired, now route to VDT for activation." The 8,962 VCN-to-VDT transitions (38.4d gap) happen by accident, not by design.

### 2. No Already-Succeeded Exclusion
Clients who have already acquired/activated/provisioned continue receiving campaigns for that same action. With 1.29M card acquisitions and 1.11M card activations in the success tables, a meaningful share of VCN/VDA/VDT audience has already completed the desired action organically. Every deployment to an already-succeeded client is pure waste — regardless of channel cost.

### 3. No Channel-Aware Contact Policy
There is no program-level policy distinguishing between channel types. A client could receive VCN (MB) + VUI (EM_IM) + VDT (EM) on the same day — a passive notification plus two emails plus an in-app message. The fact that MB is cheap does not make the email components free.

### 4. No Channel Optimization
Each campaign has a fixed channel assignment. VCN is 100% MB. VUT is 100% EM. VAW is 100% IM. There is no test of whether VUT's zero lift is a channel problem (it is — partially) or purely a targeting problem. The VAW evidence strongly suggests that IM outperforms EM for provisioning. This should be tested, not assumed.

### 5. No Lifetime Value Routing
The $78.21 revenue model applies to acquisition/activation only. Usage and provisioning campaigns have no revenue attribution. Without a unified LTV framework, the program cannot answer: "Should we spend the next deployment dollar on acquiring a new client or provisioning an existing one?"

---

## Quick Wins (This Quarter)

### 1. Discontinue VUT, Migrate Audience to VAW-Style Targeting
**Impact:** Eliminates 785K zero-effect email deployments. Root cause is now understood — VUT fails on recency (30d vs 6d), platform (all vs iOS), channel (EM vs IM), and engagement gating (none vs active mobile). The fix is not to optimize VUT; it's to apply VAW's proven targeting principles to VUT's audience.
**Implementation:** Remove VUT from campaign calendar. Create a VAW variant with relaxed criteria (7-day recency, iOS, in-app, active mobile users) to absorb eligible VUT audience. Phase expansion: 25% of VUT audience per quarter, monitoring lift per cohort for dilution.
**Projected gain:** If 20% of VUT's 785K audience qualifies under VAW-style targeting, that's 157K additional clients at 2.62% incremental lift = 4,113 additional provisioners.

### 2. Post-Success Suppression for All Campaigns
**Impact:** Stop sending acquisition messages to cardholders, activation messages to active users, provisioning messages to provisioned clients. With 1.29M acquisitions in the success table, a significant share of VCN's 2.0M audience may already hold the product. Each suppressed deployment is saved cost with zero revenue impact.
**Implementation:** Add success table lookup to each campaign's eligibility filter. Suppress clients who have already achieved the campaign's goal.

### 3. Soft-Cap VCN at 6 Contacts, Not 3
**Impact:** Acknowledges MB's low intrusiveness while cutting the exhausted tail (contacts 7-14). Preserves ~85% of VCN conversions while eliminating ~50% of VCN volume. Combined with post-success suppression (#2), this addresses the two real problems (targeting already-succeeded clients, and diminishing returns) without overcorrecting on a near-zero-cost channel.
**Implementation:** Add frequency counter to VCN targeting rules. At cap, rotate MB slot to next lifecycle-appropriate campaign.

### 4. Pause VUI Pending Redesign
**Impact:** Eliminates 393K ineffective deployments. Critically, frees EM and EM_IM channel capacity — the scarcest and most fatigue-prone channels in the portfolio. No revenue at risk.
**Implementation:** Remove VUI from campaign calendar. Redesign targeting to exclude active users, switch to IM-only delivery.

### 5. Fix VDT Cohort Instability
**Impact:** Several VDT cohorts (2025-02, 2025-08) are not significant despite the campaign's overall strong lift. Investigate whether these correspond to seasonal dips in card issuance.
**Implementation:** Pull monthly card issuance volumes and overlay against VDT cohort performance. Report findings.

---

## Strategic Recommendations (6-12 Month Roadmap)

### Phase 1: Channel-Aware Contact Policy Engine (Months 1-3)
Build a shared suppression and frequency management layer with channel-differentiated rules:
- **MB (VCN):** 6 contacts per 12 months. Post-success suppression. Slot rotation at cap.
- **IM (VAW, VDA partial):** 4 contacts per 12 months. 3-day minimum gap.
- **EM (VDT, VUI, VUT legacy):** 3 contacts per 12 months across all email campaigns. 7-day minimum gap. 90-day suppression after 2 unanswered.
- **EM_IM (VUI, VDA partial):** 2 contacts per 12 months. Counts against both EM and IM budgets.
- **Program-level safety net:** 8 total contacts per client per 30-day rolling window.

### Phase 2: Funnel-Aware Routing (Months 3-6)
Replace independent campaign targeting with a lifecycle state machine:
- **Stage 1 (Acquisition):** VCN/VDA targets non-holders only. On success, automatically route to Stage 2.
- **Stage 2 (Activation):** VDT targets holders with unactivated cards. On success, route to Stage 3.
- **Stage 3 (Usage):** Redesigned VUI targets dormant cardholders (60+ days no transaction). On success, route to Stage 4.
- **Stage 4 (Provisioning):** VAW targets active digital cardholders without wallet provisioning. iOS first, Android expansion pending Google Pay flow validation.

This eliminates most suppression rules — clients are only eligible for campaigns matching their current lifecycle stage.

### Phase 3: Channel Optimization (Months 4-8)
The VUT/VAW comparison provides a natural experiment suggesting IM outperforms EM for provisioning. Formalize this:
- **VDT:** Test IM variant against current EM-heavy approach. VDT's activation prompt ("your card is ready, activate now") may convert better as an in-app message when the user is already in a banking session.
- **VCN:** Test IM variant against current MB-only. VCN's MB notifications are passive; an IM interstitial may drive higher engagement for a subset of clients.
- Use results to build channel preference scoring per client.

### Phase 4: Revenue Attribution Overhaul (Months 6-12)
Replace the flat $78.21 model with stage-appropriate metrics:
- **Acquisition/Activation:** Keep $78.21 (or update with current figures)
- **Usage:** Incremental spend differential (Action avg spend - Control avg spend per client over 90 days)
- **Provisioning:** Incremental transaction lift post-provisioning. VAW data shows significant post-provisioning spend increases — quantify per client.
- Build a unified LTV model connecting acquisition through provisioning to ongoing revenue

---

## Revenue Opportunity

### Current Baseline (Run 3 Actuals)

| Campaign | Incremental Clients | Revenue ($78.21) | Claimable |
|----------|-------------------:|------------------:|:---------:|
| VCN | 3,591 | $280,852 | Yes |
| VDA | 2,793 | $218,359 | Yes |
| VDT | 6,138 | $480,033 | Arguable |
| VAW | 10,816 | N/A (no model) | — |
| VUI | 0 | $0 | No |
| VUT | 0 | $0 | No |
| **Total (conservative)** | **6,384** | **$499,211** | |
| **Total (with VDT)** | **12,522** | **$979,244** | |

### Estimated Uplift From Recommendations

**A. VUT discontinuation + VAW-style expansion:**
VUT's 785K audience at 0% incremental lift = $0 revenue. Root cause identified: broad targeting, wrong channel, no engagement gate. Migrating eligible clients to VAW-style targeting:
- Conservative estimate: 20% of VUT audience qualifies under tightened criteria = 157K clients
- At VAW's 2.62pp incremental lift = 4,113 additional provisioners
- Post-provisioning spend data shows +25-30% transaction lift per provisioned client
- At $2,206 avg annual spend and 25% incremental lift = $551 incremental spend per client
- **4,113 clients x $551 = $2.27M in incremental card spend** (top-line transaction volume)

**B. VCN soft cap at 6 + post-success suppression:**
- Soft cap eliminates ~7.9M deployments (contacts 7-14) while preserving ~90% of conversions
- Post-success suppression removes already-succeeded clients: est. 15% of audience = ~300K clients, ~2.4M deployments
- Combined: ~10.3M fewer deployments
- Conversion impact: ~5% reduction in VCN incrementals = ~180 fewer clients = -$14K
- **Net: -$14K revenue, ~10.3M fewer deployments**

**C. VUI pause:**
- 393K deployments eliminated, zero revenue at risk
- Frees the scarcest resource: email + IM channel capacity

**D. Post-success suppression (non-VCN campaigns):**
- VDA, VDT: Remove clients who have already achieved the campaign goal
- Estimated 10-15% of audience already succeeded = additional ~200K deployments saved
- Zero revenue impact

### Total Estimated Impact

| Lever | Deployments Saved | Revenue Impact |
|-------|------------------:|---------------:|
| VUT discontinuation | ~785K | $0 |
| VAW expansion | — | +$2.27M txn volume |
| VCN soft cap + suppression | ~10.3M | -$14K (negligible) |
| VUI pause | ~393K | $0 |
| Other post-success suppression | ~200K | $0 |
| **Total** | **~11.7M (56% of current volume)** | **Net positive** |

The program currently fires 20.8M deployments. These recommendations would reduce that to approximately 9.1M while preserving or improving revenue outcomes. The reduction is more moderate than the previous 68% estimate because MB notifications are now treated proportionally to their true cost and intrusiveness — cutting a $0 notification is not the same as cutting an email.

---

## Risk Factors

### 1. VAW May Not Scale Linearly
VAW's strong lift comes from a tightly curated audience (412K clients, single touch, IM channel, iOS only, 6-day recency, active mobile users). Expanding to absorb VUT's 785K audience will dilute targeting quality by definition — VUT's audience is broader and less engaged. VAW's lift could decay as the audience widens. Mitigation: expand in phases (25% of VUT audience per quarter), monitor lift per cohort, and stop expanding when incremental cohort lift drops below 1.0pp.

### 2. VCN Soft Cap May Be Too Generous
The revised recommendation (cap at 6 instead of 3) accepts the "MB is free" argument. But if banner blindness is real and unmeasured, even a soft cap may be too high. The 82% conversion decline from contact 1 to contact 10 is steep. Mitigation: implement at 6, review after two quarters. If contacts 4-6 show conversion rates below 0.30% (current contact-7 rate), tighten to 4.

### 3. SRM Root Cause Is Unknown
If the SRM in VCN and VDA reflects a systematic bias in the targeting pipeline (not just benign control inflation), all lift estimates for these campaigns may be unreliable. The $499K conservative revenue figure rests on campaigns with SRM warnings. Mitigation: resolve the SRM investigation before making revenue claims to leadership.

### 4. Channel-Aware Policy Adds Complexity
Differentiating frequency caps by channel (MB vs EM vs IM vs EM_IM) is the right approach but adds operational complexity. The campaign engineering team must maintain channel-level counters per client, not just campaign-level. Mitigation: start with the two extremes — implement MB and EM caps first. Add IM and EM_IM caps in Phase 1.

### 5. Funnel Routing Assumes Linear Journeys
The lifecycle state machine (acquire - activate - use - provision) assumes clients progress linearly. Some clients may provision before becoming regular users or may have multiple cards at different stages. Mitigation: build the state machine with "current state" detection (what has this client already done?) rather than strict sequential gating.

### 6. Organizational Resistance to Discontinuation
VUT and VUI have campaign owners who may resist shutdown recommendations. The VUT case is now stronger — we can explain *why* it fails (targeting breadth, channel mismatch, no engagement gate) and point to VAW as proof that the same goal is achievable. Frame it as "reinvesting VUT's audience into VAW's proven model," not "killing a campaign." For VUI, the dual-channel aggression angle gives a concrete reason: it's the only campaign hitting clients in two channels simultaneously for zero measurable return.

### 7. Revenue Model Limitations
The $78.21 per-client figure has no documented time horizon (year 1? lifetime?). The spending analysis shows VCN control acquirers spend MORE per client ($1,459) than action acquirers ($1,340) — suggesting the campaign may acquire lower-quality clients. This needs investigation before scaling. Additionally, the provisioning revenue estimate ($551/client incremental spend) is derived from aggregate post-provisioning spending data, not a controlled experiment — treat it as directional, not precise.

---

*End of audit. Data sources: VVD v2 Pipeline Run 3 (2026-03-07), VVD v1 Key Findings Catalog, Campaign Configuration from Vintage/Vvd pipeline, campaign targeting specifications.*
