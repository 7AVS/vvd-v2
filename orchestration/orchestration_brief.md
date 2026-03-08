# VVD Orchestration Brief

## Executive Summary

Six VVD campaigns deploy 20.8M contacts to 3.56M clients with zero cross-campaign coordination. The result: 80.5% of volume goes to VCN (0.47% success), 22.6% of clients are over-contacted (11+ touches at 2.41% success), and 2.15M deployment transitions overlap within 25 days. Meanwhile, VAW — with 2.1% of volume and a single contact per client — delivers 10,816 incremental clients at +2.62pp lift.

But the orchestration story is not simply "cut everything." **Channel matters.** VCN is a passive mobile banking notification — zero marginal cost, minimal intrusiveness, no inbox clutter. Email-based over-contacting (VDT, VUT, VUI) carries real costs: unsubscribe risk, inbox fatigue, brand perception damage. The recommendations must be channel-aware.

The strongest finding is not about VCN volume — it's about **VUT vs VAW**: two campaigns targeting the same outcome (wallet provisioning) with a 130x efficiency gap, entirely explained by targeting precision, channel choice, and recency filters. This is the most actionable insight in the program.

---

## 1. The Story We CAN Tell

### 1a. The Portfolio Is Structurally Inverted

The program spends most of its contact budget on its least efficient campaign:

| Campaign | % of Deployments | Success Rate | Incremental Clients | Channel |
|----------|-----------------|-------------|-------------------|---------|
| VCN | 80.5% | 0.47% | 3,591 | MB (passive banner) |
| VDA | 7.7% | 1.36% | 2,793 | EM_IM / IM |
| VUT | 4.0% | 13.86% | 155 (not sig) | EM (email 100%) |
| VDT | 3.7% | 61.05% | 6,138 | EM / DO |
| VAW | 2.1% | 5.87% | 10,816 | IM (in-app 100%) |
| VUI | 2.0% | 33.67% | 1,232 (not sig) | EM / EM_IM |

**VAW produces 3x the incremental clients of VCN with 38x fewer deployments.** VDT produces 1.7x the incremental clients of VCN with 22x fewer deployments. The volume allocation is inverted relative to efficiency.

### 1b. Channel Intrusiveness Hierarchy

Not all contacts are equal. The program uses five channel types with very different client impact:

| Channel | Intrusiveness | Campaigns | Client Impact |
|---------|--------------|-----------|---------------|
| **MB** (Mobile Banking) | Lowest | VCN | Passive banner at top of app. No push, no email, no interruption. Client sees it only when they open the app. Zero marginal cost. |
| **IM** (In-App Message) | Low | VAW, VDA (partial) | Message within the app. Slightly more prominent than MB but still in-context — client is already in the banking app. |
| **EM** (Email) | Medium | VDT, VUT, VUI (partial) | Inbox delivery. Competes with all other email. Carries unsubscribe risk. Creates brand fatigue. Has a real cost per send. |
| **EM_IM** (Email + In-App) | High | VDA (partial), VUI (partial) | Dual-channel. Client gets both an email AND an in-app message. Most intrusive combination in the program. |
| **DO** (Digital Outbound) | Medium-High | VDT (small portion) | Push notification or similar outbound. Interrupts the client outside the app. |

**This hierarchy changes how we evaluate over-contacting.** Sending 14 VCN mobile banners is fundamentally different from sending 14 emails. The diminishing returns have different denominators.

### 1c. Over-Contacting — The Nuanced View

The frequency data tells a story of diminishing returns:

| Contacts | Clients | Success Rate | Marginal Value |
|----------|---------|-------------|---------------|
| 1 | 915K (26.9%) | 12.53% | Baseline |
| 2 | 493K (14.5%) | 7.02% | -44% vs 1st |
| 3 | 259K (7.6%) | 11.07% | Rebounds (campaign mix effect) |
| 4-5 | 350K (10.3%) | 21.15% | Peak (VDT clients clustering here) |
| 6-10 | 616K (18.1%) | 8.69% | Declining |
| 11+ | 770K (22.6%) | 2.41% | -81% vs 1st |

The 4-5 bucket's high rate is NOT evidence that more contacts help. It reflects campaign composition: VDT clients (who have 61% success rates) cluster at 4-5 contacts. The 11+ bucket is almost entirely VCN clients being repeatedly shown banners at sub-3% rates.

**VCN-specific diminishing returns:**

| Contact # | Success Rate | Cumulative Drop |
|-----------|-------------|----------------|
| 1 | 1.25% | -- |
| 2 | 0.65% | -48% |
| 3 | 0.50% | -60% |
| 5 | 0.33% | -74% |
| 10 | 0.23% | -82% |

After contact 3, each additional VCN deployment converts at less than half the rate of the first. The median VCN client gets 7 contacts. The P90 gets 14.

#### The VCN Cap Debate — Both Sides

**The case FOR capping VCN at 3 contacts:**

1. **Diminishing returns are real.** Contact 10 converts at 18% the rate of contact 1. The yield curve is unambiguous.
2. **Opportunity cost of the experimentation slot.** Every VCN deployment occupies a slot in the A/B test framework. At 15.8M deployments, VCN consumes the majority of the program's experimentation capacity. Capping frees slots for higher-value campaigns.
3. **SRM investigation cost.** VCN has persistent SRM warnings (p=0.0000) across all quarters. High-volume, low-conversion campaigns make SRM harder to diagnose — the signal-to-noise ratio is terrible. Reducing volume may clean up the experiment.
4. **Analytical noise.** VCN's massive volume distorts every portfolio-level metric. Average success rate, average contacts per client, transition matrices — all are dominated by VCN. Capping would let the portfolio metrics reflect the other 5 campaigns.
5. **Even free channels have hidden costs.** App real estate is finite. A VCN banner displaces other potential content (product offers, service updates, financial insights). The opportunity cost is not zero — it's just not measured.

**The case AGAINST capping VCN:**

1. **Zero marginal cost.** A mobile banking notification costs nothing to send. There is no email infrastructure cost, no print cost, no postage. The economics of "free" change the ROI calculus entirely — even 0.23% conversion on a free channel is pure incremental value.
2. **Minimal client annoyance.** This is a passive banner inside the app. The client has to open mobile banking to see it. It doesn't push-notify. It doesn't email. It doesn't block navigation. Users expect notifications in their banking app. The intrusiveness argument that applies to email does not apply here.
3. **Still converting.** At contact 10, VCN still converts at 0.23%. That's not zero. Over 1.5M clients receiving contacts 4-14, even 0.23% yields thousands of incremental acquisitions. The product owner's argument — "we're still getting clients for free" — is mathematically valid.
4. **No evidence of negative brand impact.** Unlike email (where over-sending drives unsubscribes), there is no unsubscribe mechanism for in-app banners. We have no data showing that repeated VCN exposure causes app avoidance or negative sentiment. The harm is hypothetical.
5. **Mobile notifications are normalized.** Banking apps routinely show promotional banners. Clients are habituated to this. A VCN banner is not qualitatively different from a credit card offer or savings rate promotion.

**The resolution:** The data supports a cap, but a softer one than the original recommendation. A cap at 5 (not 3) may be the pragmatic compromise — it captures 74% of the diminishing returns while acknowledging the low-cost, low-intrusiveness channel. The stronger argument is not "stop VCN" but "redirect the analytical and experimentation resources VCN consumes toward scaling VAW and VDT." See Section 5 for the revised recommendation.

### 1d. VUT vs VAW — Why One Works and the Other Doesn't

This is the most concrete, actionable finding in the entire analysis. Two campaigns target the same outcome (wallet provisioning) with a 130x efficiency gap. The targeting differences explain everything.

| Dimension | VUT (Doesn't Work) | VAW (Works Great) |
|-----------|-------------------|-------------------|
| **Outcome** | Wallet provisioning | Wallet provisioning |
| **Lift** | +0.02pp (p=0.91) | +2.62pp (p<0.001) |
| **Channel** | EM (email 100%) | IM (in-app 100%) |
| **Recency filter** | 1 VVD txn in last **30 days** | 1 VVD purchase in last **6 days** |
| **Platform** | All platforms | **iOS only** |
| **Already-provisioned exclusion** | Not explicit | **Explicitly excludes** clients who already added to wallet |
| **Mobile engagement** | No requirement | **Must have 1+ login in last 30 days** |
| **Foreign currency** | Must have taken >$100K foreign currency in 2 years | No such filter |
| **Segment** | Standard marketing clients | Active VVD users |

**Five targeting differences that explain the gap:**

1. **Recency: 6 days vs 30 days.** VAW catches clients when VVD is top-of-mind (just used it this week). VUT's 30-day window includes clients who may have used the card once a month ago and forgotten about it. Recency is the strongest predictor of intent in financial services marketing.

2. **Platform: iOS only vs everyone.** VAW targets iOS exclusively — Apple Pay is deeply integrated, one-tap provisioning via Wallet app. VUT includes Android users where Google Pay provisioning has more friction and lower adoption. By narrowing to iOS, VAW self-selects for the population where the action is easiest to complete.

3. **Already-provisioned exclusion.** VAW explicitly excludes clients who have already added VVD to their mobile wallet. VUT's targeting does not mention this exclusion. This means VUT may be spending a significant portion of its 784K deployments on clients who already have what the campaign is promoting — pure waste.

4. **Channel: In-app vs email.** VAW uses IM (in-app message) — it reaches clients who are already inside the mobile banking app, one tap away from the Wallet provisioning flow. VUT uses email — it reaches clients who may not even have the app open. The channel-to-action friction is dramatically lower for VAW.

5. **Mobile engagement gate.** VAW requires at least 1 app login in the last 30 days. VUT has no mobile engagement requirement. VUT may be emailing clients who never use the app — asking them to add a card to a digital wallet they don't actively use.

**The recommendation is not "fix VUT." It is "discontinue VUT and expand VAW."** VUT's targeting is structurally misaligned with the action it's trying to drive. The fixes needed (tighten recency, filter to iOS, add provisioning exclusion, switch to in-app channel) would turn VUT into VAW. Just use VAW.

**Expansion potential:** VAW currently reaches 412K clients (2.1% of deployments). VUT reaches 784K. If even half of VUT's audience meets VAW's tighter targeting criteria, and VAW's +2.62pp lift holds, that's an additional ~10K incremental provisioning clients — nearly as many as VCN produces across 15.8M deployments.

### 1e. There Is No Designed Funnel

The transition matrix reveals what should be a funnel but isn't:

**Funnel stages (conceptual):**
- Stage 1 -- Acquisition: VCN, VDA (get the card)
- Stage 2 -- Activation: VDT (first use)
- Stage 3 -- Usage: VUI (ongoing transactions)
- Stage 4 -- Provisioning: VUT, VAW (add to digital wallet)

**What actually happens:**

| Transition | Volume | % of All Transitions | Classification |
|-----------|--------|---------------------|---------------|
| VCN->VCN | 13.1M | 80.9% | Self-loop (repetition) |
| VCN->VDA | 848K | 5.2% | Stage overlap (same stage) |
| VDA->VCN | 787K | 4.9% | Stage overlap (same stage) |
| VDT->VDT | 589K | 3.6% | Self-loop (repetition) |
| VDA->VDA | 300K | 1.8% | Self-loop (seasonal) |
| VUI->VUI | 234K | 1.4% | Self-loop (suspicious) |
| VUT->VAW | 151K | 0.9% | Stage overlap (same stage) |

**Only 0.9% of transitions cross funnel stages.** The program is almost entirely self-loops and same-stage lateral movement. There is no evidence of clients being guided from acquisition through activation to usage to provisioning. Each campaign operates as if the others don't exist.

Cross-stage transitions that DO exist are small:

| Cross-Stage Flow | Transitions | Direction |
|-----------------|-----------|-----------|
| VCN->VDT (Acq->Act) | 8,962 | Forward |
| VCN->VUI (Acq->Use) | 38,150 | Forward (skips activation) |
| VDT->VUT (Act->Prov) | 16,532 | Forward |
| VCN->VAW (Acq->Prov) | 10,384 | Forward (skips 2 stages) |
| VUT->VDT (Prov->Act) | 15,428 | **Backward** |
| VUT->VCN (Prov->Acq) | 9,878 | **Backward** |

The backward transitions (provisioning clients being sent acquisition campaigns) are waste by definition — these clients already have and use the card.

### 1f. Same-Day Deployments Are a System Bug

752K transitions (4.6% of all) happen on the same day across 221K clients. The biggest contributor:

- **VUI->VUI: 233K transitions, avg gap 0.0 days.** A client gets the same usage trigger multiple times on the same day. This is not a strategy decision — it's a system behavior (possibly triggered by multiple qualifying transactions in one day). No business case for sending a "use your card" message multiple times on the same day. And these are **email** triggers — each duplicate is another email hitting the inbox.

- **VDT->VDT: 589K transitions, avg gap 2.3 days.** This is the 7-day + 14-day reminder cadence. Expected, but the volume (111K clients getting an average of 5.3 re-triggers) suggests the reminder windows may be too aggressive. VDT uses email and digital outbound — both intrusive channels.

### 1g. Two Campaigns Should Not Exist in Their Current Form

**VUT (Tokenization):** +0.02pp lift, p=0.9094, single contact per client, 784K clients touched. Zero measurable effect. v1 showed negative ROI (-18X). Two independent analyses agree. The targeting analysis (Section 1d) explains why: wrong recency window, wrong platform mix, wrong channel, no provisioning exclusion. This campaign is not "broken" — it's structurally misaligned with the action it's trying to drive.

**VUI (Usage):** +0.78pp lift, not significant overall, no individual cohort reaches significance. The 32.89% control baseline means clients are already using their cards without prompting. The campaign is targeting an action that happens organically at high rates. Uses email (EM) and email+in-app (EM_IM) — the two most intrusive channel types in the program — for a campaign with no measurable effect.

---

## 2. The Gaps

### 2a. What We Cannot Tell

| Gap | Why It Matters | What We'd Need |
|-----|---------------|---------------|
| **"Already succeeded" waste** | Clients who acquired a card may keep getting VCN. We can compute this from result_df but haven't yet. | PySpark: join result_df SUCCESS=1 rows against subsequent deployments of the same campaign. Quantify volume. |
| **Revenue per deployment** | We know incremental clients but not incremental revenue per contact. Cannot compute ROI without cost data. | Campaign cost data from marketing ops. Even a per-deployment cost estimate would unlock ROMI. |
| **Demographic segmentation** | v1 showed 5x age-based variance. v2 has no UCP data (blocked on data engineering). Cannot target by segment. | UCP table access -- blocked. |
| **Cross-campaign success attribution** | A client who gets VCN (acquisition) then VDT (activation) and succeeds on VDT — did VCN contribute? We attribute to VDT only. | Attribution modeling is out of scope, but it's an honest caveat. |
| **Suppression list overlap** | We don't know if campaigns check each other's suppression lists. The overlap patterns suggest they don't. | Campaign platform configuration data. |
| **Email engagement correlation** | Run 3 collected email data (2.2M rows) but we haven't correlated open/click rates with success outcomes per campaign. | Analysis of email engagement data already in result_df. |
| **VCN brand perception impact** | We don't know if repeated MB banners cause app avoidance or reduced engagement. The argument that MB is harmless is plausible but unproven. | App engagement metrics (session frequency, duration) correlated with VCN exposure count. |

### 2b. Assumptions We Are Making

1. **Funnel stages are sequential.** We assume Acquisition -> Activation -> Usage -> Provisioning is the intended journey. No documented journey exists — we're inferring from campaign names and goals.

2. **Self-loops are waste past a threshold — but the threshold depends on channel.** MB self-loops (VCN) have a higher acceptable threshold than EM self-loops (VDT, VUI) because the cost and intrusiveness are lower.

3. **Same-campaign contacts to successes are waste.** We assume a client with SUCCESS=1 shouldn't receive the same campaign again. This may not hold for VUI (ongoing usage is valid to re-encourage) but definitely holds for VCN/VDA (you can't acquire a card twice).

4. **The 4-5 contact bucket's high success rate is compositional.** We attribute it to VDT client clustering, not to a genuine "sweet spot" at 4-5 contacts. This is testable — break the bucket by campaign — but we haven't done it yet.

5. **Control baselines are unbiased (except where SRM flags exist).** VCN and VDA both have SRM warnings. Their lift numbers should be interpreted with caution.

6. **VAW's targeting explains its performance, not just its audience quality.** It's possible VAW simply targets higher-intent clients (recent, mobile-active, iOS) and would convert even without the campaign. The +2.62pp lift over control argues against this — but the control group has the same targeting criteria, so the lift is real.

---

## 3. Key Findings — Summary

### 3a. Over-contacting: YES, but Channel-Dependent

- 770K clients (22.6%) receive 11+ contacts at 2.41% success
- VCN specifically: contact 1 converts at 1.25%, contact 10 at 0.23%
- Median VCN client gets 7 contacts; P90 gets 14
- **But VCN is a free, passive MB banner** — the cost-per-contact is effectively zero and the intrusiveness is minimal
- **Email over-contacting is the bigger concern:** VDT sends 5.3 re-triggers per client via email/DO. VUI duplicates on the same day via email. These channels carry unsubscribe risk and inbox fatigue.
- **Quantified waste (revised)**: If VCN were capped at 5 contacts per client, ~9M deployments eliminated while preserving the low-cost tail. If email campaigns (VDT, VUI) enforce same-day dedup and tighter cadence, ~800K additional deployments eliminated with higher per-unit waste reduction.

### 3b. Funnel Flow: WEAK

- 99.1% of transitions are self-loops or same-stage lateral moves
- Only 0.9% cross funnel stages
- Some backward flow exists (provisioning clients receiving acquisition campaigns)
- VUT->VAW (151K transitions, 63.9d gap) is the strongest cross-campaign flow, but it's within the same stage (both are provisioning)

### 3c. Waste: YES, Multiple Types — Channel-Weighted

| Waste Type | Volume | Clients | Channel | Severity |
|-----------|--------|---------|---------|----------|
| Same-day VUI email duplicates | 233K transitions | 116K | EM | **High** — duplicate emails, same day |
| VUT (no measurable effect, wrong channel) | 784K deployments | 785K | EM | **High** — emails with zero lift |
| Backward funnel transitions | ~25K transitions | ~25K | Mixed | **High** — definitionally wrong |
| VCN contacts 6+ (severe diminishing returns) | ~9M deployments | ~1.2M | MB | **Low** — free, passive banner |
| Overlapping deployments (<25d gap) | 2.15M transitions | 727K | Mixed | **Medium** — depends on channel |
| VUI (no significant effect) | 393K deployments | 159K | EM/EM_IM | **High** — intrusive channel, no effect |
| VDT re-triggers beyond day 14 | ~200K deployments | ~50K | EM/DO | **Medium** — cadence issue |

**The highest-severity waste is email-channel waste (VUT, VUI same-day, VUI overall).** VCN's volume is large but the per-unit waste severity is low because of the MB channel.

### 3d. Diminishing Returns: YES (VCN is clearest, but channel context matters)

VCN drops 82% from contact 1 to contact 10 — but each contact costs nothing and annoys nobody. VDA drops 28% from contact 1 to contact 2 (only 2 contacts exist). VDT is flat through contact 4 then drops — suggesting the 7-day/14-day reminder cadence works up to a point. VUI is flat (but the campaign itself doesn't work, so this is moot).

### 3e. VUT vs VAW: The Targeting Story

The 130x efficiency gap between VUT (+0.02pp) and VAW (+2.62pp) is fully explained by five targeting differences: recency (6 days vs 30), platform (iOS only vs all), provisioning exclusion (yes vs no), channel (in-app vs email), and mobile engagement gate (yes vs no). This is the most actionable finding — it doesn't require an orchestration engine, just better campaign design. See Section 1d for full analysis.

---

## 4. The "Already Succeeded" Waste Dimension

This is the most powerful uncomputed metric: **clients who already achieved their campaign's goal but keep getting contacted by the same campaign.**

### What It Would Show

For each campaign, count clients where:
1. `SUCCESS = 1` on deployment N
2. Deployment N+1 (or later) exists for the same campaign and same client

This is pure waste for acquisition campaigns (VCN, VDA) — you cannot acquire a Visa Debit card twice. For activation (VDT), it's waste once the card is activated. For usage (VUI) and provisioning (VUT, VAW), it's more nuanced — repeat usage is the goal, repeat provisioning is meaningless.

### Expected Scale

Given VCN's structure (median 7 contacts per client, 0.47% success rate), the vast majority of successful VCN clients will have subsequent VCN deployments. A client who succeeds on contact 2 will receive contacts 3 through 14 with zero chance of re-acquisition.

**Rough estimate:** 73,530 VCN successes (TG4). If the average successful client succeeds on contact ~3 (early contacts have higher rates), they'd have ~4-11 subsequent wasted deployments. That's potentially 300K-800K provably wasted VCN deployments — contacts to clients who already have the product the campaign is promoting.

For VDT: 125,247 successes from 723K deployments. VDT clients average 5.5 contacts. A client who activates on contact 1 gets ~4 more reminder triggers. That's potentially 400K+ wasted VDT deployments — and these are **email** deployments. Post-success VDT emails are not just wasted capacity, they're wasted inbox real estate with unsubscribe risk.

**Channel-weighted severity:**
- Post-success VCN: Low severity per unit (MB banner, free). But symbolically bad — "why are you telling me to get a card I already have?"
- Post-success VDT: High severity per unit (email/DO). Real unsubscribe risk on messages that are now irrelevant.
- Post-success VUT: High severity (email). But volume is low (single contact per client).

### PySpark Code Required

```python
# For each campaign: find clients who succeeded, then count their subsequent deployments

w = Window.partitionBy("CLNT_NO", "MNE").orderBy("TREATMT_STRT_DT")

tagged = result_df.filter(col("TST_GRP_CD") == "TG4") \
    .withColumn("first_success_dt",
        F.min(F.when(col("SUCCESS") == 1, col("TREATMT_STRT_DT"))).over(
            Window.partitionBy("CLNT_NO", "MNE").orderBy("TREATMT_STRT_DT")
                  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        ))

post_success = tagged.filter(
    (col("first_success_dt").isNotNull()) &
    (col("TREATMT_STRT_DT") > col("first_success_dt"))
)

post_success.groupBy("MNE").agg(
    countDistinct("CLNT_NO").alias("clients_with_post_success_contacts"),
    count("*").alias("wasted_deployments"),
    F.round(count("*") / countDistinct("CLNT_NO"), 1).alias("avg_wasted_per_client")
).orderBy("wasted_deployments", ascending=False).show()
```

This is the single most impactful number for the presentation: "X deployments were sent to clients who had already succeeded on the same campaign."

---

## 5. Proposed Orchestration Improvements

### Proposal 1: Cap VCN at 5 Contacts Per Client (Revised from 3)

**Evidence:** Contact 1 = 1.25%, Contact 5 = 0.33%, Contact 10 = 0.23%. After contact 5, diminishing returns flatten — the yield curve from 5 to 14 is nearly flat at 0.2-0.3%.

**Why 5, not 3:** VCN is a passive mobile banking banner with zero marginal cost. The standard frequency cap logic (which assumes a cost per contact) doesn't fully apply. A cap at 3 is aggressive given the channel's low intrusiveness. A cap at 5 captures 74% of the diminishing returns while preserving the product owner's legitimate argument that "free contacts still convert."

**Impact:** Eliminates ~9M of 15.8M VCN deployments (57% reduction). First 5 contacts capture the vast majority of conversions. Frees experimentation capacity and cleans up SRM analysis.

**The stronger argument for the cap is not cost — it's experimentation hygiene.** VCN's 15.8M deployments dominate every portfolio metric. Capping at 5 reduces noise in cross-campaign analysis and makes the A/B test framework more interpretable. The SRM investigation alone has consumed analyst hours that exceed the value of contacts 6-14.

**Implementation:** Add a frequency cap rule in the campaign platform. Suppress VCN for any client with 5+ prior VCN deployments in a rolling 12-month window.

### Proposal 2: Discontinue VUT, Redirect to VAW

**Evidence:** +0.02pp lift (p=0.91), single contact per client, 784K clients with zero measurable effect. v1 showed negative ROI (-18X). Two independent analyses agree. The targeting analysis (Section 1d) explains the root cause: wrong recency window, wrong platform, wrong channel, no provisioning exclusion.

**Why redirect, not fix:** The five changes needed to make VUT work (tighten recency to 6 days, filter to iOS, add provisioning exclusion, switch to in-app, add mobile engagement gate) would make VUT identical to VAW. There is no reason to maintain two campaigns for the same outcome when one has proven effectiveness.

**Impact:** Eliminates 784K email deployments (the highest-severity waste type per unit). No incremental clients lost.

**Redirect:** Clients currently eligible for VUT who also meet VAW's tighter targeting criteria (~50% estimated) should be routed to VAW. At VAW's +2.62pp lift, this could yield ~10K additional incremental provisioning clients.

**Key insight for the presentation:** "We have two campaigns trying to get clients to add VVD to their digital wallet. One uses email to reach people who used the card a month ago. The other uses in-app messaging to reach iOS users who used the card this week. One has zero effect. The other is our best performer. The difference is targeting precision and channel fit."

### Proposal 3: Fix VUI Same-Day Email Duplicates

**Evidence:** 233K VUI->VUI transitions at 0.0-day average gap. 116K clients receiving the same usage trigger multiple times on the same day. These are email triggers — each duplicate is another inbox hit.

**Impact:** Eliminates ~234K redundant email deployments. No success rate impact (duplicate same-day messages don't drive additional transactions). Reduces unsubscribe risk.

**Implementation:** Add a same-day deduplication rule. If a client already received VUI today, suppress the next trigger until tomorrow.

### Proposal 4: Suppress Post-Success Same-Campaign Contacts

**Evidence:** To be quantified (see Section 4), but structurally guaranteed to exist for VCN/VDA (can't acquire twice) and VDT (can't activate twice).

**Impact:** Estimated 300K-800K wasted VCN deployments (low severity per unit — MB), 400K+ wasted VDT deployments (high severity — email).

**Channel-aware prioritization:** Implement post-success suppression for VDT first (email channel, higher waste severity per unit), then VCN. The VDT suppression alone eliminates ~400K emails to clients who already activated their card.

**Implementation:** Real-time (or daily batch) success check before deployment. If client already has SUCCESS=1 for this campaign's metric, suppress.

### Proposal 5: Evaluate VUI for Pause/Redesign

**Evidence:** No cohort reaches statistical significance. +0.78pp on a 32.89% baseline. The campaign targets an action that already happens organically at a high rate. Uses email and email+in-app — the two most intrusive channels — for zero measurable effect.

**Impact:** Frees 393K email/EM_IM deployments. If VUI truly has no effect, no incremental value is lost — but the inbox fatigue and unsubscribe risk it creates are real costs.

**Caveat:** VUI might have a real but small effect that our sample can't detect at 95/5 split. Before discontinuing, consider running a 50/50 test for one quarter to get statistical power.

### Proposal 6: Build a Cross-Campaign Suppression Layer

**Evidence:** Backward funnel transitions exist (VUT->VCN = 9.9K, sending acquisition campaigns to clients who already have and use the card in a digital wallet). No campaign checks another campaign's success outcomes.

**Implementation:** Shared suppression list: if a client has `ACQUISITION_SUCCESS = 1`, suppress all acquisition campaigns (VCN, VDA). If `ACTIVATION_SUCCESS = 1`, suppress VDT. If `PROVISIONING_SUCCESS = 1`, suppress VUT and VAW.

### Proposal 7: Channel-Aware Orchestration Rules (New)

Rather than a single frequency cap, implement channel-specific policies:

| Channel | Max Frequency | Cool-Down | Rationale |
|---------|--------------|-----------|-----------|
| **MB** (VCN) | 5 per rolling 12 months | None needed | Low intrusiveness, zero cost. Cap exists for experimentation hygiene, not client experience. |
| **IM** (VAW, VDA) | 3 per rolling 90 days | 14 days between same-campaign contacts | In-app is low-friction but should not feel spammy within the app. |
| **EM** (VDT, VUT, VUI) | 2 per rolling 90 days | 30 days between same-campaign contacts | Email has the highest cost per unit: unsubscribe risk, inbox competition, send costs. Tightest cap. |
| **EM_IM** (VDA, VUI combo) | 1 per rolling 90 days | 60 days | Dual-channel is the most intrusive. Strictest limits. |
| **DO** (VDT partial) | 2 per rolling 90 days | 14 days | Push notifications are interruptive. Match email cadence. |

### Summary of Volume Impact (Revised, Channel-Weighted)

| Proposal | Deployments Eliminated | Channel | Severity per Unit |
|----------|----------------------|---------|-------------------|
| Cap VCN at 5 | ~9M | MB | Low |
| Discontinue VUT | 784K | EM | **High** |
| Fix VUI same-day | ~234K | EM | **High** |
| Post-success suppression (VDT priority) | ~400K-1.2M (est.) | EM/MB | **High** (VDT) / Low (VCN) |
| Pause VUI | 393K | EM/EM_IM | **High** |
| **Combined** | **~10.8M-11.6M** | Mixed | -- |

**Approximately half of all deployments could be eliminated.** But the more important metric is that **~1.4M high-severity email deployments would be eliminated** (VUT + VUI same-day + VUI pause + VDT post-success). These carry real costs: unsubscribe risk, inbox fatigue, brand perception damage. The VCN MB volume reduction (~9M) is a bonus that improves analytical clarity but has lower per-unit impact on client experience.

---

## 6. What Would Make This Presentation-Ready

### 6a. Must-Have Analysis (run on Lumina)

| Analysis | What It Produces | PySpark Complexity | Priority |
|---------|-----------------|-------------------|----------|
| **Already-succeeded waste** | Count of deployments sent after client's first success, per campaign, split by channel | Medium — window function + filter | **P0** |
| **Frequency bucket by campaign** | Break the 4-5 bucket into per-campaign views to prove the compositional effect | Low — add MNE to groupBy | P1 |
| **Funnel transition classification** | Tag each transition as Forward / Lateral / Backward / Self-loop | Medium — map MNE to stage, compare consecutive | P1 |
| **VUT targeting overlap with VAW** | How many VUT clients also meet VAW criteria? (Requires targeting table access) | Low if data available | P2 |

### 6b. Must-Have Visuals (for slides)

| Visual | Format | What It Shows |
|--------|--------|------------|
| **Volume vs Efficiency scatter** | Bubble chart: x=deployments, y=success rate, size=incremental clients, **color=channel** | The structural inversion, now channel-coded. VCN (green/MB) is a giant low bubble, VUT (red/EM) is medium-sized with near-zero lift, VAW (blue/IM) is small and high. |
| **VCN diminishing returns curve** | Line chart: x=contact number, y=success rate | The 82% drop from contact 1 to 10. Mark the proposed cap at 5. Annotate: "Zero-cost MB channel — cap is for analytical hygiene, not cost savings." |
| **VUT vs VAW comparison table** | Side-by-side targeting comparison (from Section 1d) | The 5 differences that explain the 130x gap. This is the slide that drives the "discontinue VUT" recommendation. |
| **Waste waterfall (channel-weighted)** | Stacked bar: email waste in red, MB waste in gray | Separates high-severity (email) from low-severity (MB) waste. Headline: "1.4M email deployments with no effect." |
| **Transition type breakdown** | Stacked bar: Self-loop / Lateral / Forward / Backward as % of transitions | Shows 99.1% is non-progression. |

### 6c. Nice-to-Have (if time allows)

| Analysis | Value |
|---------|-------|
| Email open/click correlation with success | Prove that engaged email recipients convert better -> supports channel optimization and VUT's email problem |
| VDT reminder cadence analysis | Show that 7-day reminder works but 14-day+ doesn't -> support reducing VDT re-trigger window |
| VAW scaling simulation | If VAW were extended to VUT's eligible audience (iOS subset), project incremental clients using current lift |
| VCN app engagement correlation | Do clients who see 14 VCN banners open the app less frequently over time? Would strengthen/weaken the cap argument. |

### 6d. Slide Deck Structure (revised)

1. **The Portfolio** (1 slide) — 6 campaigns, their goals, funnel stages, **channel mapping**
2. **The Problem** (1 slide) — Volume-efficiency scatter (channel-colored). "80% of contacts go to our lowest-converting campaign — but it's a free channel."
3. **The Channel Story** (1 slide) — Intrusiveness hierarchy. "Not all contacts are equal. 1.4M emails with no effect vs 15.8M free banners."
4. **The VUT vs VAW Story** (1 slide) — Side-by-side targeting. "Same outcome, 130x efficiency gap, explained by 5 targeting differences." This is the hero slide.
5. **The Evidence** (1 slide) — Waste waterfall (channel-weighted). Post-success numbers. Transition matrix summary.
6. **The Recommendations** (1 slide) — Discontinue VUT, redirect to VAW. Cap VCN at 5. Post-success suppression for VDT. Channel-aware frequency rules.

Six slides, 10 minutes. The VUT vs VAW comparison is the new centerpiece — it's concrete, actionable, and doesn't require a technology project. The VCN story is nuanced (free channel, but cap for hygiene). The email waste story carries the urgency.

---

## 7. Sankey/Flow Visualization Concept

### What It Would Look Like

**Nodes (left to right):**
- Column 1: Entry campaigns (first campaign a client receives)
- Column 2: Second campaign
- Column 3: Third campaign
- (etc., up to 4-5 columns)

**Flows:** Width proportional to number of clients taking each path.

### What It Would Show

The dominant visual would be a massive VCN->VCN->VCN->VCN pipe — a thick band looping back on itself that dwarfs everything else. This is both the finding and the problem: the "journey" is a treadmill.

Thin threads would show clients who do progress: VCN->VDT (8.9K), VDT->VUI (rare), VCN->VAW (10.4K). These would be barely visible against the VCN self-loop mass.

### Is It Worth Building?

**Probably not for the 10-minute presentation.** The Sankey would be visually dramatic but hard to read at slide scale. The VCN self-loop would dominate so completely that cross-campaign flows would be invisible without logarithmic scaling or filtering out self-loops.

A better alternative for the presentation: **a simplified flow diagram** (not data-driven Sankey) showing the 4 funnel stages as boxes, with arrow widths representing actual transition volumes. Label the VCN->VCN self-loop as "13.1M" and the cross-stage transitions as their actual small numbers. The contrast tells the story better than a Sankey would.

If you DO build the Sankey, filter out self-loops and show only cross-campaign transitions. That would surface the VCN<->VDA back-and-forth (1.6M combined), the VUT->VAW pipeline (151K), and the small cross-stage trickle. Use Plotly's `go.Sankey` — it works in the Lumina Jupyter environment with CDN mode.

### Implementation Estimate

- Filtered Sankey (no self-loops): ~30 lines of PySpark for the transition aggregation, ~40 lines of Plotly for the chart. 1 hour.
- Full Sankey with self-loops: Same code but visually useless without logarithmic scaling hacks.
- Simplified flow diagram: Manual in PowerPoint. 15 minutes. Better for the audience.

**Recommendation:** Skip the Sankey. Use the transition matrix numbers in a clean PowerPoint flow diagram. Save the Sankey for an appendix or follow-up deep-dive if stakeholders want to explore.

---

## 8. The Narrative Arc (10-Minute Presentation, Revised)

**Opening (1 min):** "We run 6 VVD campaigns across 4 channels. Together they touch 3.56 million clients with 20.8 million deployments. But they don't talk to each other — and they don't all use the right channel."

**The portfolio (2 min):** Show the volume-efficiency scatter, color-coded by channel. "80% of our contacts are VCN mobile banners — free, passive, non-intrusive. They convert at 0.47%. Meanwhile, VAW uses in-app messaging and converts at 5.87% with 38x fewer contacts." Establish that not all contacts are equal.

**The VUT vs VAW story (3 min):** This is the hero moment. Show the side-by-side targeting table. "Two campaigns, same goal: get clients to add VVD to their digital wallet. One emails people who used the card a month ago. The other sends an in-app message to iOS users who used the card this week. One has zero effect. The other is our best performer. The difference is five targeting decisions." Walk through: recency, platform, provisioning exclusion, channel, mobile engagement. This is concrete, specific, and immediately actionable.

**The waste (2 min):** Show the channel-weighted waste waterfall. "1.4 million emails with no measurable effect — VUT, VUI duplicates, VUI itself. These carry real costs: unsubscribe risk, inbox fatigue. And we're still emailing clients who already activated their card." Drop the already-succeeded number here.

**The fix (1.5 min):** "Three immediate actions, no technology project needed. One: discontinue VUT, redirect those clients to VAW — same goal, proven channel. Two: post-success suppression for VDT — stop emailing clients who already activated. Three: channel-aware frequency rules — tighter caps on email, looser on in-app banners."

**The ask (0.5 min):** "Can we pilot the VUT-to-VAW redirect and VDT post-success suppression for Q2? We'll measure incremental clients per deployment as the success metric."

---

## Appendix: Raw Numbers for Reference

### Total Portfolio

- **20,030,733** deployments (before email expansion)
- **3,563,102** unique clients
- **5.62** average deployments per client
- **16,232,112** transitions between consecutive deployments
- Date range: 2025-01-02 to 2026-03-06

### Incremental Clients (statistically significant campaigns only)

| Campaign | Incremental Clients | Cost per Incremental (deployments) | Channel |
|----------|-------------------|----------------------------------|---------|
| VAW | 10,816 | 38.1 deployments per incremental client | IM |
| VDT | 6,138 | 117.9 deployments per incremental client | EM/DO |
| VCN | 3,591 | 4,403 deployments per incremental client | MB |
| VDA | 2,793 | 540.8 deployments per incremental client | EM_IM/IM |
| **Total** | **23,338** | **858.4 avg** | |

VCN requires 4,403 deployments per incremental client. VAW requires 38. That's a 116x efficiency gap. But VCN's deployments cost nothing (MB) while VDT's and VUT's deployments carry real costs (email). **The efficiency gap is real, but the economic gap depends on channel cost.**

### Channel Distribution of Deployments

| Channel | Deployments | % of Total | Campaigns | Intrusiveness |
|---------|------------|-----------|-----------|---------------|
| MB | ~15.8M | 76% | VCN | Lowest |
| IM | ~850K | 4% | VAW, VDA (partial) | Low |
| EM | ~3.2M | 15% | VDT, VUT, VUI (partial) | Medium |
| EM_IM | ~900K | 4% | VDA (partial), VUI (partial) | High |
| DO | ~250K | 1% | VDT (partial) | Medium-High |

**76% of all deployments are the lowest-intrusiveness channel (MB). 15% are email. 4% are dual-channel (EM_IM).** The email and EM_IM segments — though smaller — are where the highest-severity waste concentrates.
