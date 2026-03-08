# VVD Campaign Story Cards

## Presentation Context
- This is a SECTION within a larger team presentation, NOT a standalone deck
- No title slides — we're a spotlight within the team's deck
- Most teammates present 1 campaign. We have 6. Must be concise.
- Audience: team + leadership. Need: what campaigns do, results, revenue, recommendations.

---

## VCN — Contextual Notification (Acquisition)

**What it does**: Monthly screening of ALL clients who don't have a VVD card. If they qualify within the decision tree → send notification to acquire.
**Cadence**: Monthly batch. Every month, new non-holders enter the funnel.
**Channel**: [VERIFY — notification type]
**Split**: 95/5 (Action/Control)

**Results**:
- Lift: +0.20pp (mock — need real from latest run)
- Significance: *** (p<0.001)
- Volume: Largest campaign (~2.2M action clients)

**Issues**:
- Persistent SRM warnings (p=0.0000) across ALL cohorts — sample ratio is consistently off
- WHY is the sample ratio wrong? Need investigation.

**Open questions**:
- [ ] Revenue / NIBT impact
- [ ] SRM root cause — is this a data issue or a real randomization problem?
- [ ] What's the pain point? What's working well?

**Recommendation**: TBD (depends on SRM + revenue)

---

## VDA — Seasonal Acquisition Campaign

**What it does**: Seasonal batch campaign for VVD card acquisition. Uses seasonal context (Black Friday is ONE occasion, there's another during the year).
**Cadence**: ~2x per year (seasonal, batch)
**Channel**: [VERIFY]
**Split**: 95/5

**Results**:
- Lift: +0.32pp (mock)
- Significance: ***
- Volume: Smallest (~59K action clients)

**Issues**:
- Small volume limits statistical power per cohort
- Seasonal timing matters — results vary by occasion

**Open questions**:
- [ ] Revenue / NIBT
- [ ] Which seasonal occasions exactly? (Black Friday + ?)
- [ ] Does lift vary between the two seasonal runs?

**Recommendation**: Continue (low cost, strong lift, seasonal)

---

## VDT — Activation Trigger

**What it does**: Detects clients with ISSUED but NOT ACTIVATED VVD card. If not activated within 7 days of issuance → send alert/email. If still no action → second reminder 7 days later.
**Cadence**: Triggered (7-day + 14-day reminders)
**Channel**: Email or alert [VERIFY which one]
**Split**: 90/10

**Results**:
- Lift: +4.10pp (mock — strongest lift)
- Significance: ***
- Volume: ~385K action clients

**Issues**:
- 95% of VVD cards are digital and auto-activate at issuance
- Only ~5% need manual activation — shrinking addressable market
- Card_Type matters here — WHICH cards need manual activation?
- Business owners may not have visibility on the card type breakdown

**Key insight**: This is where the Card_Type check (adhoc_card_type_check.py) matters. If we can show that "03" (Digital) auto-activates and only non-digital need this campaign, that's a concrete finding for the business.

**Open questions**:
- [ ] Revenue / NIBT
- [ ] Verify channel (email? push notification?)
- [ ] Card type breakdown — what % of issued cards actually need activation?
- [ ] Volume trend — is the addressable market shrinking over time as more cards go digital?

**Recommendation**: Continue, but flag shrinking TAM. Card type analysis is the value-add here.

---

## VUI — Usage Trigger

**What it does**: Triggers to active VVD cardholders to increase transaction frequency/volume.
**Cadence**: Triggered
**Channel**: [VERIFY]
**Split**: 95/5

**Results (IMPORTANT — historical context)**:
- Last measurement (pre-v2): 0.37pp lift, NOT statistically significant
- Deep dive October 2025: recommended changing campaign configurations
- Current v2 data may show discrepancy — could be timing difference
- NEED TO VERIFY current numbers carefully

**Issues**:
- Historical non-significance is the headline
- Campaign config change was recommended — was it implemented?
- If still not significant: why keep running?
- If newly significant: is it the config change or measurement timing?

**Open questions**:
- [ ] Revenue / NIBT (even small lift × large base could = big dollars)
- [ ] Was the Oct 2025 config change recommendation implemented?
- [ ] Timing discrepancy between v1 and v2 measurements
- [ ] What exactly are the triggers? What behavior are we trying to change?

**Recommendation**: TBD — depends on whether config changed and current significance

---

## VUT — Tokenization / Wallet Provisioning

**What it does**: Triggers to add VVD card to digital wallet (Apple Pay / Google Pay).
**Cadence**: [VERIFY — triggered? seasonal?]
**Channel**: [VERIFY]
**Split**: 95/5
**Target**: Clients already using their VVD cards [VERIFY — is it usage or just active?]

**Results**:
- Lift: -0.30pp (NEGATIVE)
- Significance: NOT significant (p=0.15)
- Control outperforms Action

**Issues**:
- Negative ROI — campaign may be counterproductive
- WHY does this campaign exist? What was the original intent?
- VAW (same goal) works — so the problem is VUT's execution, not the concept

**Open questions**:
- [ ] Revenue / NIBT (expected negative)
- [ ] Why does this campaign exist? What problem was it solving?
- [ ] Is it seasonal? Triggered?
- [ ] Who exactly is targeted — active users? Recent users?
- [ ] Why does VAW work but VUT doesn't? Messaging? Channel? Timing?

**Recommendation**: Discontinue. Redirect budget to VAW.

---

## VAW — Add To Wallet

**What it does**: Similar goal to VUT (wallet provisioning) but different approach.
**Cadence**: Seasonal [VERIFY]
**Channel**: [VERIFY]
**Split**: 80/20
**Target**: Clients already using their VVD cards [VERIFY]
**Note**: iOS only (Apple Pay) [VERIFY — is it really iOS only?]

**Results**:
- Lift: +1.60pp, significant
- Volume: 320K action clients

**Issues**:
- If iOS only, what about Android/Google Pay? Is VUT the Android equivalent?
- 80/20 split gives better statistical power than most campaigns

**Open questions**:
- [ ] Revenue / NIBT
- [ ] iOS only confirmed?
- [ ] Is VUT = Android and VAW = iOS? Or different approach same platform?
- [ ] Seasonal timing — when does it run?
- [ ] Why does VAW work but VUT doesn't?

**Recommendation**: Expand (take VUT's budget, scale up)

---

## Cross-Cutting Questions (ALL campaigns)

- [ ] Revenue / NIBT model — user will provide calculation doc
- [ ] Overcontacting — clients receiving multiple campaigns. Waste?
- [ ] Funnel flow — do acquisition successes flow to activation to usage to provisioning?
- [ ] VCN SRM — does this affect the whole program's credibility?

---

## Presentation Section Structure (within team deck)

**Slide 1**: Campaign Portfolio Overview
- What are the 6 campaigns, funnel map, experiment design table
- Quick visual: which stage of the customer lifecycle each targets

**Slide 2**: Acquisition Results (VCN + VDA)
- Vintage curves (by cohort), KPIs, revenue, recommendation

**Slide 3**: Activation & Usage Results (VDT + VUI)
- Same pattern. VDT card-type insight. VUI significance issue.

**Slide 4**: Wallet Provisioning (VUT + VAW)
- The contrast story: same goal, opposite results
- Discontinue VUT, expand VAW

**Slide 5** (if time): Recommendations Summary
- Continue / Optimize / Expand / Discontinue per campaign
- Revenue summary
- Next steps

---

## Revenue / NIBT

Waiting for user to provide calculation document.
Will be saved to this folder when received.
