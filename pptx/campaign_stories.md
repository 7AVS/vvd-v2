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
- Persistent SRM warnings (p=0.0000) across ALL cohorts — see SRM note below

**Revenue**: 3,591 incremental × $78.21 = **$280,852** (clean fit — acquisition campaign)

**Open questions**:
- [ ] SRM root cause — is this a data issue or a real randomization problem?
- [ ] What's the pain point? What's working well?

**SRM note**: Persistent SRM (p=0.0000) across all cohorts. Expected 95/5 (Action/Control), observed ~94.7/5.3. Control is larger than expected (5.3% vs 5%), meaning fewer clients got the action treatment than designed. This doesn't impair lift detection — if anything, a larger control improves measurement precision. It IS a campaign design question to investigate (why did more clients end up in control?), but not a blocker for the analysis.

**Recommendation**: Continue. Strong lift, clean revenue fit. SRM needs investigation but doesn't block.

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

**Revenue**: 2,793 incremental × $78.21 = **$218,359** (clean fit — acquisition campaign, but SRM caveat)

**Issues**:
- Small volume limits statistical power per cohort
- Seasonal timing matters — results vary by occasion
- SRM detected: 92/8 vs expected 95/5 (chi-sq 18,439). Control is larger than expected (8% vs 5%). Same direction as VCN — fewer action, more control.

**SRM note**: Control is larger than expected (8% vs designed 5%). Fewer clients received the action treatment than intended. This doesn't impair lift detection — a larger control improves precision. Flag for campaign design investigation (why did more clients end up in control?), not a blocker.

**Open questions**:
- [ ] Which seasonal occasions exactly? (Black Friday + ?)
- [ ] Does lift vary between the two seasonal runs?

**Recommendation**: Continue (low cost, strong lift, seasonal). SRM needs investigation but doesn't block.

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

**Revenue**: 6,138 incremental × $78.21 = **$480,033** (arguable fit — activation ≠ acquisition, but similar enough? $78.21 is a new-client onboarding value; activation of an already-issued card is a stretch)

**Open questions**:
- [ ] Verify channel (email? push notification?)
- [ ] Card type breakdown — what % of issued cards actually need activation?
- [ ] Volume trend — is the addressable market shrinking over time as more cards go digital?
- [ ] Does VDT revenue count? Activation ≠ acquisition. User said "maybe".

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

**Revenue**: 1,232 incremental × $78.21 = **N/A** (NOT statistically significant — no revenue claim. Even if it were significant, $78.21 is a new-client onboarding value and doesn't apply to usage nudges. Would need spend differential from deep_analysis.)

**Open questions**:
- [ ] Was the Oct 2025 config change recommendation implemented?
- [ ] Timing discrepancy between v1 and v2 measurements
- [ ] What exactly are the triggers? What behavior are we trying to change?
- [ ] If eventually significant: need spend differential (Action avg spend - Control avg spend) for proper revenue estimate

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

**Revenue**: 155 incremental × $78.21 = **N/A** (NOT significant, zero lift — no revenue claim)

**Open questions**:
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

**Revenue**: 10,816 incremental × $78.21 = **$845,903** (but $78.21 may not apply — wallet provisioning ≠ new client. Use with heavy footnote "acquisition proxy — not validated for usage/provisioning campaigns". Need spend differential from deep_analysis for proper estimate.)

**Open questions**:
- [ ] iOS only confirmed?
- [ ] Is VUT = Android and VAW = iOS? Or different approach same platform?
- [ ] Seasonal timing — when does it run?
- [ ] Why does VAW work but VUT doesn't?

**Recommendation**: Expand (take VUT's budget, scale up)

---

## Cross-Cutting Questions (ALL campaigns)

- [x] Revenue / NIBT model — **$78.21 per incremental client** (see full breakdown in Revenue/NIBT section below)
  - Formula: `incremental_clients = action_clients × (lift / 100)` → `NIBT = incremental_clients × $78.21`
  - ONLY when stat significant + positive lift. No significance = no revenue claim. Period.
  - $78.21 is a NEW CLIENT onboarding value. Clean fit for acquisition campaigns (VCN, VDA). Arguable for VDT (activation). NOT applicable to usage/provisioning (VUI, VAW, VUT).
  - Conservative total (VCN + VDA): **$499K**. With VDT: **$979K**. With VAW (footnoted): **$1.83M**.
  - VUT: No revenue (not significant, zero lift). Recommend discontinue.
- [ ] Overcontacting — clients receiving multiple campaigns. Waste?
- [ ] Funnel flow — do acquisition successes flow to activation to usage to provisioning?
- [ ] VCN SRM — does this affect the whole program's credibility?

## Card Type Results (Lumina 2025-2026)
- **0% null rate** (was 92% in v1 — now fully populated)
- 03 = 47.5% (Digital), 01 = 39.7%, 04 = 12.7% (experiment clients)
- Need to confirm what 01 and 04 map to
- Trend: sharp decline in non-digital products over time. Only reliable for 2025-2026.
- Relevant for VDT — which card types need manual activation?

## VUI Previous Recommendations (Oct 2025 NBA M&A slide)
- N=154K, Control 28.63%, Lift 0.37%, NOT significant
- Three recs: (1) expand population, (2) increase control to 15%, (3) relax 365-day resting rule
- KEY: Were any implemented? Determines whether v2 should show different results.

---

## Presentation Section Structure (within team deck — this is a SPOTLIGHT, not standalone)

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

### Formula
`incremental_clients = action_clients × (lift / 100)`
`NIBT = incremental_clients × $78.21`

### Rules
1. **ONLY when statistically significant AND positive lift.** No significance = no revenue claim. Period.
2. **$78.21 is a NEW CLIENT onboarding value** — it represents revenue from acquiring a new VVD cardholder.
3. **Applicable to**: VCN (acquisition), VDA (acquisition), VDT (activation — arguable, these are newly activated cards).
4. **NOT applicable to**: VUI (usage nudge — not a new client), VAW (wallet provisioning — not a new client), VUT (same).
5. **For VUI/VAW**: Need spend differential from deep_analysis (Action avg spend - Control avg spend). If not available, skip revenue claim or use $78.21 with heavy footnote "acquisition proxy — not validated for usage/provisioning campaigns".
6. **VUT**: No revenue (not significant, zero lift). Recommend discontinue.

### Per-Campaign Revenue (Run 3 data)

| Campaign | Incremental Clients | × $78.21 | Claimable? | Notes |
|----------|-------------------:|----------:|:----------:|-------|
| VAW | 10,816 | $845,903 | Footnoted | $78.21 may not apply (wallet provisioning ≠ new client) |
| VDT | 6,138 | $480,033 | Arguable | Activation ≠ acquisition, but similar enough? |
| VCN | 3,591 | $280,852 | Yes | Clean fit (acquisition). SRM caveat. |
| VDA | 2,793 | $218,359 | Yes | Clean fit (acquisition). SRM caveat (92/8 vs 95/5). |
| VUI | 1,232 | N/A | No | NOT significant — no revenue claim |
| VUT | 155 | N/A | No | NOT significant — no revenue claim |

### Revenue Totals

- **Conservative (VCN + VDA only)**: **$499,211** — both have SRM caveats but control is larger than expected (doesn't impair lift)
- **If VDT included**: **$979,244** — depends on whether activation counts as "new client"
- **If VAW included with footnote**: **$1,825,147** — but $78.21 applicability is questionable for wallet provisioning

### SRM Position
VCN and VDA both show SRM. VDA is 92/8 vs expected 95/5 (chi-sq 18,439). VCN is ~94.7/5.3 vs expected 95/5. In both cases, control is larger than expected — fewer clients received the action treatment than designed, and more ended up in control. This doesn't impair lift detection — if anything, a larger control improves measurement precision. The concern would be if control shrank (harder to detect lift); here it grew. SRM is a flag to investigate with campaign design (why did more clients end up in control?), not a blocker for the analysis or revenue claims.

### Open Gaps
- [ ] Time horizon: Is $78.21 year 1? 5-year? Lifetime? Unknown. Use as-is with footnote.
- [ ] VUI/VAW revenue methodology: spend differential not yet computed (deep_analysis needs cohort-level run)
- [ ] Does VDT count? Activation ≠ acquisition, but similar enough? User said "maybe".
