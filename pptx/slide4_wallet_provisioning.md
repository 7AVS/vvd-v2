# Same Goal, Opposite Results — VAW's Targeting Precision Exposes VUT's Design Flaws

---

## VUT — Tokenization
**One-time batch (Jun 2025)** | Email | 95/5 split | 90-day window

Overall: +0.02pp lift (0.1% relative) — NOT significant (p=0.91). Action rate 13.8% vs Control 13.8%.

### Zero Lift, Confirmed Twice

v1 analysis showed negative ROI (-18x). v2 confirms: still zero measurable effect. Single email blast to ~785K clients in June 2025.

Originally designed with 6 report groups across Student/Non-Student segments and EM/IM channels. During config revisions, 5 of 6 groups were removed — all student segments and all in-app channels eliminated. Only PVUTAG01 (Age of Majority, Email) remains. The removed IM channels may have been the ones that could have worked, given VAW's success with in-app messaging.

A known SAS bug affects the success definition: the `any_wallet` flag references the wrong alias (Success02 instead of Success03), and uses a -30 day pre-treatment window that counts wallet activity before treatment start.

NIBT is not applicable — no significance, no revenue claim.

**Recommendation**: Discontinue. The five design changes needed to make VUT effective would transform it into VAW. Redirect the 785K email deployments to VAW expansion.

---

## VAW — Add To Wallet
**Monthly** | In-app (iOS only) | 80/20 split | 30-day window

Overall: +2.62pp lift (80.7% relative) — significant at 99.9% across all 10 cohorts. Best deployment efficiency in the portfolio.

### Best Efficiency in Portfolio

38 deployments per incremental client. For comparison: VCN requires 4,403 (116x more). 10,816 incremental clients — the highest absolute count of any campaign. Single-touch: every client receives exactly one in-app message.

### Five Design Advantages Over VUT

VAW and VUT target the same outcome (wallet provisioning) but VAW succeeds on every design dimension where VUT fails:

1. **Recency**: 6-day activation window vs VUT's 30 days — catches clients when VVD is top-of-mind
2. **Platform**: iOS only (Apple Pay 1-tap provisioning) vs all platforms with varying friction
3. **Channel**: In-app message (client already in banking app, one tap from wallet) vs email (out of context)
4. **Mobile engagement gate**: Must have logged in within 30 days vs no filter (VUT emails app-never users)
5. **Already-provisioned exclusion**: Explicitly excludes clients who already added to wallet vs no exclusion (VUT wastes budget on already-provisioned)

### Post-Provisioning Spend Lift

Deep analysis (Difference-in-Differences) shows provisioned clients in the Action group spend **+$70 per client per 90-day window** more than the Control equivalent. This validates the revenue case more rigorously than the $78.21 acquisition proxy, which doesn't fit wallet provisioning (these are existing clients adopting a feature, not new customers).

**Recommendation**: Expand VAW. Consider an Android equivalent (Google Pay). This campaign's targeting model — high recency, platform-specific, in-app channel, engagement-gated, waste-excluded — should be the template for future campaign design.

---

## Design Notes

1. VUT and VAW target the same outcome but differ in every design dimension. The performance gap is fully explained by targeting precision and channel fit, not audience quality.
2. VUI → VUT resting rule: VUT excludes clients targeted by VUI in the last 90 days. This is the only cross-campaign dependency.
3. VAW's actual deployment split is 80/20 (config doc says 5% control). The larger control provides substantially more statistical power.
