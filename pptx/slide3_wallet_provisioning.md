# Same Goal, Opposite Results — VAW's Targeting Precision Exposes VUT's Design Flaws

---

## VUT — Tokenization
**One-time batch (Jun 2025)** | Email | 95/5 split | 90-day window

Overall lift: +0.02pp (0.1% relative) — NOT significant (p=0.91). Action 13.8% vs Control 13.8%. Single cohort, single batch. Campaign period: Jun 9 -- Sep 9, 2025.

Scale: ~785K action clients, ~41K control. One deployment per client.

### Key Insights

- **Zero measurable effect.** v1 showed negative ROI (-18x). v2 confirms: still zero.
- Originally 6 report groups — 5 removed during config revisions (all student segments and all IM channels eliminated). Only PVUTAG01 (AoM, EM) remains.
- **Known SAS bug**: success definition references wrong alias (Success02 instead of Success03), and uses a -30 day pre-treatment window that counts activity before treatment start.
- Email to broad audience with no platform restriction, no recency filter, no provisioning exclusion.
- **NIBT**: $0 — no significance, no revenue claim.
- **Recommendation**: **DISCONTINUE.** The five design changes needed to make VUT work would transform it into VAW. Redirect budget to VAW expansion.

---

## VAW — Add To Wallet
**Monthly** | In-app (iOS only) | 80/20 split | 30-day window

Overall lift: +2.62pp (80.7% relative) — significant at 99.9% across ALL cohorts. All 10 cohorts significant (Apr 2025 -- Feb 2026). Lift range: +1.16pp to +3.21pp.

Scale: 10 monthly cohorts. ~402K unique action clients, ~100K control. Single contact per client.

**Best deployment efficiency in portfolio**: 38 deployments per incremental client (VCN = 4,403, VDA = 541). 10,816 incremental clients — highest of all 6 campaigns.

### Cohort Consistency

| Cohort | Action Rate | Lift | Sig |
|--|--|--|--|
| 2025-06 | 5.95% | +3.21pp | SS |
| 2025-07 | -- | +2.95pp | SS |
| 2025-08 | -- | +2.96pp | SS |
| 2025-09 | -- | +1.81pp | SS |
| 2025-10 | -- | +2.00pp | SS |
| 2025-11 | -- | +1.79pp | SS |
| 2025-12 | 5.14% | +1.78pp | SS |
| 2026-01 | 5.19% | +1.29pp | SS |
| 2026-02 | 5.01% | +1.76pp | SS |

### Why VAW Works Where VUT Fails

Five design advantages that explain the divergence:

1. **Recency**: 6-day activation window (VUT: 30 days). Catches clients when VVD is top-of-mind.
2. **Platform**: iOS only — Apple Pay 1-tap provisioning (VUT: all platforms, higher friction).
3. **Channel**: In-app message — contextual, client already in banking app (VUT: email, out of context).
4. **Mobile engagement gate**: Must have logged in last 30 days (VUT: no filter, emails app-never users).
5. **Already-provisioned exclusion**: No waste on clients who already have wallet (VUT: no exclusion).

### Key Insights

- **Single-touch campaign**: Avg contacts = 1.0. No repeat targeting. All 10,816 incremental clients from one well-targeted message.
- **Post-provisioning spend lift**: +$70/client per 90-day window (Difference-in-Differences). Validates revenue case beyond the $78.21 proxy.
- **NIBT**: $846K (10,816 x $78.21) — but heavily footnoted. $78.21 is an acquisition proxy. Wallet provisioning is not new client acquisition. Better model: post-provisioning spend differential.
- **Recommendation**: **EXPAND.** Consider Android equivalent (Google Pay). Redirect VUT's 785K email budget here. This is the model for targeted campaigns.

---

## Design Notes

1. VUT and VAW target the same outcome (wallet provisioning) but differ fundamentally in every design dimension: channel, platform, recency, engagement filtering, and already-provisioned exclusion.
2. VUI to VUT resting rule: VUT excludes clients targeted by VUI in the last 90 days. This is the only explicit cross-campaign dependency.
3. VAW's 80/20 split (config doc says 5% control, actual deployment is 80/20) provides substantially more statistical power than VUT's 95/5.

---

## Presenter Notes

### The VUT vs VAW Story (if asked to elaborate)
"We have two campaigns trying to get clients to add their VVD card to their digital wallet. One emails people who used the card sometime in the past month, to all platforms. The other sends an in-app message to iOS users who used the card this week and actively use mobile banking. One has zero effect. The other delivers our best efficiency in the portfolio. The difference is targeting precision and channel fit."

### VAW NIBT Caveat
If challenged on the $846K: "The $78.21 model was designed for new customer acquisition. Wallet provisioning is feature adoption, not customer acquisition. However, our deep analysis shows provisioned clients spend $70 more per quarter than non-provisioned (DiD-controlled). Over a 1-year horizon at ~15% margin, that's approximately $42/client/year in incremental profit. The revenue case is real — it's just better measured through spend lift than the acquisition proxy."

### VUT Discontinuation — How to Frame
Don't frame as failure. Frame as: "The data tells us VUT's design doesn't match the action we're asking clients to take. VAW proves the same goal is highly achievable with better targeting. Rather than fixing VUT — which would require changing 5 core design elements to match VAW — we recommend redirecting resources to VAW expansion."
