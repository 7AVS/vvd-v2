# Both Acquisition Campaigns Deliver Significant Lift — VDA is 8x More Efficient

---

## VCN — Contextual Notification
**Monthly trigger** | MB (mobile push) | 95/5 split | 30-day window

Overall: +0.18pp lift (62.9% relative), 99.9% significance across all 12 cohorts.

### The Over-Contacting Problem

VCN accounts for 80.5% of all portfolio deployments (15.8M) but produces only 3,591 incremental clients — 4,403 deployments per incremental client, the worst efficiency in the portfolio.

Diminishing returns are severe:

| Contact # | Success Rate | Decline from #1 |
|-----------|-------------|-----------------|
| 1 | 1.25% | — |
| 2 | 0.65% | -48% |
| 3 | 0.50% | -60% |
| 5 | 0.33% | -74% |
| 10 | 0.23% | -82% |

Median client receives 7 contacts. P90 = 14. 770K clients sit in the 11+ contact bucket with only 2.4% success rate.

MB (mobile push) is a zero-marginal-cost channel, so the economic waste is muted compared to email. However, brand fatigue and app avoidance risk are unmeasured.

**SRM Warning**: Observed 94.7/5.3 vs designed 95/5 (p=0.000). Randomization may be broken. Lift may be biased in unknown direction.

**NIBT**: $281K (3,591 incremental × $78.21). Clean model fit — these are genuinely new cardholders.

**Recommendation**: Implement contact frequency cap (3-5 contacts max). Investigate SRM root cause. Measure whether lift holds with fewer contacts.

---

## VDA — Seasonal Acquisition
**Seasonal batch (~2x/yr)** | EM+IM / IM (Banner/O&O) | 95/5 split | 90-day window

Overall: +0.31pp lift (29.9% relative), 99.9% significance. Two seasonal waves (Jul 2025, Nov 2025).

### The Channel Test Result

VDA runs a natural two-arm channel test. The result is clear:

| Channel | RPT_GRP_CD | Clients | Lift | Sig |
|---------|-----------|---------|------|-----|
| Email + Banner (EM_IM) | PVDAAG03 | 317,260 | **+0.65pp** | 99.9% |
| Banner only (IM) | PVDAAG04 | 595,599 | +0.14pp | 99.9% |

Email adds 4.6x the lift for seasonal acquisition. The 250K email cap (tenure-prioritized, ≤8 years) limits PVDAAG03 volume — clients who don't qualify for email fall to PVDAAG04.

541 deployments per incremental client — 8x more efficient than VCN.

**SRM Warning**: Observed 92/8 vs designed 95/5 (p=0.000). Worse deviation than VCN. Same direction — more clients in control than designed.

**NIBT**: $218K (2,793 incremental × $78.21). Clean model fit — new cardholders.

**Recommendation**: Maintain dual-channel for seasonal campaigns. Consider a third seasonal occasion (spring, back-to-school) to strengthen the pattern. Investigate SRM root cause — shared mechanism with VCN?

---

## Design Notes

1. Both campaigns have SRM warnings with the same direction (more control than designed). May share a common root cause in the randomization mechanism.
2. VCN's efficiency gap vs VDA (4,403 vs 541 deployments per incremental) is partly explained by channel cost: MB is free, EM costs money. ROI per dollar spent may tell a different story.
3. NIBT applies cleanly to both — these are acquisition campaigns producing genuinely new cardholders.
