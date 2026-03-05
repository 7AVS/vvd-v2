# VVD v1 Catalog: Calculations Catalog

> Generated from the v1 analysis archive. This document catalogs every metric, formula, filter, and aggregation used across the VVD analysis phases.

---

## Phase 1: Universe Overview

### Universe Size
- **Total deployments**: Row count of `final_df` filtered to action group TG4
- **Unique clients**: `CLNT_NO` DISTINCT count
- **Average deployments per client**: `total_deployments / unique_clients`

### Test Group Split
- **Deployments by TST_GRP_CD**: TG4 = Action, TG7 = Control
- **SRM (Sample Ratio Mismatch) via chi-square**:
  - `chi_sq = SUM((observed - expected)^2 / expected)`
  - `p_value = 1 - chi2.cdf(chi_sq, df=1)`

### Campaign Volume
- **Groupby MNE**: deployments, unique_clients
- **Percentage of total**: `deployments / total_deployments * 100`

### Contact Frequency
- **Groupby CLNT_NO**: `total_contacts = COUNT(*)`
- **Buckets**: 1, 2, 3, 4-5, 6-10, 11+
- **Monthly rate**: `total_deployments / (unique_clients * months_covered)`

### Success Rate Baselines
- **Any success**: `GREATEST()` across all success columns (ACQUISITION_SUCCESS, ACTIVATION_SUCCESS, USAGE_SUCCESS, PROVISIONING_SUCCESS, ISSUE_TO_ACTIVATION_SUCCESS)
- **Rate**: `SUM(success) / COUNT(*)`
- **Lift**: `action_rate - control_rate`

### Statistical Tests
- **Z-test for proportions** (two-tailed)
- **P-value**: `2 * (1 - PHI(|z_score| / SQRT(2)))`

---

## Phase 2: Campaign Interaction Analysis

### Campaign Performance
- **By MNE**: action/control deployments, unique_clients
- **Average contacts**: `deployments / unique_clients`

### Contact Sequence
- **Window function**: `PARTITION BY CLNT_NO ORDER BY TREATMT_STRT_DT`
- **Contact_sequence**: `ROW_NUMBER()`
- **Success rate by sequence bucket**

### Campaign Overlap
- **Self-join on CLNT_NO**, filter `DATEDIFF <= 25 days`
- **Metrics**: `COUNT DISTINCT clients`, `AVG days_between`, `MEDIAN days_between`

### Monthly Trends
- **Consistency score**: `(active_months / total_months) * 100`
- **Month-over-month change**: `((this_month - prev_month) / prev_month) * 100`

---

## Phase 3: Overcontacting Analysis

### Frequency Stats
- **By campaign**: `COUNT DISTINCT CLNT_NO`, `SUM deployments`
- **Percentiles**: `AVG`, `MAX`, `PERCENTILE(0.5)`, `PERCENTILE(0.75)`, `PERCENTILE(0.90)`

### Gap Analysis
- **Self-join**: `days_between = DATEDIFF(b.TREATMT_STRT_DT, a.TREATMT_STRT_DT)`
- **Gap categories**: 0-7, 8-15, 16-20, 21-25, 26-30, 31-60, 61-90, 90+

### VCN Orchestration
- **LAG function**: `LAG(TREATMT_STRT_DT)` for previous deployment date
- **days_since_last**: difference from previous deployment
- **Violations**: `SUM(CASE WHEN days_since_last < 30 THEN 1 ELSE 0 END)`

### Response Decay
- **attempt_number**: `ROW_NUMBER()` partitioned by client, ordered by date
- **Success rate by attempt**: limited to first 10 attempts

### Client Fatigue
- **VCN contact count mapped to fatigue levels**:
  - No VCN: 0 contacts
  - Low: 1-3 contacts
  - Medium: 4-6 contacts
  - High: 7-10 contacts
  - Very High: 11+ contacts
- **Impact %**: `((rate_with_fatigue - baseline) / baseline) * 100`

---

## Phase 4: Deep Dive Analysis

### NIBT Calculation
- **Conservative**: `revenue - (monthly_cost * (months_in_period / total_months))`
- **Realistic**: `revenue - (monthly_cost * 0.75)`

### Demographic Breakdowns

| Dimension | Bins |
|-----------|------|
| AGE_RNG | 18-24, 25-34, 35-44, 45-54, 55-64, 65+ |
| INCOME | <$30K, $30-50K, $50-75K, $75-100K, $100-150K, $150K+ |
| CREDIT_SCORE | <650, 650-699, 700-749, 750-799, 800+ |
| TENURE | 0-30d, 31-90d, 91-180d, 181-365d, 1-2yr, 2-3yr, 3-5yr, 5-10yr, 10+yr |
| PROFITABILITY | Negative, $0, $1-99, $100-499, $500-999, $1000-2499, $2500+ |

### Service Category
- **Total products**: `T_TOT_CNT + I_TOT_CNT + B_TOT_CNT + C_TOT_CNT`
- **Buckets**: 0, 1, 2, 3, 4+ products

### Response Timing
- **Days until first success**: date difference from treatment start to first success event
- **Day buckets**: 0-3, 4-7, 8-14, 15-30, 31+
- **Deployments until success**: `ROW_NUMBER()` partitioned by client, ordered by date, up to first success

### Success Factors
- **Responders vs Non-responders profiles**:
  - `AVG(AGE)`
  - `AVG(TENURE)`
  - `AVG(PROF_TOT_ANNUAL)`

---

## Journey Analysis

### Responder Identification
- **VCN**: `MNE = 'VCN' AND ACQUISITION_SUCCESS = 1`
- **VDA**: `MNE = 'VDA' AND ACTIVATION_SUCCESS = 1`

### Post-Activation Paths
- **Filter**: `TREATMT_STRT_DT > last_activation_date`
- **post_activation_order**: `ROW_NUMBER()` partitioned by client, ordered by treatment start date

### Sankey Flow
- **14 nodes**, dual flows per node (responded / not responded)
- **Opacity**: responded = 0.7, no response = 0.25

---

## Campaign Success Mapping (Used Across All Phases)

| Campaign | Success Column | Notes |
|----------|---------------|-------|
| VCN | ACQUISITION_SUCCESS | Standard |
| VDA | ACTIVATION_SUCCESS | Backend workaround -- should be ACQUISITION_SUCCESS |
| VDT | ACTIVATION_SUCCESS | Standard |
| VUI | USAGE_SUCCESS | Standard |
| VAW | PROVISIONING_SUCCESS | Standard |
| VUT | PROVISIONING_SUCCESS | Standard |
