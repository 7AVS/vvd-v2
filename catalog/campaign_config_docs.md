# VVD Campaign Configuration Documents
Source: Internal campaign config docs, photographed Mar 8-9, 2026
Note: ~~strikethrough~~ = removed/deprecated in document revisions

---

## VCN — Contextual Notification

### Target Audience
RBC clients with active PBA, active in Mobile, no VVD card (physical or digital):
1. Opened DDA account
2. Client requested physical card 8+ weeks prior, still inactive
3. Do not have a Virtual Visa Debit Card (meets eligibility)
4. Newcomers included
5. Chequing account money out >$500/month in past 6 months (High Debit Card User)
6. Number of transactions on chequing >10 in past 6 months (High Debit Card User)
7. Monthly credit card transactions <10 in past 6 months (Low Credit Card User)
8. Students 14+ using Student Banking app (operational)

### Segmentation
- Segment 1: Standard marketing (Age of Majority)
- Segment 2: Student operational (14+)

### Success Definition
- Increase activation of new VVD cards by 30%

### Control Group
- Static, based on last 2 digits of client SRF
- TG7 (5%): last digits 95, 96, 97, 98, 99
- Static to measure cumulative impact of multiple VCN messages over time

### Decision Tree
```
Inclusion Universe
  → Student 14+?
    → Yes: OLB Enrolled? → Mobile Channel Eligible? → PVCNAG02 [MB]
    → No (Age of Majority): OLB Enrolled? → Mobile Channel Eligible? → PVCNAG01 [MB]
  Not eligible → No Offer
```

### Report Groups
| Action Code | Report Group | Segment | Channel |
|---|---|---|---|
| PVCN01AA | PVCNAG01 | Age of Majority (standard marketing) | MB |
| PVCN02AA | PVCNAG02 | Student 14+ (operational) | MB |
| PVCNNMAA | Control (5%) | Both groups | XX |

---

## VDA — Seasonal Acquisition

### Target Audience
RBC clients with active PBA, active in OLB:
1. High Client Cards User - >$1,000/month OR >10 transactions/month in past 6 months (~~20~~ → 10)
2. ~~High ATM User - $600/month in past 6 months~~ REMOVED
3. Low Credit Card Users - $500 or less/month OR <6 transactions/month in past 6 months
4. ~~IOP Users in past 6 months~~ REMOVED
5. ~~Paypal Users in past 6 months~~ REMOVED
6. Newcomers
7. Do not have a Virtual Visa Debit Card
8. ~~Exclude clients in IOP decommissioning campaign (IPD) Nov 2022 and Mar 2023~~ REMOVED

### Prioritization
- Email cap: 250,000
- Prioritize by client tenure ≤ ~~6~~ 8 years (Tenure = 1,2,3,4,5,6,7,8)
- Banner and O&O channel: tenure 1-25 years

### Test and Learn
- Isolate for HSBC clients (pending approval to market new HSBC clients)

### Success Definition (highlighted yellow)
- New VVD activation test over control
- VVD usage test over control

### Reporting Matrix
ACTIVE:
- # of clients requesting VVD - physical and digital
- # of clients activating VVD in mobile app - physical digital
- Average $ of VVD purchase amount
- Average purchases made within the first month
- Average amount of products held by clients

REMOVED:
- ~~# of clients issued VVD~~
- ~~Type of products held by clients~~
- ~~# of wallet provisions~~
- ~~# of wallet transactions - in app and International POS~~

### Decision Tree
```
Exclusion Qualified Universe
  → OLB Eligible? (No → No Action)
  → EM Eligible?
    → Yes: Tenure ≤ 8 years?
      → Yes: PVDAAG03 [EM + Banner/O&O] (250K cap)
      → No: Tenure ≤ 25 years? → PVDAAG04 [Banner/O&O only] (no cap)
    → No: Tenure ≤ 25 years? → PVDAAG04 [Banner/O&O only] (no cap)
```

EM Selection Priority: Select ALL tenure 1-8 first, randomly select rest from tenure ≤ ~~6~~ 8.
Non-EM: Select remaining tenure ≤ ~~6~~ 8 not in EM, then ALL tenure ~~7~~ 9 to 25.

### Report Groups
| Action Code | Report Group | Channel | Volume Cap |
|---|---|---|---|
| PVDA03AA | PVDAAG03 | EM_IM (Email + Banner/O&O) | 250K |
| PVDA04AA | PVDAAG04 | IM (Banner/O&O only) | No cap |
| PVDANMAA | Control (5%) | XX | — |

Note: Reuses action codes from 2023325VDA.

---

## VDT — Activation Trigger

### Target Audience
RBC clients including students 13-17 with 1+ non-activated VVD card issued within last 7 days.
Includes replacements (lost/stolen/expired). Legal/compliance approval for 13-17 year olds attached.

### Trigger Timing
| Wave | Trigger |
|---|---|
| 1. Activation Email | VVD card issued date + 7 calendar days |
| 2. Reminder Email | VVD card issued date + 15 calendar days |

### Segmentation
- Student (Age 13-17)
- Non-Student (Age > 17)

### Success Definition
- ~~10% increase in VVD activation with the first email~~ REMOVED
- ~~5% increase in VVD activation after the second email~~ REMOVED

### Control Group
- Static, based on last 2 digits of client SRF
- TG7 (10%): last digits 90-99
- Reminder control comes from activation control pool
- Rule priority: Rule 1 (activation resting) > Rule 2 (reminder resting)

### Decision Tree — Activation Email
```
Top of house criteria passed
  → Already received activation EM for same VVD card? → Yes: EXCLUDED (Resting Rule 1)
  → No → Age > 17?
    → No (Student): EM Eligible? → Yes: PVDTAG01 [EM] / No: PVDTAG02 [DO]
    → Yes (Non-Student): EM Eligible? → Yes: PVDTAG03 [EM] / No: PVDTAG04 [DO]
```

### Decision Tree — Reminder Email
```
Decisioned by activation EM 8-12 days ago
  → Already received reminder EM for same card? → Yes: EXCLUDED (Resting Rule 2)
  → Card stolen/lost/cancelled? → Yes: EXCLUDED
  → Card was activated? → Yes: EXCLUDED
  → Age > 17?
    → No (Student): EM Eligible? → Yes: PVDTAG11 [EM] / No: EXCLUDED
    → Yes (Non-Student): EM Eligible? → Yes: PVDTAG13 [EM] / No: EXCLUDED
```
Note: Reminder has NO DO fallback — non-EM-eligible clients are excluded.

### Report Groups
| Action Code | Report Group | Segment | Wave | Channel |
|---|---|---|---|---|
| PVDT01AA | PVDTAG01 | Student | Activation | EM |
| PVDT02AA | PVDTAG02 | Student | Activation | DO |
| PVDT03AA | PVDTAG03 | Non-Student | Activation | EM |
| PVDT04AA | PVDTAG04 | Non-Student | Activation | DO |
| PVDT11AA | PVDTAG11 | Student | Reminder | EM |
| PVDT13AA | PVDTAG13 | Non-Student | Reminder | EM |
| PVDTNMAA | Control (10%) | All groups | Both | XX |

### TACTIC_DECISN_VRB_INFO String
- RESP_END: DS+30 (30-day measurement window from decision date)
- SEG: 13-17 (Student) or 17+ (Non-Student)
- LOB: PYT (Payments)
- VVD card number included from byte 101

### Data Dictionary Notes
- Source: DDWV01.VISA_DR_CRD_DLY, STS_CD = 06
- ~~EXPRY_DT filter with hardcoded 2021-09-15~~ REMOVED
- CRD_CNTRL_CD = 0

---

## VUI — Usage Trigger

### Target Audience
RBC clients who:
- Activated digital VVD card in mobile in last 60 days but no purchase yet
- Active mobile user (login within last 90 days)

### Segmentation
- Standard marketing (Age of Majority)
- Student (Age 14+) — Legal/Compliance approval: "To be attached" (PENDING)

### Success Definition
- Increase monthly transactions to 11.5 trxs/mth (from current 10.8)
- Increase average transaction amount to $55 (from current $45)
- Control 5%

### Decision Tree
```
Inclusion/Exclusion Universe
  → Age of Majority?
    → Yes: Made VVD txn in last 30 days? (No → EXCLUDE)
      → Low Credit Card User? (No → EXCLUDE)
      → High Debit User? (No → EXCLUDE)
      → Foreign currency >$100 in past 2 years? (No → EXCLUDE)
      → EM Eligible?
        → Yes + Mobile Banner Eligible: PVUIAG01 [EM_IM]
          (unless dropped due to email dedup → PVUIAG03 [IM])
        → Yes + NOT Mobile Banner: PVUIAG02 [EM]
    → No (Student 14+): Using Mobile Banking App?
      → EM Eligible? → Mobile Channel Eligible? → PVUIAG03 [IM]
  → PVUIAG04 [EM] (Student Email+Mobile eligible, highlighted yellow)
```

### Control Group
- Static, based on client SRF last 2 digits
- TG7: 5%

### Report Groups
| Action Code | Report Group | Segment | Channel | Status |
|---|---|---|---|---|
| PVUI01AA | PVUIAG01 | Age of Majority; EM + Mobile Banner eligible | EM_IM | ACTIVE |
| PVUI02AA | PVUIAG02 | Age of Majority; EM only eligible | EM | ACTIVE |
| PVUI03AA | PVUIAG03 | Age of Majority; Mobile Banner only (+ email dedup overflow) | IM | ACTIVE |
| PVUI04AA | PVUIAG04 | Student 14+; EM + Mobile Banner eligible | EM | ACTIVE |
| PVUI05AA | ~~PVUIAG05~~ | ~~Student 14+; EM only eligible~~ | ~~EM~~ | REMOVED |
| PVUI06AA | ~~PVUIAG06~~ | ~~Student 14+; Mobile Banner only~~ | ~~IM~~ | REMOVED |
| PVDTNMAA | Control (5%) | All groups | XX | ACTIVE |

Note: Control action code is PVDTNMAA (uses VDT prefix — likely shared control pool or naming error).

---

## VUT — Tokenization Usage

### Target Audience
Clients who meet ANY of 3 criteria:
- Made at least 1 VVD transaction in last 30 days
- Low Credit Card User AND High Debit User
- Taken out >$100 in foreign currency in past 2 years

### Exclusion
- Clients communicated to via VUI in last 90 days (resting rule)
- ~~Student Operational segment~~ REMOVED
- ~~Students with age 14+ using Mobile Banking app~~ REMOVED

### Success Definition
- 18% incremental transaction volume over 5 years
- 17% incremental revenue over 5 years
- Control 5%

### Campaign Timeline
| | Start | End |
|---|---|---|
| Campaign period | Jun 9, 2025 | Sep 9, 2025 |
| Build Month | May 2025 | |
| Decision Frequency | One time | |

### Analytics Requirements
ACTIVE:
- # of clients using VVD
- Average $ of VVD purchase amount
- Average purchases made within first month
- Average amount of products held by clients
- Type of products held by clients
- Newcomer card activations and usage

REMOVED:
- ~~Usage by age group~~ (consistent with student segment removal)

### Decision Tree
```
Exclusion Universe
  → Meets any of 3 criteria? (No → EXCLUDE)
  → Targeted by VUI in last 90 days? (Yes → EXCLUDE)
  → EM Eligible? (No → EXCLUDE)
  → PVUTAG01 [EM] — TG4: 95%, TG7: 5%
```

### Report Groups
| Action Code | Report Group | Segment | Channel | Status |
|---|---|---|---|---|
| PVUT01AA | PVUTAG01 | Age of Majority; EM eligible | EM | ACTIVE |
| ~~PVUT02AA~~ | ~~PVUTAG02~~ | ~~Student 14+; EM eligible~~ | ~~EM~~ | REMOVED |
| ~~PVUT03AA~~ | ~~PVUTAG03~~ | ~~Age of Majority; Mobile Banner only~~ | ~~IM~~ | REMOVED |
| ~~PVUT04AA~~ | ~~PVUTAG04~~ | ~~Student 14+; EM + Mobile Banner~~ | ~~EM_IM~~ | REMOVED |
| ~~PVUT05AA~~ | ~~PVUTAG05~~ | ~~Student 14+; EM only~~ | ~~EM~~ | REMOVED |
| ~~PVUT06AA~~ | ~~PVUTAG06~~ | ~~Student 14+; Mobile Banner only~~ | ~~IM~~ | REMOVED |
| PVUTNMAA | Control (5%) | PVUTAG01 only | XX | ACTIVE |

Note: 5 of 6 report groups removed. All Student segments and all IM channels eliminated. VUT control code is PVUTNMAA (VUI prefix).

---

## VAW — Add To Wallet

### Target Audience
RBC clients who:
- Are active VVD users (at least 1 purchase using VVD in last 60 days)
- Have activated digital or hybrid VVD card in mobile
- Have NEVER added VVD to mobile wallet (exclude previously provisioned)
- Are low Credit Card users ($100 or less/month OR <2 transactions/month in past 6 months)
- Are active mobile user (logged in at least 1 time in last 30 days)
- Are iOS users (handled by MarTech, CIDM not required)

### Success Definition
- 25% of active VVD users provisioning card to wallet
- Increased usage of in-app purchases
- Increased usage of tap in store internationally

### Campaign Timeline
| | Start | End |
|---|---|---|
| Campaign period | Apr 14, 2025 | Mar 31, 2026 |
| Build Month | Mar 2025 | |
| Decisioning | Apr 10, 2025 onwards |
| Decision Frequency | Monthly |

### Control
- 5% (highlighted cyan in doc)

### Report Groups
| Action Code | Report Group | Channel | Category |
|---|---|---|---|
| PVAW01AA | PVAWAG01 | IM (In-app) | 400 SALES III / 420 GROW/USAGE |
| PVAWNMAA | Control (5%) | XX | — |

### TACTIC_DECISN_VRB_INFO
- String added to TACTIC_EVNT_IP_AR_HIST

---

## Channel-Level Significance Results
Source: vvd_v3_channel_significance spreadsheet (PXL_20260309_041959596.jpg)

| MNE | RPT_GRP_CD | Segment | Channel | Action Clients | Lift (pp) | p-value | Sig |
|---|---|---|---|---|---|---|---|
| VAW | PVAWAG01 | All | IM | 401,708 | +2.69 | 0 | 99.9% |
| VCN | PVCNAG01 | Age of Majority | MB | 59,091 | +0.28 | 0 | 99.9% |
| VCN | PVCNAG02 | Student 14+ | MB | 1,906,641 | +0.19 | 0 | 99.9% |
| VDA | PVDAAG03 | EM eligible | EM_IM | 317,260 | +0.65 | 0 | 99.9% |
| VDA | PVDAAG04 | Non-EM | IM | 595,599 | +0.14 | 0.0002 | 99.9% |
| VDT | PVDTAG01 | Student Activation | EM | 10,289 | +3.03 | 0.021 | 95% |
| VDT | PVDTAG02 | Student Activation | DO | 2,629 | -3.06 | 0.325 | No |
| VDT | PVDTAG03 | Non-Student Activation | EM | 99,951 | +4.70 | 0 | 99.9% |
| VDT | PVDTAG04 | Non-Student Activation | DO | 16,325 | -0.41 | 0.736 | No |
| VDT | PVDTAG11 | Student Reminder | EM | 5,175 | +5.38 | 0.009 | 99% |
| VDT | PVDTAG13 | Non-Student Reminder | EM | 56,850 | +4.87 | 0 | 99.9% |
| VUI | PVUIAG01 | AoM; EM+IM | EM_IM | 102,572 | +0.62 | 0.337 | No |
| VUI | PVUIAG02 | AoM; EM only | EM | 1,205 | -6.54 | 0.321 | No |
| VUI | PVUIAG03 | AoM; IM only | IM | 31,979 | +1.26 | 0.288 | No |
| VUI | PVUIAG04 | Student; EM | EM | 13,066 | +2.64 | 0.171 | No |
| VUT | PVUTAG01 | AoM; EM | EM | 784,849 | +0.07 | 0.624 | No |
