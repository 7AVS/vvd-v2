# RPT_GRP_CD → Channel Mapping (Corrected from Config Docs)

Source: Official campaign configuration documents + channel significance data

## Complete Mapping

| MNE | RPT_GRP_CD | Segment | Channel | Wave | Status | Lift (pp) | Sig |
|-----|-----------|---------|---------|------|--------|-----------|-----|
| VAW | PVAWAG01 | All (iOS, mobile-engaged) | IM | Monthly | ACTIVE | +2.69 | Yes |
| VCN | PVCNAG01 | Age of Majority | MB | Monthly | ACTIVE | +0.28 | Yes |
| VCN | PVCNAG02 | Student 14+ | MB | Monthly | ACTIVE | +0.19 | Yes |
| VDA | PVDAAG03 | EM eligible, tenure ≤8 | EM_IM | Seasonal | ACTIVE | +0.65 | Yes |
| VDA | PVDAAG04 | Non-EM, tenure 1-25 | IM (Banner/O&O) | Seasonal | ACTIVE | +0.14 | Yes |
| VDT | PVDTAG01 | Student, EM eligible | EM | Activation (day 7) | ACTIVE | +3.03 | Yes (95%) |
| VDT | PVDTAG02 | Student, NOT EM eligible | DO | Activation (day 7) | ACTIVE | -3.06 | No |
| VDT | PVDTAG03 | Non-Student, EM eligible | EM | Activation (day 7) | ACTIVE | +4.70 | Yes |
| VDT | PVDTAG04 | Non-Student, NOT EM eligible | DO | Activation (day 7) | ACTIVE | -0.41 | No |
| VDT | PVDTAG11 | Student, EM eligible | EM | Reminder (day 15) | ACTIVE | +5.38 | Yes (99%) |
| VDT | PVDTAG13 | Non-Student, EM eligible | EM | Reminder (day 15) | ACTIVE | +4.87 | Yes |
| VUI | PVUIAG01 | AoM; EM + Mobile Banner | EM_IM | Triggered | ACTIVE | +0.62 | No |
| VUI | PVUIAG02 | AoM; EM only | EM | Triggered | ACTIVE | -6.54 | No |
| VUI | PVUIAG03 | AoM; Mobile Banner only (+ dedup overflow) | IM | Triggered | ACTIVE | +1.26 | No |
| VUI | PVUIAG04 | Student 14+; EM + Mobile Banner | EM | Triggered | ACTIVE | +2.64 | No |
| VUI | ~~PVUIAG05~~ | ~~Student 14+; EM only~~ | ~~EM~~ | — | REMOVED | — | — |
| VUI | ~~PVUIAG06~~ | ~~Student 14+; IM only~~ | ~~IM~~ | — | REMOVED | — | — |
| VUT | PVUTAG01 | AoM; EM eligible | EM | One-time | ACTIVE | +0.07 | No |
| VUT | ~~PVUTAG02~~ | ~~Student 14+; EM eligible~~ | ~~EM~~ | — | REMOVED | — | — |
| VUT | ~~PVUTAG03~~ | ~~AoM; Mobile Banner only~~ | ~~IM~~ | — | REMOVED | — | — |
| VUT | ~~PVUTAG04~~ | ~~Student 14+; EM+IM~~ | ~~EM_IM~~ | — | REMOVED | — | — |
| VUT | ~~PVUTAG05~~ | ~~Student 14+; EM only~~ | ~~EM~~ | — | REMOVED | — | — |
| VUT | ~~PVUTAG06~~ | ~~Student 14+; IM only~~ | ~~IM~~ | — | REMOVED | — | — |

## Channel Code Legend

| Code | Full Name | Description |
|------|-----------|-------------|
| MB | Mobile Banking | Mobile push notification |
| EM | Email | Email communication |
| IM | In-app Message | In-app message or Banner/Ad-serve/O&O |
| DO | Digital Online | Fallback for non-EM-eligible clients (VDT only). NOT a deliberate channel test |
| EM_IM | Email + In-app | Multi-touch: email AND in-app/banner |
| XX | Control Holdout | No communication |

## Key Corrections from Config Docs

1. **DO is NOT a deliberate channel** — it's a fallback for VDT clients who are not email-eligible. Only appears on activation wave, NOT reminder wave (non-EM clients are excluded from reminders)
2. **VCN split is Student vs Age of Majority**, not creative test. AG01=AoM, AG02=Student
3. **VDA AG03 = EM+Banner/O&O** (250K email cap), AG04 = Banner/O&O only (no email, no cap). Split is EM eligibility + tenure-based prioritization
4. **VDT has 2 waves**: Activation (day 7, 4 groups) and Reminder (day 15, 2 groups). Reminder has no DO fallback
5. **VUI AG05/AG06 removed** — Student EM-only and Student IM-only segments crossed out
6. **VUT massively simplified** — 5 of 6 groups crossed out. All Student segments + all IM channels removed. Only PVUTAG01 (AoM, EM) remains
7. **VUI action codes use PVUT prefix** — potential naming confusion with VUT campaign
8. **VUT control code is PVUTNMAA** (VUI prefix) — shared control pool or naming issue

## Cross-Campaign Insights

### Segment Patterns
- Student segments were removed from VUI (AG05/06) and VUT (AG02/04/05/06)
- Student segments remain active in VCN (AG02), VDT (AG01/11), VUI (AG04)
- VAW has no student segment (iOS-only targeting handles this differently)

### Channel Effectiveness by Goal
| Goal | Best Channel | Evidence |
|------|-------------|---------|
| Acquisition | MB (push) for always-on, EM+IM for seasonal | VCN MB works, VDA EM_IM >> IM alone |
| Activation | EM (email) | VDT EM groups all significant, DO groups all fail |
| Usage | None work | VUI: no channel significant |
| Provisioning | IM (in-app) | VAW IM = +2.69pp; VUT EM = +0.07pp (not sig) |

### Resting Rules
- VDT: Rest activation EM per VVD card. Rest reminder EM per VVD card. Rule 1 > Rule 2
- VUT: Exclude clients targeted by VUI in last 90 days
- VUI: 365-day resting rule (recommendation to relax to 30 days, status unknown)
