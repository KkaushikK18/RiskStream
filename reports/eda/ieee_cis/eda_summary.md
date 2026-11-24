# EDA: ieee_cis

> IEEE-CIS is real but anonymized: column meanings are partly hidden, the costs in configs/thresholds.yaml are assumptions, and results apply to this dataset only.

- 590,537 transactions over 182 days, 20,663 fraud (3.50%); 13,553 distinct card1 values.
- String columns: 31; V columns: 339.

## Fraud rate by week

| week | rows | fraud rate |
|---:|---:|---:|
| 0 | 27,596 | 2.91% |
| 1 | 28,463 | 2.52% |
| 2 | 35,701 | 2.43% |
| 3 | 34,906 | 2.07% |
| 4 | 24,539 | 3.62% |
| 5 | 20,919 | 3.84% |
| 6 | 20,564 | 4.00% |
| 7 | 19,761 | 4.36% |
| 8 | 21,432 | 4.33% |
| 9 | 22,534 | 4.42% |
| 10 | 20,953 | 4.20% |
| 11 | 20,174 | 3.35% |
| 12 | 22,391 | 4.07% |
| 13 | 27,429 | 3.22% |
| 14 | 21,269 | 3.38% |
| 15 | 20,891 | 4.15% |
| 16 | 21,078 | 5.07% |
| 17 | 23,575 | 3.53% |
| 18 | 19,257 | 3.78% |
| 19 | 18,572 | 3.29% |
| 20 | 18,549 | 3.44% |
| 21 | 21,443 | 3.08% |
| 22 | 19,811 | 2.85% |
| 23 | 21,038 | 3.47% |
| 24 | 19,940 | 3.68% |
| 25 | 17,752 | 4.21% |

## By product

| ProductCD | rows | share | fraud rate |
|---|---:|---:|---:|
| W | 439,667 | 74.5% | 2.04% |
| C | 68,519 | 11.6% | 11.69% |
| R | 37,699 | 6.4% | 3.78% |
| H | 33,024 | 5.6% | 4.77% |
| S | 11,628 | 2.0% | 5.90% |

## Identity data present

| has_identity | rows | share | fraud rate |
|---|---:|---:|---:|
| 0 | 446,304 | 75.6% | 2.09% |
| 1 | 144,233 | 24.4% | 7.85% |

## Device type

| DeviceType | rows | share | fraud rate |
|---|---:|---:|---:|
| (missing) | 449,727 | 76.2% | 2.10% |
| desktop | 85,165 | 14.4% | 6.52% |
| mobile | 55,645 | 9.4% | 10.17% |

## Purchaser email domain (top 15)

| P_emaildomain | rows | share | fraud rate |
|---|---:|---:|---:|
| gmail.com | 228,354 | 38.7% | 4.35% |
| yahoo.com | 100,934 | 17.1% | 2.28% |
| (missing) | 94,454 | 16.0% | 2.95% |
| hotmail.com | 45,250 | 7.7% | 5.30% |
| anonymous.com | 36,998 | 6.3% | 2.32% |
| aol.com | 28,289 | 4.8% | 2.18% |
| comcast.net | 7,888 | 1.3% | 3.12% |
| icloud.com | 6,267 | 1.1% | 3.14% |
| outlook.com | 5,096 | 0.9% | 9.46% |
| msn.com | 4,092 | 0.7% | 2.20% |
| att.net | 4,033 | 0.7% | 0.74% |
| live.com | 3,041 | 0.5% | 2.76% |
| sbcglobal.net | 2,970 | 0.5% | 0.40% |
| verizon.net | 2,705 | 0.5% | 0.81% |
| ymail.com | 2,396 | 0.4% | 2.09% |

## Amount (USD)

| is_fraud | p10 | p50 | p90 | p99 | mean |
|---:|---:|---:|---:|---:|---:|
| 0 | 26.31 | 68.50 | 267.08 | 1104.00 | 134.51 |
| 1 | 18.94 | 75.00 | 335.00 | 994.00 | 149.24 |

Amounts with cents (often a currency conversion):

| has cents | rows | share | fraud rate |
|---|---:|---:|---:|
| False | 305,012 | 51.6% | 3.57% |
| True | 285,525 | 48.4% | 3.43% |

## Missing values (mean share per column group)

| group | missing |
|---|---:|
| C (counts) | 0.0% |
| D (time deltas) | 58.2% |
| M (match flags) | 49.9% |
| V (Vesta) | 43.0% |
| identity id_* | 84.8% |
| addr/dist | 43.9% |
