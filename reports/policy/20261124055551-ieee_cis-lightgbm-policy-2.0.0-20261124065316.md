# Decision policy 2.0.0 for model 20261124055551-ieee_cis-lightgbm

> IEEE-CIS is real but anonymized: column meanings are partly hidden, the costs in configs/thresholds.yaml are assumptions, and results apply to this dataset only. The costs are assumptions, not business-approved figures.

- Owner: unassigned (portfolio project: assumptions, not business-approved figures). Approval status: **unapproved**.
- Thresholds chosen on validation (83,654 rows, 2,828 fraud): REVIEW at p >= 0.1460, DECLINE at p >= 0.2775.
- Costs: fraud approved = 0 + 1 x amount; legitimate declined = 25 + 0.02 x amount; review = 5; reviews stop 95% of fraud; review queue <= 2% of traffic.

## Outcomes

| | validation (tuning) | test (thresholds fixed) |
|---|---:|---:|
| approve rate | 95.12% | 93.97% |
| review rate | 1.98% | 2.58% |
| decline rate | 2.90% | 3.44% |
| fraud approved | 991 | 1,190 |
| fraud reviewed | 318 | 304 |
| fraud declined | 1,519 | 1,620 |
| legitimate payments declined, cost | 25,971 | 41,402 |
| total policy cost | 198,614 | 254,447 |
| share of fraud amount stopped | 63.49% | 57.79% |

## Sensitivity to the cost assumptions (validation)

Each row re-optimizes thresholds under one changed assumption. If outcomes swing a lot, the business needs to pin that number down before the policy is trusted.

| scenario | change | REVIEW at | DECLINE at | approve | review | decline | fraud approved |
|---|---|---:|---:|---:|---:|---:|---:|
| baseline | none | 0.1460 | 0.2775 | 95.12% | 1.98% | 2.90% | 991 |
| old_flat_decline_cost | false_decline_cost=50.0, false_decline_amount_fraction=0.0 | 0.1775 | 0.3837 | 95.65% | 1.97% | 2.38% | 1056 |
| cheap_declines | false_decline_amount_fraction=0.005 | 0.1460 | 0.2775 | 95.12% | 1.98% | 2.90% | 991 |
| costly_declines | false_decline_amount_fraction=0.05 | 0.1460 | 0.2775 | 95.12% | 1.98% | 2.90% | 991 |
| costly_reviews | review_cost=20.0 | 0.1460 | 0.2552 | 95.12% | 1.64% | 3.24% | 991 |
| weaker_reviewers | review_fraud_catch_rate=0.8 | 0.1460 | 0.2775 | 95.12% | 1.98% | 2.90% | 991 |
