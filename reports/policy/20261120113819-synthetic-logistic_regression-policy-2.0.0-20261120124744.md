# Decision policy 2.0.0 for model 20261120113819-synthetic-logistic_regression

> Synthetic data: illustrates the mechanics only.

- Owner: unassigned (portfolio project: assumptions, not business-approved figures). Approval status: **unapproved**.
- Thresholds chosen on validation (8,347 rows, 38 fraud): REVIEW at p >= 1.0000, DECLINE at p >= 1.0000.
- Costs: fraud approved = 0 + 1 x amount; legitimate declined = 25 + 0.02 x amount; review = 5; reviews stop 95% of fraud; review queue <= 2% of traffic.

## Outcomes

| | validation (tuning) | test (thresholds fixed) |
|---|---:|---:|
| approve rate | 99.63% | 99.60% |
| review rate | 0.00% | 0.00% |
| decline rate | 0.37% | 0.40% |
| fraud approved | 7 | 6 |
| fraud reviewed | 0 | 0 |
| fraud declined | 31 | 33 |
| legitimate payments declined, cost | 0 | 0 |
| total policy cost | 1,962,986 | 753,119 |
| share of fraud amount stopped | 58.22% | 98.56% |

## Sensitivity to the cost assumptions (validation)

Each row re-optimizes thresholds under one changed assumption. If outcomes swing a lot, the business needs to pin that number down before the policy is trusted.

| scenario | change | REVIEW at | DECLINE at | approve | review | decline | fraud approved |
|---|---|---:|---:|---:|---:|---:|---:|
| baseline | none | 1.0000 | 1.0000 | 99.63% | 0.00% | 0.37% | 7 |
| old_flat_decline_cost | false_decline_cost=50.0, false_decline_amount_fraction=0.0 | 0.0020 | 0.0020 | 66.92% | 0.00% | 33.08% | 0 |
| cheap_declines | false_decline_amount_fraction=0.005 | 0.0038 | 0.0038 | 90.45% | 0.00% | 9.55% | 4 |
| costly_declines | false_decline_amount_fraction=0.05 | 1.0000 | 1.0000 | 99.63% | 0.00% | 0.37% | 7 |
| costly_reviews | review_cost=20.0 | 1.0000 | 1.0000 | 99.63% | 0.00% | 0.37% | 7 |
| weaker_reviewers | review_fraud_catch_rate=0.8 | 1.0000 | 1.0000 | 99.63% | 0.00% | 0.37% | 7 |
