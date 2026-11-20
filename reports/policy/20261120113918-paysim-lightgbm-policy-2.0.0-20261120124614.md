# Decision policy 2.0.0 for model 20261120113918-paysim-lightgbm

> PaySim is a simulation, and the costs are assumptions, not business-approved figures.

- Owner: unassigned (portfolio project: assumptions, not business-approved figures). Approval status: **unapproved**.
- Thresholds chosen on validation (49,620 rows, 804 fraud): REVIEW at p >= 0.0185, DECLINE at p >= 1.0000.
- Costs: fraud approved = 0 + 1 x amount; legitimate declined = 25 + 0.02 x amount; review = 5; reviews stop 95% of fraud; review queue <= 2% of traffic.

## Outcomes

| | validation (tuning) | test (thresholds fixed) |
|---|---:|---:|
| approve rate | 98.27% | 98.69% |
| review rate | 0.11% | 0.09% |
| decline rate | 1.62% | 1.22% |
| fraud approved | 0 | 0 |
| fraud reviewed | 1 | 3 |
| fraud declined | 803 | 835 |
| legitimate payments declined, cost | 0 | 0 |
| total policy cost | 20,222 | 366,138 |
| share of fraud amount stopped | 100.00% | 99.98% |

## Sensitivity to the cost assumptions (validation)

Each row re-optimizes thresholds under one changed assumption. If outcomes swing a lot, the business needs to pin that number down before the policy is trusted.

| scenario | change | REVIEW at | DECLINE at | approve | review | decline | fraud approved |
|---|---|---:|---:|---:|---:|---:|---:|
| baseline | none | 0.0185 | 1.0000 | 98.27% | 0.11% | 1.62% | 0 |
| old_flat_decline_cost | false_decline_cost=50.0, false_decline_amount_fraction=0.0 | 0.0185 | 0.0185 | 98.27% | 0.00% | 1.73% | 0 |
| cheap_declines | false_decline_amount_fraction=0.005 | 0.0185 | 1.0000 | 98.27% | 0.11% | 1.62% | 0 |
| costly_declines | false_decline_amount_fraction=0.05 | 0.0185 | 1.0000 | 98.27% | 0.11% | 1.62% | 0 |
| costly_reviews | review_cost=20.0 | 0.0185 | 1.0000 | 98.27% | 0.11% | 1.62% | 0 |
| weaker_reviewers | review_fraud_catch_rate=0.8 | 0.0185 | 1.0000 | 98.27% | 0.11% | 1.62% | 0 |
