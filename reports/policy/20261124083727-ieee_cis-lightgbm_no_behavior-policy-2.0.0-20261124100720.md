# Decision policy 2.0.0 for model 20261124083727-ieee_cis-lightgbm_no_behavior

> IEEE-CIS is real but anonymized: column meanings are partly hidden, the costs in configs/thresholds.yaml are assumptions, and results apply to this dataset only. The costs are assumptions, not business-approved figures.

- Owner: unassigned (portfolio project: assumptions, not business-approved figures). Approval status: **unapproved**.
- Thresholds chosen on validation (83,654 rows, 2,828 fraud): REVIEW at p >= 0.1059, DECLINE at p >= 0.1765.
- Costs: fraud approved = 0 + 1 x amount; legitimate declined = 25 + 0.02 x amount; review = 5; reviews stop 95% of fraud; review queue <= 2% of traffic.

## Outcomes

| | validation (tuning) | test (thresholds fixed) |
|---|---:|---:|
| approve rate | 94.48% | 93.53% |
| review rate | 1.84% | 2.10% |
| decline rate | 3.68% | 4.37% |
| fraud approved | 879 | 1,067 |
| fraud reviewed | 231 | 244 |
| fraud declined | 1,718 | 1,803 |
| legitimate payments declined, cost | 39,106 | 59,653 |
| total policy cost | 190,502 | 247,297 |
| share of fraud amount stopped | 68.08% | 62.66% |

## Sensitivity to the cost assumptions (validation)

Each row re-optimizes thresholds under one changed assumption. If outcomes swing a lot, the business needs to pin that number down before the policy is trusted.

| scenario | change | REVIEW at | DECLINE at | approve | review | decline | fraud approved |
|---|---|---:|---:|---:|---:|---:|---:|
| baseline | none | 0.1059 | 0.1765 | 94.48% | 1.84% | 3.68% | 879 |
| old_flat_decline_cost | false_decline_cost=50.0, false_decline_amount_fraction=0.0 | 0.1482 | 0.3443 | 95.82% | 1.84% | 2.34% | 1039 |
| cheap_declines | false_decline_amount_fraction=0.005 | 0.0975 | 0.1566 | 94.15% | 1.84% | 4.01% | 846 |
| costly_declines | false_decline_amount_fraction=0.05 | 0.1059 | 0.1765 | 94.48% | 1.84% | 3.68% | 879 |
| costly_reviews | review_cost=20.0 | 0.1059 | 0.1765 | 94.48% | 1.84% | 3.68% | 879 |
| weaker_reviewers | review_fraud_catch_rate=0.8 | 0.0975 | 0.1566 | 94.15% | 1.84% | 4.01% | 846 |
