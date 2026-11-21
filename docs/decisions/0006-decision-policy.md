# ADR 0006: Decision policy versioned separately, amount-scaled decline cost

Status: accepted (Phase 2). Supersedes the cost matrix part of ADR 0005.

## Context

The first policy treated declining a legitimate payment as a flat cost of 50.
PaySim payments often exceed 100,000 (fraud median 441,423), so declining always
looked cheaper than a review that lets 5% of fraud through. The optimizer
therefore never used REVIEW.

Example: a 400,000 payment with fraud probability 1% had expected costs
approve 4,000, review 205, decline 49.5, so the policy declined it.

## Decision

1. **Wrong-decline cost grows with the amount:** 25 + 2% of the amount. The
   amount part stands for lost fees and the chance the customer leaves.
   Under it, the example above costs 7,945 to decline, so it is reviewed.
2. **Review capacity is a hard limit:** at most 2% of traffic.
3. **The policy is a separate, versioned artifact.** `tune-policy` reads a
   saved model, chooses thresholds on validation and writes
   `models/policies/<model_version>/policy-<policy_version>-<timestamp>.json`.
   Costs can change without retraining. Serving will load one model and one
   policy, and the policy records which model it was tuned for.
4. **Sensitivity analysis:** each run re-optimizes under alternative costs
   (`sensitivity` in `configs/thresholds.yaml`) and reports how outcomes move.
5. **Approval status:** policies are written as `unapproved` with an owner
   field. In this project nobody can approve them; in a real system the fraud
   operations owner would.

## Result for model `20261120113918-paysim-lightgbm`

From `reports/policy/20261120113918-paysim-lightgbm-policy-2.0.0-20261120124614.md`:

| | validation | test |
|---|---:|---:|
| approve / review / decline | 98.27 / 0.11 / 1.62% | 98.69 / 0.09 / 1.22% |
| fraud approved / reviewed / declined | 0 / 1 / 803 | 0 / 3 / 835 |

Every sensitivity scenario except the old flat cost produced the same
thresholds. For this model the policy is insensitive to the exact cost guesses,
because its scores are almost all close to 0 or 1.

## Consequences and limits

* The DECLINE threshold is 1.0: only payments whose calibrated probability is
  exactly 1 are declined. Isotonic calibration assigns 1.0 to its top score
  range, and PaySim fraud is nearly perfectly separated. A weaker model would
  have a lower DECLINE threshold.
* Thresholds compare only the probability. The cheapest action also depends on
  the amount, so a per-payment expected-cost decision would be more precise.
  Phase 3 serves the validated threshold policy unchanged. An amount-aware
  policy is deferred to Phase 8, where policy changes can be shadowed and
  canaried before they decide real traffic.
* The test set is reported for each new policy, but thresholds are never chosen
  on it. Costs must not be adjusted in response to test results.
