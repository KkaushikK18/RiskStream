# ADR 0005: Class imbalance, calibration and cost-based thresholds

Status: accepted (Phase 2)

## Decision

* **Imbalance:** training uses all fraud rows plus a deterministic 10% of
  legitimate rows (hash of `transaction_id`), after features are computed on
  the full history. Class weights and `scale_pos_weight` are tuned
  hyperparameters. No SMOTE. Validation and test keep their real distribution.
* **Calibration:** undersampling and class weights inflate scores, and fraud
  prevalence shifts between periods (0.105% train, 1.62% validation). A
  calibrator is fitted on validation scores. The method (none, sigmoid,
  isotonic) is chosen by 5-fold cross-fitted Brier score, then refit on all
  validation rows.
* **Thresholds:** two thresholds split scores into APPROVE / REVIEW / DECLINE.
  They minimize expected cost on validation under `configs/thresholds.yaml`
  (fraud loss = amount; false decline = 50; review = 5; reviews catch 95% of
  fraud), with the review queue capped at 2% of traffic.
* **Selection:** the candidate is the eligible model with the highest
  validation PR-AUC. The test set is read once, after selection, for the
  candidate only.

## Consequences

* Validation is used for tuning, calibration, thresholds and selection, so
  validation metrics are optimistic. Only test metrics are reported as
  estimates.
* Calibration fitted on the validation period assumes the test period has a
  similar fraud rate (1.22% vs 1.62%). Phase 7 monitoring has to watch for
  calibration drift.
* The cost figures are assumptions. Changing them changes the thresholds.
