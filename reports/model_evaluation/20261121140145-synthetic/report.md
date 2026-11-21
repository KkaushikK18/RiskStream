# Model evaluation 20261121140145-synthetic

> Synthetic generator data: these numbers only show the pipeline runs.

- Dataset `synthetic`, feature version `1.0.0`, git `ddbe17c170` (uncommitted changes), MLflow run `2c2699b2a0ac49f489ec819007b4114e`.
- Training rows (undersampled): 3,432 (138 fraud). Validation rows: 8,347. Test rows: 8,299.
- Candidate selected by validation pr_auc: **logistic_regression**. The test set was used once, after selection, for this candidate only.

## Validation comparison (used for every choice; optimistic)

Thresholds, calibrator and hyperparameters were all fitted on validation, so these values overstate what to expect on new data.

| model | features | PR-AUC | ROC-AUC | recall @ FPR 0.1% | Brier | calib. | precision | recall | FPR | policy cost | p50 latency ms | size KB |
|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| logistic_regression | 34 | 0.8228 | 0.9744 | 81.58% | 0.00084 | isotonic | 100.00% | 81.58% | 0.0000% | 1,962,986 | 1.63 | 2 |
| lightgbm | 34 | 0.8181 | 0.9577 | 81.58% | 0.00084 | isotonic | 100.00% | 81.58% | 0.0000% | 1,962,986 | 1.37 | 91 |
| xgboost | 34 | 0.8179 | 0.9584 | 81.58% | 0.00084 | isotonic | 100.00% | 81.58% | 0.0000% | 1,962,986 | 0.76 | 82 |
| random_forest | 34 | 0.6082 | 0.9644 | 50.00% | 0.00234 | isotonic | 15.74% | 81.58% | 1.9978% | 2,009,186 | 31.26 | 35 |
| decision_tree | 34 | 0.1359 | 0.9290 | 0.00% | 0.00394 | isotonic | 16.40% | 81.58% | 1.9016% | 2,201,528 | 0.16 | 1 |
| lightgbm_no_balance (ablation) | 29 | 0.0798 | 0.8792 | 7.89% | 0.00441 | sigmoid | 5.19% | 21.05% | 1.7571% | 4,115,512 | 1.23 | 232 |

Precision, recall and FPR are at the REVIEW threshold (anything not approved counts as flagged). Policy cost uses `configs/thresholds.yaml`.

- **isolation_forest** (unsupervised, trained on legitimate rows only): validation PR-AUC 0.0050, ROC-AUC 0.5542. It outputs an anomaly score, not a fraud probability, and was not eligible for selection.

## Candidate on the untouched test set: logistic_regression

- Best parameters: `{"max_iter": 1000, "C": 1.246878665907565, "class_weight": "balanced"}`
- Calibration: isotonic (cross-fitted validation Brier: none 0.00936, sigmoid 0.00094, isotonic 0.00084)
- Thresholds (chosen on validation): REVIEW at p >= 1.0000, DECLINE at p >= 1.0000

| metric | validation | test |
|---|---:|---:|
| fraud prevalence | 0.46% | 0.47% |
| PR-AUC | 0.8228 | 0.8523 |
| ROC-AUC | 0.9744 | 0.9653 |
| Brier | 0.00084 | 0.00084 |
| ECE | 0.00000 | 0.00049 |
| precision@100 | 31.00% | 33.00% |
| precision@500 | 6.60% | 6.80% |
| recall @ FPR <= 0.1% | 81.58% | 84.62% |
| recall @ FPR <= 1.0% | 81.58% | 84.62% |
| precision at REVIEW threshold | 100.00% | 100.00% |
| recall at REVIEW threshold | 81.58% | 84.62% |
| f1 at REVIEW threshold | 89.86% | 91.67% |
| FPR at REVIEW threshold | 0.0000% | 0.0000% |
| FNR at REVIEW threshold | 18.42% | 15.38% |
| precision at DECLINE threshold | 100.00% | 100.00% |
| recall at DECLINE threshold | 81.58% | 84.62% |
| f1 at DECLINE threshold | 89.86% | 91.67% |
| FPR at DECLINE threshold | 0.0000% | 0.0000% |
| FNR at DECLINE threshold | 18.42% | 15.38% |
| approve / review / decline rate | 99.63% / 0.00% / 0.37% | 99.60% / 0.00% / 0.40% |
| fraud approved / reviewed / declined | 7 / 0 / 31 | 6 / 0 / 33 |
| policy cost | 1,962,986 | 753,119 |
| cost if everything approved | 4,698,005 | 52,275,757 |
| cost reduction vs approve-all | 58.22% | 98.56% |

Test confusion at the REVIEW threshold: TP 33, FP 0, FN 6, TN 8,260.

## Figures

- `pr_curves_val.png`
- `calibration_candidate.png`
- `importance_candidate.png`
- `importance_lightgbm_no_balance.png` (if trained)
