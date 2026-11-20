# Model evaluation 20261120113819-synthetic

> Synthetic generator data: these numbers only show the pipeline runs.

- Dataset `synthetic`, feature version `1.0.0`, git `c8b4a4c35c` (uncommitted changes), MLflow run `9b9cb1b2ee054bc785b2c79a5c7444a6`.
- Training rows (undersampled): 3,432 (138 fraud). Validation rows: 8,347. Test rows: 8,299.
- Candidate selected by validation pr_auc: **logistic_regression**. The test set was used once, after selection, for this candidate only.

## Validation comparison (used for every choice; optimistic)

Thresholds, calibrator and hyperparameters were all fitted on validation, so these values overstate what to expect on new data.

| model | features | PR-AUC | ROC-AUC | recall @ FPR 0.1% | Brier | calib. | precision | recall | FPR | policy cost | p50 latency ms | size KB |
|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| logistic_regression | 34 | 0.8228 | 0.9744 | 81.58% | 0.00084 | isotonic | 1.38% | 100.00% | 32.7717% | 136,150 | 1.42 | 2 |
| random_forest | 34 | 0.8189 | 0.9649 | 81.58% | 0.00084 | isotonic | 1.00% | 100.00% | 45.0836% | 187,300 | 29.39 | 45 |
| lightgbm | 34 | 0.8181 | 0.9577 | 81.58% | 0.00084 | isotonic | 0.81% | 100.00% | 56.1199% | 233,150 | 1.14 | 91 |
| xgboost | 34 | 0.8179 | 0.9584 | 81.58% | 0.00084 | isotonic | 1.23% | 97.37% | 35.8647% | 177,379 | 0.73 | 82 |
| decision_tree | 34 | 0.1359 | 0.9290 | 0.00% | 0.00394 | isotonic | 1.68% | 92.11% | 24.7081% | 342,699 | 0.18 | 1 |
| lightgbm_no_balance (ablation) | 29 | 0.0798 | 0.8792 | 7.89% | 0.00441 | sigmoid | 1.07% | 100.00% | 42.3757% | 172,799 | 1.48 | 232 |

Precision, recall and FPR are at the REVIEW threshold (anything not approved counts as flagged). Policy cost uses `configs/thresholds.yaml`.

- **isolation_forest** (unsupervised, trained on legitimate rows only): validation PR-AUC 0.0050, ROC-AUC 0.5542. It outputs an anomaly score, not a fraud probability, and was not eligible for selection.

## Candidate on the untouched test set: logistic_regression

- Best parameters: `{"max_iter": 1000, "C": 1.246878665907565, "class_weight": "balanced"}`
- Calibration: isotonic (cross-fitted validation Brier: none 0.00936, sigmoid 0.00094, isotonic 0.00084)
- Thresholds (chosen on validation): REVIEW at p >= 0.0020, DECLINE at p >= 0.0020

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
| precision at REVIEW threshold | 1.38% | 1.43% |
| recall at REVIEW threshold | 100.00% | 97.44% |
| f1 at REVIEW threshold | 2.72% | 2.81% |
| FPR at REVIEW threshold | 32.7717% | 31.7918% |
| FNR at REVIEW threshold | 0.00% | 2.56% |
| precision at DECLINE threshold | 1.38% | 1.43% |
| recall at DECLINE threshold | 100.00% | 97.44% |
| f1 at DECLINE threshold | 2.72% | 2.81% |
| FPR at DECLINE threshold | 32.7717% | 31.7918% |
| FNR at DECLINE threshold | 0.00% | 2.56% |
| approve / review / decline rate | 66.92% / 0.00% / 33.08% | 67.90% / 0.00% / 32.10% |
| fraud approved / reviewed / declined | 0 / 0 / 38 | 1 / 0 / 38 |
| policy cost | 136,150 | 268,189 |
| cost if everything approved | 4,698,005 | 52,275,757 |
| cost reduction vs approve-all | 97.10% | 99.49% |

Test confusion at the REVIEW threshold: TP 38, FP 2,626, FN 1, TN 5,634.

## Figures

- `pr_curves_val.png`
- `calibration_candidate.png`
- `importance_candidate.png`
- `importance_lightgbm_no_balance.png` (if trained)
