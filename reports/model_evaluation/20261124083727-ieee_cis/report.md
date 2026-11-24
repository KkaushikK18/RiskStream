# Model evaluation 20261124083727-ieee_cis

> IEEE-CIS is real but anonymized: column meanings are partly hidden, the costs in configs/thresholds.yaml are assumptions, and results apply to this dataset only.

- Dataset `ieee_cis`, feature version `ieee-1.1.0`, git `1ac11bc22b` (uncommitted changes), MLflow run `7c30259cc1be43b3b5232d8681d60f90`.
- Training rows (undersampled): 115,573 (14,721 fraud). Validation rows: 83,654. Test rows: 89,325.
- Candidate selected by validation pr_auc: **lightgbm_no_behavior**. The test set was used once, after selection, for this candidate only.

## Validation comparison (used for every choice; optimistic)

Thresholds, calibrator and hyperparameters were all fitted on validation, so these values overstate what to expect on new data.

| model | features | PR-AUC | ROC-AUC | recall @ FPR 0.1% | Brier | calib. | precision | recall | FPR | policy cost | p50 latency ms | size KB |
|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| lightgbm_no_behavior | 407 | 0.6238 | 0.9299 | 25.92% | 0.01882 | sigmoid | 42.21% | 68.92% | 3.3009% | 190,502 | 2.28 | 14,049 |
| lightgbm | 442 | 0.6114 | 0.9267 | 24.01% | 0.01927 | sigmoid | 39.82% | 68.95% | 3.6461% | 197,376 | 2.46 | 14,206 |
| lightgbm_no_vesta_v (ablation) | 161 | 0.6097 | 0.9282 | 25.67% | 0.01932 | sigmoid | 39.55% | 68.49% | 3.6622% | 200,872 | 2.18 | 14,175 |
| xgboost | 442 | 0.5820 | 0.9314 | 23.76% | 0.01960 | isotonic | 43.65% | 63.86% | 2.8840% | 198,718 | 2.73 | 7,042 |
| random_forest | 442 | 0.4576 | 0.8876 | 15.21% | 0.02313 | isotonic | 30.24% | 58.66% | 4.7349% | 270,614 | 49.36 | 5,097 |
| logistic_regression | 419 | 0.4427 | 0.8595 | 14.96% | 0.02349 | isotonic | 33.66% | 54.49% | 3.7575% | 257,019 | 0.97 | 16 |
| decision_tree | 442 | 0.3852 | 0.8507 | 15.63% | 0.02478 | isotonic | 31.03% | 48.51% | 3.7735% | 315,017 | 0.18 | 17 |

Precision, recall and FPR are at the REVIEW threshold (anything not approved counts as flagged). Policy cost uses `configs/thresholds.yaml`.

- **isolation_forest** (unsupervised, trained on legitimate rows only): validation PR-AUC 0.1225, ROC-AUC 0.7768. It outputs an anomaly score, not a fraud probability, and was not eligible for selection.

## Candidate on the untouched test set: lightgbm_no_behavior

- Best parameters: `{"n_estimators": 1200, "num_leaves": 255, "learning_rate": 0.09490525874133698, "min_child_samples": 50, "subsample": 0.8111379118104607, "colsample_bytree": 0.534777905637904, "reg_lambda": 0.27288475911711724, "scale_pos_weight": 1.0}`
- Calibration: sigmoid (cross-fitted validation Brier: none 0.02015, sigmoid 0.01882, isotonic 0.01885)
- Thresholds (chosen on validation): REVIEW at p >= 0.1059, DECLINE at p >= 0.1765

| metric | validation | test |
|---|---:|---:|
| fraud prevalence | 3.38% | 3.49% |
| PR-AUC | 0.6238 | 0.5564 |
| ROC-AUC | 0.9299 | 0.9103 |
| Brier | 0.01882 | 0.02175 |
| ECE | 0.00202 | 0.00430 |
| precision@100 | 99.00% | 97.00% |
| precision@500 | 94.60% | 92.60% |
| recall @ FPR <= 0.1% | 25.92% | 23.47% |
| recall @ FPR <= 1.0% | 54.81% | 47.75% |
| precision at REVIEW threshold | 42.21% | 35.40% |
| recall at REVIEW threshold | 68.92% | 65.74% |
| f1 at REVIEW threshold | 52.36% | 46.02% |
| FPR at REVIEW threshold | 3.3009% | 4.3336% |
| FNR at REVIEW threshold | 31.08% | 34.26% |
| precision at DECLINE threshold | 55.82% | 46.16% |
| recall at DECLINE threshold | 60.75% | 57.90% |
| f1 at DECLINE threshold | 58.18% | 51.37% |
| FPR at DECLINE threshold | 1.6826% | 2.4394% |
| FNR at DECLINE threshold | 39.25% | 42.10% |
| approve / review / decline rate | 94.48% / 1.84% / 3.68% | 93.53% / 2.10% / 4.37% |
| fraud approved / reviewed / declined | 879 / 231 / 1718 | 1067 / 244 / 1803 |
| policy cost | 190,502 | 247,297 |
| cost if everything approved | 450,145 | 477,356 |
| cost reduction vs approve-all | 57.68% | 48.19% |

Test confusion at the REVIEW threshold: TP 2,047, FP 3,736, FN 1,067, TN 82,475.

## Figures

- `pr_curves_val.png`
- `calibration_candidate.png`
- `importance_candidate.png`
- `importance_<ablation>.png` for each ablation model
