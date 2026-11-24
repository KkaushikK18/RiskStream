# Model evaluation 20261124055551-ieee_cis

> IEEE-CIS is real but anonymized: column meanings are partly hidden, the costs in configs/thresholds.yaml are assumptions, and results apply to this dataset only.

- Dataset `ieee_cis`, feature version `ieee-1.0.0`, git `532686525f` (uncommitted changes), MLflow run `35f4f5bfec2b4553a8e605c5f3d7dc43`.
- Training rows (undersampled): 115,573 (14,721 fraud). Validation rows: 83,654. Test rows: 89,325.
- Candidate selected by validation pr_auc: **lightgbm**. The test set was used once, after selection, for this candidate only.

## Validation comparison (used for every choice; optimistic)

Thresholds, calibrator and hyperparameters were all fitted on validation, so these values overstate what to expect on new data.

| model | features | PR-AUC | ROC-AUC | recall @ FPR 0.1% | Brier | calib. | precision | recall | FPR | policy cost | p50 latency ms | size KB |
|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| lightgbm_no_behavior (ablation) | 407 | 0.6238 | 0.9299 | 25.92% | 0.01882 | sigmoid | 42.21% | 68.92% | 3.3009% | 190,502 | 2.24 | 14,049 |
| lightgbm | 449 | 0.5954 | 0.9300 | 22.07% | 0.01939 | isotonic | 45.02% | 64.96% | 2.7751% | 198,614 | 2.21 | 14,527 |
| lightgbm_no_vesta_v (ablation) | 168 | 0.5815 | 0.9280 | 19.20% | 0.01974 | isotonic | 37.47% | 69.02% | 4.0296% | 206,436 | 2.15 | 14,533 |
| xgboost | 449 | 0.5744 | 0.9287 | 23.09% | 0.01992 | isotonic | 37.89% | 66.41% | 3.8082% | 208,624 | 2.73 | 7,527 |
| random_forest | 449 | 0.4613 | 0.8862 | 15.95% | 0.02313 | isotonic | 31.94% | 57.71% | 4.3031% | 260,039 | 51.79 | 5,096 |
| logistic_regression | 426 | 0.4466 | 0.8614 | 14.85% | 0.02339 | isotonic | 35.47% | 53.50% | 3.4061% | 259,745 | 2.38 | 16 |
| decision_tree | 449 | 0.3835 | 0.8518 | 15.52% | 0.02473 | isotonic | 38.89% | 42.72% | 2.3483% | 316,128 | 0.18 | 17 |

Precision, recall and FPR are at the REVIEW threshold (anything not approved counts as flagged). Policy cost uses `configs/thresholds.yaml`.

- **isolation_forest** (unsupervised, trained on legitimate rows only): validation PR-AUC 0.1320, ROC-AUC 0.7758. It outputs an anomaly score, not a fraud probability, and was not eligible for selection.

## Candidate on the untouched test set: lightgbm

- Best parameters: `{"n_estimators": 1200, "num_leaves": 255, "learning_rate": 0.09490525874133698, "min_child_samples": 50, "subsample": 0.8111379118104607, "colsample_bytree": 0.534777905637904, "reg_lambda": 0.27288475911711724, "scale_pos_weight": 1.0}`
- Calibration: isotonic (cross-fitted validation Brier: none 0.02113, sigmoid 0.01958, isotonic 0.01956)
- Thresholds (chosen on validation): REVIEW at p >= 0.1460, DECLINE at p >= 0.2775

| metric | validation | test |
|---|---:|---:|
| fraud prevalence | 3.38% | 3.49% |
| PR-AUC | 0.5954 | 0.5300 |
| ROC-AUC | 0.9300 | 0.9106 |
| Brier | 0.01939 | 0.02231 |
| ECE | 0.00000 | 0.00543 |
| precision@100 | 100.00% | 100.00% |
| precision@500 | 94.80% | 93.20% |
| recall @ FPR <= 0.1% | 22.07% | 20.52% |
| recall @ FPR <= 1.0% | 51.84% | 43.38% |
| precision at REVIEW threshold | 45.02% | 35.74% |
| recall at REVIEW threshold | 64.96% | 61.79% |
| f1 at REVIEW threshold | 53.18% | 45.29% |
| FPR at REVIEW threshold | 2.7751% | 4.0122% |
| FNR at REVIEW threshold | 35.04% | 38.21% |
| precision at DECLINE threshold | 62.67% | 52.68% |
| recall at DECLINE threshold | 53.71% | 52.02% |
| f1 at DECLINE threshold | 57.84% | 52.35% |
| FPR at DECLINE threshold | 1.1197% | 1.6877% |
| FNR at DECLINE threshold | 46.29% | 47.98% |
| approve / review / decline rate | 95.12% / 1.98% / 2.90% | 93.97% / 2.58% / 3.44% |
| fraud approved / reviewed / declined | 991 / 318 / 1519 | 1190 / 304 / 1620 |
| policy cost | 198,614 | 254,447 |
| cost if everything approved | 450,145 | 477,356 |
| cost reduction vs approve-all | 55.88% | 46.70% |

Test confusion at the REVIEW threshold: TP 1,924, FP 3,459, FN 1,190, TN 82,752.

## Figures

- `pr_curves_val.png`
- `calibration_candidate.png`
- `importance_candidate.png`
- `importance_<ablation>.png` for each ablation model
