# Model evaluation 20261120113918-paysim

> PaySim is a simulation. These results do not represent performance on real payment fraud.

- Dataset `paysim`, feature version `1.0.0`, git `c8b4a4c35c` (uncommitted changes), MLflow run `c3283ac8d81948e0bc3214e7ef89f8df`.
- Training rows (undersampled): 631,541 (6,571 fraud). Validation rows: 49,620. Test rows: 68,526.
- Candidate selected by validation pr_auc: **lightgbm**. The test set was used once, after selection, for this candidate only.

## Validation comparison (used for every choice; optimistic)

Thresholds, calibrator and hyperparameters were all fitted on validation, so these values overstate what to expect on new data.

| model | features | PR-AUC | ROC-AUC | recall @ FPR 0.1% | Brier | calib. | precision | recall | FPR | policy cost | p50 latency ms | size KB |
|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| lightgbm | 34 | 0.9999 | 1.0000 | 99.88% | 0.00002 | isotonic | 93.82% | 100.00% | 0.1086% | 2,650 | 1.21 | 1,140 |
| logistic_regression | 34 | 0.9997 | 1.0000 | 99.88% | 0.00002 | isotonic | 77.53% | 100.00% | 0.4773% | 11,650 | 1.50 | 2 |
| xgboost | 34 | 0.9996 | 1.0000 | 99.88% | 0.00003 | sigmoid | 69.19% | 100.00% | 0.7334% | 17,900 | 0.99 | 397 |
| random_forest | 34 | 0.9996 | 1.0000 | 99.88% | 0.00008 | sigmoid | 69.19% | 100.00% | 0.7334% | 17,900 | 51.06 | 263 |
| decision_tree | 34 | 0.9988 | 0.9993 | 99.88% | 0.00004 | sigmoid | 100.00% | 99.50% | 0.0000% | 399,045 | 0.16 | 3 |
| lightgbm_no_balance (ablation) | 29 | 0.6222 | 0.9695 | 39.05% | 0.00865 | isotonic | 4.67% | 100.00% | 33.6406% | 792,489 | 1.65 | 3,600 |

Precision, recall and FPR are at the REVIEW threshold (anything not approved counts as flagged). Policy cost uses `configs/thresholds.yaml`.

- **isolation_forest** (unsupervised, trained on legitimate rows only): validation PR-AUC 0.0268, ROC-AUC 0.6913. It outputs an anomaly score, not a fraud probability, and was not eligible for selection.

## Candidate on the untouched test set: lightgbm

- Best parameters: `{"n_estimators": 600, "num_leaves": 63, "learning_rate": 0.13298161683184337, "min_child_samples": 100, "subsample": 0.6908954887139107, "colsample_bytree": 0.8218339148063339, "reg_lambda": 0.14023144866075152, "scale_pos_weight": 5.0}`
- Calibration: isotonic (cross-fitted validation Brier: none 0.00003, sigmoid 0.00003, isotonic 0.00003)
- Thresholds (chosen on validation): REVIEW at p >= 0.0185, DECLINE at p >= 0.0185

| metric | validation | test |
|---|---:|---:|
| fraud prevalence | 1.62% | 1.22% |
| PR-AUC | 0.9999 | 1.0000 |
| ROC-AUC | 1.0000 | 1.0000 |
| Brier | 0.00002 | 0.00000 |
| ECE | 0.00000 | 0.00002 |
| precision@100 | 100.00% | 100.00% |
| precision@500 | 100.00% | 100.00% |
| recall @ FPR <= 0.1% | 99.88% | 100.00% |
| recall @ FPR <= 1.0% | 100.00% | 100.00% |
| precision at REVIEW threshold | 93.82% | 93.11% |
| recall at REVIEW threshold | 100.00% | 100.00% |
| f1 at REVIEW threshold | 96.81% | 96.43% |
| FPR at REVIEW threshold | 0.1086% | 0.0916% |
| FNR at REVIEW threshold | 0.00% | 0.00% |
| precision at DECLINE threshold | 93.82% | 93.11% |
| recall at DECLINE threshold | 100.00% | 100.00% |
| f1 at DECLINE threshold | 96.81% | 96.43% |
| FPR at DECLINE threshold | 0.1086% | 0.0916% |
| FNR at DECLINE threshold | 0.00% | 0.00% |
| approve / review / decline rate | 98.27% / 0.00% / 1.73% | 98.69% / 0.00% / 1.31% |
| fraud approved / reviewed / declined | 0 / 0 / 804 | 0 / 0 / 838 |
| policy cost | 2,650 | 3,100 |
| cost if everything approved | 1,152,846,386 | 1,561,133,479 |
| cost reduction vs approve-all | 100.00% | 100.00% |

Test confusion at the REVIEW threshold: TP 838, FP 62, FN 0, TN 67,626.

## Figures

- `pr_curves_val.png`
- `calibration_candidate.png`
- `importance_candidate.png`
- `importance_lightgbm_no_balance.png` (if trained)
