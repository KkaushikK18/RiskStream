# IEEE-CIS: second dataset (offline)

IEEE-CIS Fraud Detection (Vesta Corporation, Kaggle 2019) is real, anonymized
e-commerce card data. It is used here **offline only**: data checks, features,
training and evaluation. The scoring API, streaming and Redis stay on PaySim
(see "Why offline first" in `workflow.md` and ADR 0011).

> IEEE-CIS is real but anonymized: column meanings are partly hidden, the costs in
> `configs/thresholds.yaml` are assumptions, and results apply to this dataset only.
> Kaggle competition data: kept in `data/raw/ieee-cis/` (git-ignored), never redistributed.

## Commands

No Docker needed. MLflow must point at a local file when the container is not running:

```powershell
$env:MLFLOW_TRACKING_URI = "sqlite:///$($PWD -replace '\\','/')/mlflow.db"
python -m riskstream.cli prepare-data   --dataset ieee_cis   # 27 s
python -m riskstream.cli build-features --dataset ieee_cis   # 14 s
python -m riskstream.cli train          --dataset ieee_cis   # 55 min, peak 1.9-2.5 GB
python -m riskstream.cli tune-policy    --dataset ieee_cis   # policy file + report
```

## Data

| | |
|---|---|
| Files used | `train_transaction.csv` (590,540 rows, 394 columns) joined with `train_identity.csv` (144,233 rows, 41 columns) on TransactionID. `test_*.csv` has no labels and is not used. |
| Time | `TransactionDT` is seconds from an undisclosed reference; `day = (TransactionDT - 86400) // 86400` runs 0-181 |
| Fraud | 20,663 of 590,540 (3.50%) |
| Validation | 590,537 valid; **3 rejected** as exact duplicate records (same second, amount, card and every other field, consecutive TransactionIDs). All structural checks pass: unique ids, every identity row matches a transaction, time never goes backwards in file order. |
| Split (by day) | train 0-120: 417,558 rows, 3.53% fraud; val 121-150: 83,654, 3.38%; test 151-181: 89,325, 3.49%. 89.7% of validation and 87.8% of test card1 values also appear in training. |

Key EDA facts (`reports/eda/ieee_cis/eda_summary.md`): fraud rate differs a lot by
product (C 11.7%, W 2.0%), by identity presence (7.9% with identity data, 2.1% without)
and by device (mobile 10.2%, desktop 6.5%, missing 2.1%). Missing values are common:
D columns 58%, V columns 43%, identity columns 85% on average.

## Features (version `ieee-1.1.0`, 442 in the full set)

Code: `src/riskstream/features/ieee_cis.py`; config: `configs/ieee_cis_features.yaml`.

| Group | Count | What |
|---|---:|---|
| transaction, time | 5 | amount, log amount, cents, hour of day, day of week (relative to the unknown reference) |
| card / address | 8 | card1-3, card5, addr1-2, dist1-2 |
| Vesta C, D, M | 37 | counts, time deltas, match flags (T/F to 1/0) |
| Vesta V | 281 | of 339: 1 constant and 57 near-duplicates (|r| > 0.95 within missing-pattern groups) dropped, chosen on training rows only |
| identity | 24 | has_identity plus 23 numeric id fields |
| category encodings | 52 | 23 string columns as frequency-rank code and frequency, plus 6 id columns as frequency; vocabularies fitted on **training rows only** and saved in `encoders.json` |
| uid behaviour | 21 | per client `uid` (card1 + addr1 + first-seen day from D1): count, sum, mean, log-amount std and z-score, ratio to mean over 1, 7, 30 days, plus all-time count, hours since last, activity ratio. (card1 behaviour was removed in 1.1.0.) |
| uid novelty | 14 | per client, for product, both email domains, device type, device, OS, browser: share of earlier transactions with the same value, and a never-used-before flag |

Point-in-time rule: a window of W days for a transaction at second t covers the entity's
transactions in [t - W days, t - 1 s]. Transactions in the same second, including the
transaction itself, are excluded, and labels are never read. A brute-force recomputation
on 300 random real transactions matched all 2,256 checked values.

Feature sets: `full`; `linear` (no integer codes, for logistic regression);
`no_behavior` (none of our history features; the selected one); ablations `no_vesta_v`,
`no_novelty`.

## Results: current model (feature version `ieee-1.1.0`, run `20261124083727-ieee_cis`)

Validation was used for every choice: hyperparameters, feature set, calibration,
thresholds and model selection.

| model | feature set | features | val PR-AUC | val ROC-AUC | recall @ 0.1% FPR |
|---|---|---:|---:|---:|---:|
| **lightgbm_no_behavior** (selected) | no_behavior | 407 | **0.624** | 0.930 | 25.9% |
| lightgbm | full | 442 | 0.611 | 0.927 | 24.0% |
| lightgbm_no_vesta_v (ablation) | no_vesta_v | 161 | 0.610 | 0.928 | 25.7% |
| xgboost | full | 442 | 0.582 | 0.931 | 23.8% |
| random_forest | full | 442 | 0.458 | 0.888 | 15.2% |
| logistic_regression | linear | 419 | 0.443 | 0.860 | 15.0% |
| decision_tree | full | 442 | 0.385 | 0.851 | 15.6% |
| isolation_forest (unsupervised) | full | 442 | 0.123 | | |

Selected candidate on the test set (fraud prevalence 3.49%), compared with the
`ieee-1.0.0` model:

| metric | v1.1 validation | **v1.1 test** | v1.0 test |
|---|---:|---:|---:|
| PR-AUC | 0.624 | **0.556** | 0.530 |
| ROC-AUC | 0.930 | 0.910 | 0.911 |
| recall at 0.1% FPR | 25.9% | 23.5% | 20.5% |
| recall at 1% FPR | 54.8% | 47.8% | 43.4% |
| precision@500 | 94.6% | 92.6% | 93.2% |
| approve / review / decline | 94.5 / 1.8 / 3.7% | 93.5 / 2.1 / 4.4% | 94.0 / 2.6 / 3.4% |
| fraud reviewed or declined | 1,949 of 2,828 | 2,047 of 3,114 (65.7%) | 1,924 (61.8%) |
| policy cost vs approve-all | -57.7% | -48.2% | -46.7% |

**The test set has now been used twice**: once for the v1.0 candidate and once for this
one. The v1.1 decision was made on validation only (the experiment below never read the
test split), but a second look at test still makes its number slightly optimistic.
A clean estimate needs new, later data, which Phase 7's delayed labels will provide.

Compare PaySim: test PR-AUC 0.9999 with balance features, 0.62 without. For reference
only: the winning Kaggle solution scored about 0.946 ROC-AUC on the competition's private
leaderboard, with heavy client-level feature work. That test period and metric are not
comparable with this split.

Policy: `models/policies/20261124083727-ieee_cis-lightgbm_no_behavior/policy-2.0.0-20261124100720.json`
(status `unapproved`). It uses the same cost assumptions as PaySim (USD here), which have
not been checked for e-commerce.

## Behaviour features (v1.1.0)

In `ieee-1.0.0`, removing our 42 behaviour features *raised* validation PR-AUC (0.624 vs
0.595, same hyperparameters). This section records how that was investigated and fixed.

**Method.** `scripts/ieee_behavior_experiment.py` trains LightGBM with the same two
hyperparameter sets (from the v1.0 tuning run) on each feature variant, and compares
validation PR-AUC with a paired bootstrap (300 resamples) against the baseline. It never
reads the test split. Reports: `reports/experiments/ieee_behavior_round{1,2,3}.md`.

**Step 1: test the drift explanation.** A train-vs-validation PSI showed which features
shift: `card1_txn_count_all` (PSI 0.24; median 219 in train, 695 in validation),
`card1_activity_ratio_1d_30d` (0.19), `uid_txn_count_all` (0.13) and
`uid_hours_since_last_txn` (0.10). Windowed amounts, means and z-scores barely move
(PSI < 0.03).

| variant (round 1) | val PR-AUC | vs v1.0 full, 95% CI |
|---|---:|---|
| A: v1.0 full | 0.600 | baseline |
| B: no behaviour features | 0.617 | +0.018 [+0.013, +0.023] |
| C: drop the drifting "since start" features, cap hours-since-last at 30 days | 0.594 | -0.005 [-0.008, -0.002] |
| D: C, and drop the first 30 training days (incomplete 30-day windows) | 0.588 | -0.012 [-0.017, -0.008] |
| F: uid features only (C rules, warm-up) | 0.603 | +0.004 [-0.001, +0.009] |

Removing the drifting features did **not** help. So the first explanation (drift with
traffic volume) was wrong, or at least not the main cause. Dropping the warm-up days
only removes training data.

**Step 2: which entity hurts, and is it redundancy with Vesta's features?**

| variant (round 2) | val PR-AUC | vs v1.0 full, 95% CI |
|---|---:|---|
| K: uid behaviour only (card1 behaviour removed) | 0.612 | +0.013 [+0.009, +0.018] |
| J: no V columns, card1 + uid behaviour | 0.591 | -0.009 [-0.015, -0.004] |
| I: no V columns, uid behaviour only | 0.608 | +0.009 [+0.003, +0.016] |
| H: no V columns, no behaviour | 0.612 | +0.013 [+0.006, +0.020] |

The **card1 behaviour features are the problem** (K vs A, J vs I), whether or not the V
columns are present. card1 is a group of many card holders, so its aggregates mix
thousands of clients; the model fits group patterns that do not hold in later weeks.
The uid (one client) features are neutral: about the same with or without them (I vs H).

**Step 3: new client-level "novelty" features.** For each client: the share of earlier
transactions with the same product, email domains, device type, device, OS and browser,
and a "never used before" flag. They were checked against a brute-force recomputation
(0 mismatches in 2,100 values).

| variant (round 3, v1.1 tables) | val PR-AUC | vs v1.1 full, 95% CI |
|---|---:|---|
| v1.1 full (uid behaviour + novelty) | 0.610 | baseline |
| without novelty | 0.609 | +0.000 [-0.003, +0.003] |
| without any of our behaviour features | 0.617 | +0.008 [+0.004, +0.011] |

Novelty adds nothing measurable either.

**What v1.1.0 changed.**
- The card1 behaviour features are removed. They were harmful (round 2).
- The uid behaviour and novelty features stay in the feature tables and in the `full`
  set, so they remain measurable.
- The feature set became a tuned choice. `lightgbm` (full) and `lightgbm_no_behavior` get
  the same 10 hyperparameter trials, both are eligible, and selection on validation
  PR-AUC decides. It chose `no_behavior`.

**Conclusion.** On this dataset, our own history features do not add to what Vesta's
C (counts), D (time deltas) and V columns already provide; those are built by the payment
provider from the full transaction history, which we never see. This is a negative
result and is reported as one. The behaviour code still matters for the planned online
version: there, the provider's columns would not exist, and features computed from our
own history would be the only way to know a client's past.

## Next

- Phase 7 uses both datasets for drift and retraining; new labelled periods will give a
  clean test estimate.
- Serving IEEE-CIS online (new API schema, Redis features) is optional, after Phase 8.
- Cost assumptions for e-commerce: to be decided (they are PaySim's today).
