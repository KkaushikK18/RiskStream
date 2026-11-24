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
python -m riskstream.cli train          --dataset ieee_cis   # 56 min, peak 2.5 GB
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

## Features (version `ieee-1.0.0`, 449 in the full set)

Code: `src/riskstream/features/ieee_cis.py`; config: `configs/ieee_cis_features.yaml`.

| Group | Count | What |
|---|---:|---|
| transaction, time | 5 | amount, log amount, cents, hour of day, day of week (relative to the unknown reference) |
| card / address | 8 | card1-3, card5, addr1-2, dist1-2 |
| Vesta C, D, M | 37 | counts, time deltas, match flags (T/F to 1/0) |
| Vesta V | 281 | of 339: 1 constant and 57 near-duplicates (|r| > 0.95 within missing-pattern groups) dropped, chosen on training rows only |
| identity | 24 | has_identity plus 23 numeric id fields |
| category encodings | 52 | 23 string columns as frequency-rank code and frequency, plus 6 id columns as frequency; vocabularies fitted on **training rows only** and saved in `encoders.json` |
| behaviour | 42 | per `card1` and per `uid` (card1 + addr1 + first-seen day from D1): count, sum, mean, log-amount std and z-score, ratio to mean over 1, 7, 30 days, plus all-time count, hours since last, activity ratio |

Point-in-time rule: a window of W days for a transaction at second t covers the entity's
transactions in [t - W days, t - 1 s]. Transactions in the same second, including the
transaction itself, are excluded, and labels are never read. A brute-force recomputation
on 300 random real transactions matched all 2,256 checked values.

Feature sets: `full`; `linear` (no integer codes, for logistic regression); ablations
`no_vesta_v` and `no_behavior`.

## Results (run `20261124055551-ieee_cis`, 2026-11-24)

Validation was used for every choice (hyperparameters, calibration, thresholds, model
selection). The test set was used once, for the selected candidate only.

| model | features | val PR-AUC | val ROC-AUC | recall @ 0.1% FPR |
|---|---:|---:|---:|---:|
| **lightgbm** (selected) | 449 | **0.595** | 0.930 | 22.1% |
| lightgbm_no_behavior (ablation) | 407 | 0.624 | 0.930 | 25.9% |
| lightgbm_no_vesta_v (ablation) | 168 | 0.582 | 0.928 | 19.2% |
| xgboost | 449 | 0.574 | 0.929 | 23.1% |
| random_forest | 449 | 0.461 | 0.886 | 16.0% |
| logistic_regression | 426 | 0.447 | 0.861 | 14.9% |
| decision_tree | 449 | 0.384 | 0.852 | 15.5% |
| isolation_forest (unsupervised) | 449 | 0.132 | 0.776 | |

Selected candidate on the **test set** (fraud prevalence 3.49%):

| metric | validation | test |
|---|---:|---:|
| PR-AUC | 0.595 | **0.530** |
| ROC-AUC | 0.930 | 0.911 |
| recall at 1% FPR | 51.8% | 43.4% |
| precision@500 | 94.8% | 93.2% |
| approve / review / decline | 95.1 / 2.0 / 2.9% | 94.0 / 2.6 / 3.4% |
| fraud caught (reviewed + declined) | 1,837 of 2,828 | 1,924 of 3,114 (61.8%) |
| policy cost vs approve-all | -55.9% | -46.7% |

Compare PaySim: test PR-AUC 0.9999 with balance features, 0.62 without. IEEE-CIS gives
a realistic, harder number. For reference only: the winning Kaggle solution scored about
0.946 ROC-AUC on the competition's private leaderboard, with heavy client-level feature
work; that test period and metric are not comparable with this split.

### What the ablations show

1. **Our behaviour features hurt validation PR-AUC.** The two runs used identical
   hyperparameters for their first 6 trials, and without the 42 behaviour features PR-AUC
   was higher in all 6 pairs (by 0.013-0.020). Yet the full model gives those features 41%
   of its splits. The likely cause is drift: card1 is a group of many cards, so its counts
   and sums follow total traffic, which changes from week to week (17,000-36,000 rows).
   Patterns learned in training then shift in later weeks. This is a finding to act on,
   not a bug (see "Next" below).
2. **Vesta's V columns help a little:** -0.014 PR-AUC without them. A real-time system
   could not compute them itself, so a served model would either receive them from the
   payment provider or lose about that much.
3. The validation-to-test drop (0.595 to 0.530) is larger than on PaySim. Fraud
   patterns change over the 6 months, which is what Phase 7 (drift monitoring and
   retraining) is for.

Policy: `models/policies/20261124055551-ieee_cis-lightgbm/policy-2.0.0-20261124065316.json`
(status `unapproved`). It uses the same cost assumptions as PaySim (USD here), which are
untested for e-commerce.

## Next

- Feature iteration `ieee-1.1.0`: remove or normalize the volume-dependent card1 behaviour
  features, keep uid ones, choose on validation. The test set has now been used once;
  a second test evaluation must be reported as such.
- Phase 7 uses both datasets for drift and retraining.
- Serving IEEE-CIS online (new API schema, Redis features) is optional, after Phase 8.
