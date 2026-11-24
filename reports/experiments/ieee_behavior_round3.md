# IEEE-CIS behaviour-feature experiment

Validation only (the test split is not read). Same LightGBM hyperparameters for every variant (2 sets from the ieee-1.0.0 tuning run). Warm-up = training rows with day < 30 removed (their 30-day windows are incomplete); cap = hours since last transaction undefined beyond 720 h.

| variant | features | train rows | mean val PR-AUC | vs A (paired bootstrap, 95% CI) |
|---|---:|---:|---:|---|
| A_full | 442 | 115,573 | 0.6098 | baseline |
| B_no_behavior | 407 | 115,573 | 0.6168 | +0.0075 [+0.0038, +0.0108] |
| M_no_novelty | 428 | 115,573 | 0.6093 | +0.0001 [-0.0028, +0.0033] |
