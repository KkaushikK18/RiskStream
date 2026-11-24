# IEEE-CIS behaviour-feature experiment

Validation only (the test split is not read). Same LightGBM hyperparameters for every variant (2 sets from the ieee-1.0.0 tuning run). Warm-up = training rows with day < 30 removed (their 30-day windows are incomplete); cap = hours since last transaction undefined beyond 720 h.

| variant | features | train rows | mean val PR-AUC | vs A (paired bootstrap, 95% CI) |
|---|---:|---:|---:|---|
| A_v1.0_full | 449 | 115,573 | 0.5999 | baseline |
| H_no_v_no_behavior | 126 | 115,573 | 0.6120 | +0.0128 [+0.0061, +0.0202] |
| I_no_v_uid_behavior | 147 | 115,573 | 0.6083 | +0.0093 [+0.0031, +0.0160] |
| J_no_v_full_behavior | 168 | 115,573 | 0.5905 | -0.0094 [-0.0149, -0.0035] |
| K_uid_behavior | 428 | 115,573 | 0.6115 | +0.0126 [+0.0085, +0.0176] |
