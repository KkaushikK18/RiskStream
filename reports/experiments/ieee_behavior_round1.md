# IEEE-CIS behaviour-feature experiment

Validation only (the test split is not read). Same LightGBM hyperparameters for every variant (2 sets from the ieee-1.0.0 tuning run). Warm-up = training rows with day < 30 removed (their 30-day windows are incomplete); cap = hours since last transaction undefined beyond 720 h.

| variant | features | train rows | mean val PR-AUC | vs A (paired bootstrap, 95% CI) |
|---|---:|---:|---:|---|
| A_v1.0_full | 449 | 115,573 | 0.5999 | baseline |
| B_no_behavior | 407 | 115,573 | 0.6168 | +0.0184 [+0.0134, +0.0233] |
| C_windowed_capped | 445 | 115,573 | 0.5940 | -0.0051 [-0.0084, -0.0017] |
| D_windowed_capped_warmup | 445 | 79,457 | 0.5877 | -0.0124 [-0.0167, -0.0079] |
| E_no_behavior_warmup | 407 | 79,457 | 0.6087 | +0.0098 [+0.0045, +0.0152] |
| F_uid_only_windowed_warmup | 426 | 79,457 | 0.6034 | +0.0039 [-0.0009, +0.0087] |
| G_full_warmup | 449 | 79,457 | 0.5899 | -0.0097 [-0.0145, -0.0055] |
