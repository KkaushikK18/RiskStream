# Validation report: paysim

- Result: **PASSED**
- Rows: 6,362,620 total, 6,362,620 valid, 0 rejected (0.0000%; tolerance 0.1000%)

| Check | Severity | Failing rows | Result |
|---|---|---:|---|
| `canonical_schema`: columns and dtypes match | structural | 0 | pass |
| `unique_transaction_id`: transaction_id is unique | structural | 0 | pass |
| `temporal_order`: step never decreases in source order | structural | 0 | pass |
| `non_empty`: dataset has rows | structural | 0 | pass |
| `null_required_field`: required field is null | row | 0 | pass |
| `unknown_transaction_type`: type not in ['CASH_IN', 'CASH_OUT', 'DEBIT', 'PAYMENT', 'TRANSFER'] | row | 0 | pass |
| `unknown_dest_type`: dest_type not in ['CUSTOMER', 'MERCHANT'] | row | 0 | pass |
| `negative_amount`: amount < 0 | row | 0 | pass |
| `non_finite_numeric`: NaN or infinite amount/balance | row | 0 | pass |
| `negative_balance`: a balance is negative | row | 0 | pass |
| `invalid_label`: is_fraud not in {0, 1} | row | 0 | pass |
| `invalid_flag`: is_flagged_fraud not in {0, 1} | row | 0 | pass |
| `invalid_step`: step < 1 | row | 0 | pass |
| `duplicate_record`: exact repeat of an earlier record (all fields except id) | row | 0 | pass |
| `zero_amount`: amount == 0 | info | 16 | measured |
| `orig_balance_mismatch`: orig_balance_before - amount != orig_balance_after (beyond 0.01) | info | 1,476,150 | measured |
| `dest_balance_mismatch`: dest_balance_before + amount != dest_balance_after (beyond 0.01), customer dests | info | 2,037,152 | measured |
| `amount_exceeds_orig_balance`: amount > orig_balance_before | info | 4,079,080 | measured |
| `merchant_dest_zero_balances`: merchant destination with both balances 0 (PaySim does not track merchant balances) | info | 2,151,495 | measured |
