# Validation report: ieee_cis

- Result: **PASSED**
- Rows: 590,540 total, 590,537 valid, 3 rejected (0.0005%; tolerance 0.1000%)

| Check | Severity | Failing rows | Result |
|---|---|---:|---|
| `canonical_schema`: columns and dtypes match | structural | 0 | pass |
| `unique_transaction_id`: transaction_id is unique | structural | 0 | pass |
| `unique_identity_id`: train_identity TransactionID is unique | structural | 0 | pass |
| `identity_orphans`: every identity row matches a transaction | structural | 0 | pass |
| `temporal_order`: TransactionDT never decreases in file order | structural | 0 | pass |
| `non_empty`: dataset has rows | structural | 0 | pass |
| `null_required_field`: a required field is null | row | 0 | pass |
| `non_positive_amount`: amount <= 0 | row | 0 | pass |
| `non_finite_amount`: amount is NaN or infinite | row | 0 | pass |
| `invalid_label`: is_fraud not in {0, 1} | row | 0 | pass |
| `time_before_reference`: TransactionDT < 86400 (day < 0) | row | 0 | pass |
| `duplicate_record`: exact repeat of an earlier record (all fields except the id) | row | 3 | FAIL |
| `no_identity_row`: transaction has no train_identity row | info | 446,307 | measured |
| `addr1_missing`: addr1 is null | info | 65,706 | measured |
| `d1_missing`: D1 is null | info | 1,269 | measured |
| `uid_undefined`: card1, addr1 or D1 null: the uid entity is undefined | info | 66,794 | measured |
| `amount_over_5000`: amount > 5,000 USD | info | 18 | measured |
