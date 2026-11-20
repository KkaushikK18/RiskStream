# ADR 0004: Excluded columns and the balance-feature ablation

Status: accepted (Phase 2)

## Context

EDA on PaySim's TRANSFER and CASH_OUT rows (the only types that contain fraud) shows:

| signal | legit | fraud |
|---|---:|---:|
| amount equals the full origin balance | 0.00% | 97.82% |
| destination balances both 0 | 0.06% | 49.63% |
| origin balance before is 0 | 47.37% | 0.50% |

`isFlaggedFraud` is the output of the simulator's own rule (16 rows).

## Decision

Excluded from model features:

* `is_flagged_fraud`: another rule's output.
* `orig_balance_after`, `dest_balance_after`: post-transaction values.
* `dest_balance_before`: belongs to the receiving account, which in a real
  system is often at another institution.
* identifiers, `event_seq` and absolute `step`.

Kept: `orig_balance_before` and features derived from it, including
`amount_drains_orig_balance`. The sender's own institution knows the balance
before authorizing a payment, so this is available at inference time.

Because "amount equals the full balance" separates fraud almost perfectly in
PaySim, and that pattern comes from how the simulator generates fraud, a
second LightGBM model (`lightgbm_no_balance`) is trained without any balance
feature. It is a diagnostic, not a deployment candidate.

## Consequences

* Headline metrics with balance features describe the simulator's fraud
  mechanism. They should not be read as evidence that the approach would work
  on real fraud.
* The no-balance ablation shows how much signal the behavioral and
  transaction features carry on their own.
