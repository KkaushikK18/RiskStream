# ADR 0002: Chronological split at steps 595 / 669

Status: accepted (Phase 1)

## Context

PaySim's `step` is a simulated hour from 1 to 743. Transaction volume is very
uneven: about 91% of rows fall in steps 1-400, and later hours have only a few
thousand rows each. Fraud, by contrast, occurs at a roughly constant rate of
200-300 cases per simulated day across the whole month.

Random splitting would mix future events into training, and would place rows
of the same destination account on both sides of the split at random times.

## Decision

Split by `step` with inclusive upper bounds configured in `configs/data.yaml`:

| split | steps | rows | fraud cases |
|---|---|---:|---:|
| train | 1-595 | 6,244,474 (98.14%) | 6,571 (80.01%) |
| val | 596-669 | 49,620 (0.78%) | 804 (9.79%) |
| test | 670-743 | 68,526 (1.08%) | 838 (10.20%) |

(Measured by `prepare-data`; see `data/processed/paysim/splits/split_summary.json`.)

The boundaries put roughly 80/10/10 of the fraud cases in each split, which is
what limits how precisely validation and test metrics can be measured.

## Consequences

* Fraud prevalence differs strongly between periods: 0.105% in training,
  1.62% in validation, 1.22% in test. This is a property of the simulator's
  late-month volume collapse, not of the split code. Probabilities from a model
  trained on the training period must be recalibrated (done on validation).
* Validation and test periods are small in rows, so metrics have wide
  uncertainty. 804 and 838 positives are enough for PR-AUC to be meaningful,
  but small differences between models are not.
* About 53% of destination accounts seen in validation and 54% in test also
  appear in training. This entity overlap is expected in a time split and is
  what behavioral features rely on. No future information crosses the boundary,
  because features are computed point-in-time (ADR 0003).
