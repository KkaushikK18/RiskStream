# ADR 0011: IEEE-CIS as a second dataset, offline only

Status: accepted (after Phase 6)

## Context

PaySim is simulated and its fraud is too easy to separate (test PR-AUC 0.9999). IEEE-CIS
is real, anonymized e-commerce data, but its columns share nothing with PaySim's.

## Decisions

1. **Own schema, shared core names.** IEEE-CIS keeps its source column names (card1,
   V258, ...) and adds the core columns the training code needs under the PaySim names
   (`transaction_id`, `event_seq`, `amount`, `is_fraud`). Training, calibration, the
   cost policy and model bundles are reused unchanged. Rejected: forcing IEEE-CIS into
   PaySim's canonical schema, which would discard most of its information.
2. **Offline only.** The serving, streaming and Redis layers stay PaySim-format. Bringing
   IEEE-CIS online needs a new API version and online feature code, and is worth doing only
   after offline results justify it and Phase 8 adds multi-model serving.
3. **Encoders and V selection fitted on training rows only**, saved next to the feature
   tables and included in the feature schema hash, so a bundle cannot be loaded with
   features built from different encoders.
4. **Split by day**: 0-120 / 121-150 / 151-181, about 71 / 14 / 15% of rows, similar fraud
   rates. Rejected: Kaggle's test file, which has no labels.
5. **Per-dataset model config** (`configs/model_<dataset>.yaml` replaces keys of
   `model.yaml`), so each dataset has its own models and ablations.
6. **Same cost matrix as PaySim**, labelled as an untested assumption. Choosing e-commerce
   costs is a business decision.

## Consequences

* A second, harder benchmark: test PR-AUC 0.530 (feature version 1.0.0), then 0.556 (1.1.0).
* 1.1.0 (2026-11-24): card-group (card1) history features removed after a validation-only
  experiment showed them harmful; the remaining client (uid) history and novelty features
  became a feature-set choice made on validation, which selected the model without them.
  The test set has been used twice; reported as such in docs/ieee_cis.md.
* Peak memory 2.5 GB during training: Docker must be stopped on the development laptop.
