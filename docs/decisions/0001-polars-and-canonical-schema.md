# ADR 0001: Polars data pipeline with a canonical transaction schema

Status: accepted (Phase 1)

## Context

PaySim has 6,362,620 rows (about 470 MB of CSV). The development machine has
5.9 GB of RAM. The pipeline must also accept other datasets later (IEEE-CIS),
whose columns share nothing with PaySim.

## Decision

* Use Polars lazy scans for ingestion, validation, EDA, splitting and offline
  features. Convert to NumPy only at the model boundary.
* Each dataset gets an adapter that maps source columns to one canonical
  schema (`src/riskstream/data/schema.py`). Everything downstream uses canonical
  names only.
* Validation is custom Polars expressions instead of Pandera, to avoid a
  dependency and keep checks lazy.

## Consequences

* The full PaySim pipeline (ingest, validate, EDA, split) runs in about 42 s on
  the development laptop.
* Adding IEEE-CIS means writing a new adapter and deciding how its columns map
  (or don't) to canonical fields. Its features will need their own definitions.
* Pandas-based tools must receive explicit conversions.
