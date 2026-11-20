# ADR 0003: Point-in-time features from shared window primitives

Status: accepted (Phase 2)

## Context

Behavioral features must not use events at or after the prediction time, and
the same feature must be computed identically in offline training and online
serving. The specification forbids implementing a feature formula twice.

PaySim facts that shape the design (from `reports/eda/paysim/eda_summary.md`):

* Time resolution is one hour. Rows inside the same step have a file order,
  but that order is not a timestamp.
* 99.85% of origin accounts appear exactly once, so origin history is almost
  always empty.
* Destination accounts repeat: 64.44% of rows involve a destination seen more
  than once, up to 113 times.

## Decision

1. Windows are whole hours: 24 h and 168 h. For a transaction at step `s`,
   the window `W` covers steps `[s - W, s - 1]`. Same-step events, including
   the transaction itself, are excluded.
2. Behavioral features center on the destination account, with only all-time
   count and recency for the origin.
3. Both paths compute the same **primitives** (count, sum, sum of log1p, sum of
   squared log1p, TRANSFER count, last step). Every model-facing feature is
   derived from primitives by one set of Polars expressions in
   `features/derive.py`, used by both paths.
   * Offline: per-(entity, step) aggregates, running sums, and as-of joins.
   * Online: per-entity hourly buckets (`features/online.py`); Phase 4 moves the
     same buckets into Redis.
4. A parity test replays events through the online store and compares every
   feature with the offline table.
5. Undefined statistics (mean of zero events, std of one event) are NaN, never
   zero. Zero is used only where it is the true value (count of prior events).

## Consequences

* Excluding same-step events throws away some real signal (a burst of
  transactions inside one hour). This is the price of not trusting an ordering
  the data does not actually provide.
* Online features depend on the stream having delivered all events of earlier
  hours. The Phase 4 consistency model must state this.
* Features computed offline use all splits' events as history, so validation
  rows see training-period behavior, as a live system would. Labels are never
  read by feature code (tested).
