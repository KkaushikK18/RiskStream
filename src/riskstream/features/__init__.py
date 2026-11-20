"""Feature engineering shared by offline training and online inference.

Layout:
* ``registry``  - feature specs (name, dtype, semantics, availability, version)
* ``derive``    - the ONE implementation of every feature formula, written as
                  Polars expressions over transaction columns + window primitives
* ``offline``   - computes window primitives for a whole dataset (point-in-time)
* ``online``    - computes the same primitives incrementally per entity
* ``parity``    - compares the two paths

Both paths produce identical *primitives* (counts and sums per time window) and
then call the same ``derive`` expressions, so no formula exists twice.
"""
