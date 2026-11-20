"""Seeded random search over the spaces declared in ``configs/model.yaml``.

Random search was chosen over a Bayesian optimizer to avoid another
dependency: with 6-10 trials per model the difference is small, and the
sampled parameters are fully reproducible from the seed.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np


def sample_params(space: dict[str, dict], rng: np.random.Generator) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, spec in space.items():
        (kind, arg), = spec.items()
        if kind == "choice":
            out[name] = arg[int(rng.integers(len(arg)))]
        elif kind == "int":
            out[name] = int(rng.integers(arg[0], arg[1] + 1))
        elif kind == "float":
            out[name] = float(rng.uniform(arg[0], arg[1]))
        elif kind == "loguniform":
            out[name] = float(math.exp(rng.uniform(math.log(arg[0]), math.log(arg[1]))))
        else:
            raise ValueError(f"Unknown search spec {kind} for {name}")
    return out


def trial_params(space: dict, fixed: dict, n_trials: int, seed: int) -> list[dict[str, Any]]:
    rng = np.random.default_rng(seed)
    return [fixed | sample_params(space, rng) for _ in range(n_trials)]
