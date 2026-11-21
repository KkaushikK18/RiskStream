"""Decision policy loaded from a versioned policy file (written by ``tune-policy``).

The policy maps a calibrated fraud probability to:

    p <  review            -> APPROVE (risk LOW)
    review <= p < decline  -> REVIEW  (risk MEDIUM)
    p >= decline           -> DECLINE (risk HIGH)

It is a simulated payment decision policy, not a validated fraud rule set.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path


class PolicyError(RuntimeError):
    pass


@dataclass(frozen=True)
class Decision:
    decision: str
    risk_level: str


@dataclass(frozen=True)
class DecisionPolicy:
    policy_version: str
    model_version: str
    review_threshold: float
    decline_threshold: float
    approval_status: str
    path: str

    @classmethod
    def load(cls, path: Path) -> DecisionPolicy:
        path = Path(path)
        if not path.exists():
            raise PolicyError(f"Policy file not found: {path}")
        try:
            data = json.loads(path.read_text())
            review = float(data["thresholds"]["review"])
            decline = float(data["thresholds"]["decline"])
            policy = cls(
                policy_version=str(data["policy_version"]),
                model_version=str(data["model_version"]),
                review_threshold=review,
                decline_threshold=decline,
                approval_status=str(data.get("approval", {}).get("status", "unapproved")),
                path=str(path),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise PolicyError(f"Invalid policy file {path}: {exc}") from exc
        if not (0.0 <= review <= decline) or not math.isfinite(decline):
            raise PolicyError(f"Invalid thresholds in {path}: review={review}, decline={decline}")
        return policy

    def decide(self, probability: float) -> Decision:
        if not (0.0 <= probability <= 1.0):
            raise ValueError(f"probability out of range: {probability}")
        if probability >= self.decline_threshold:
            return Decision("DECLINE", "HIGH")
        if probability >= self.review_threshold:
            return Decision("REVIEW", "MEDIUM")
        return Decision("APPROVE", "LOW")
