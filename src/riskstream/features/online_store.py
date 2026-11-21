"""Redis online feature store.

Holds the same window primitives as ``features.offline`` / the PostgreSQL
history query, maintained incrementally from ``transactions.scored`` events.
The model-facing formulas stay in ``features.derive``; this module only
produces primitives.

Key layout (``ns`` = configured namespace, e.g. ``rs:v1``):

* ``{ns}:dw:{dest_id}`` - hash of window buckets for one destination:
  ``{step}:c|s|l|q|t`` = count, sum(amount), sum(log1p), sum(log1p^2),
  TRANSFER count. Buckets older than the entity's newest step minus the longest
  window are pruned on write; a wall-clock TTL is a safety net only.
* ``{ns}:da:{b}`` / ``{ns}:oa:{b}`` - lifetime stats for destination / origin
  accounts, packed into ``stat_buckets`` hashes (bucket = crc32(id) mod N) so
  Redis stores them as compact listpacks. Value ``total|last|cnt_last|prev``:
  total events, newest step, events at the newest step, newest step before it.
* ``{ns}:ev:{event_id}`` - dedup marker (SET NX EX) for redelivered events.
* ``{ns}:steps`` - applied-event count per step (replay barrier, monitoring).
* ``{ns}:meta`` - bootstrap marker, window floor, heartbeats and lag reports.

Every event is applied by one Lua script, so dedup, all counter updates and
pruning are atomic. Concurrent consumers on different partitions are safe.
All keys of one event live on one Redis instance; Redis Cluster would need
hash tags and a different layout.

Read path: one pipelined round trip (window hash, two lifetime fields, meta).
A read is refused (``FeatureStoreNotUsable``) instead of guessed when:
the store was never bootstrapped, the pipeline is not demonstrably live, the
query is for a step older than data already applied for the entity (late
query), or the window data is incomplete for the requested step.
"""

from __future__ import annotations

import math
import time
import zlib
from dataclasses import dataclass

import redis

from riskstream.config import StreamingConfig
from riskstream.features.derive import primitive_columns, window_primitive_names

STATS = ("c", "s", "l", "q", "t")  # matches window_primitive_names order


class FeatureStoreError(RuntimeError):
    """Redis unreachable or failing."""


class FeatureStoreNotUsable(RuntimeError):
    """Redis answered, but its data must not be used for this request."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


APPLY_LUA = """
-- KEYS: 1 dedupe, 2 dest window, 3 dest lifetime bucket, 4 orig lifetime bucket, 5 meta, 6 steps
-- ARGV: 1 dedupe_ttl, 2 step, 3 amount, 4 log_amount, 5 log_amount_sq, 6 is_transfer,
--       7 dest_id, 8 orig_id, 9 max_window, 10 window_ttl_s, 11 now_ms
-- Returns 1 applied, 0 duplicate, -1 store not bootstrapped (caller retries later).
if redis.call('HEXISTS', KEYS[5], 'bootstrap_completed_at_ms') == 0 then
  return -1
end
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) == false then
  return 0
end
local step = tonumber(ARGV[2])
local maxw = tonumber(ARGV[9])

local function lifetime(key, id)
  local v = redis.call('HGET', key, id)
  local total, last, cl, prev
  if not v then
    total, last, cl, prev = 1, step, 1, -1
  else
    local a, b, c, d = string.match(v, '^(%-?%d+)|(%-?%d+)|(%-?%d+)|(%-?%d+)$')
    total, last, cl, prev = tonumber(a) + 1, tonumber(b), tonumber(c), tonumber(d)
    if step == last then
      cl = cl + 1
    elseif step > last then
      prev, last, cl = last, step, 1
    elseif step > prev then
      prev = step
    end
  end
  redis.call('HSET', key, id, string.format('%d|%d|%d|%d', total, last, cl, prev))
  return last
end

local newest = lifetime(KEYS[3], ARGV[7])
lifetime(KEYS[4], ARGV[8])

-- window buckets: skip events already outside every future query's window
if step >= newest - maxw then
  local p = tostring(step) .. ':'
  redis.call('HINCRBYFLOAT', KEYS[2], p .. 'c', 1)
  redis.call('HINCRBYFLOAT', KEYS[2], p .. 's', ARGV[3])
  redis.call('HINCRBYFLOAT', KEYS[2], p .. 'l', ARGV[4])
  redis.call('HINCRBYFLOAT', KEYS[2], p .. 'q', ARGV[5])
  redis.call('HINCRBYFLOAT', KEYS[2], p .. 't', ARGV[6])
  if step == newest then
    local floor = newest - maxw
    local fields = redis.call('HKEYS', KEYS[2])
    for _, f in ipairs(fields) do
      local s = tonumber(string.match(f, '^(%-?%d+):'))
      if s and s < floor then redis.call('HDEL', KEYS[2], f) end
    end
  end
  redis.call('EXPIRE', KEYS[2], ARGV[10])
end

redis.call('HINCRBY', KEYS[6], ARGV[2], 1)
redis.call('HINCRBY', KEYS[5], 'applied_events', 1)
redis.call('HSET', KEYS[5], 'last_applied_at_ms', ARGV[11])
local cur = tonumber(redis.call('HGET', KEYS[5], 'max_applied_step') or '-1')
if step > cur then redis.call('HSET', KEYS[5], 'max_applied_step', ARGV[2]) end
return 1
"""

META_FIELDS = ("bootstrap_completed_at_ms", "window_floor", "relay_heartbeat_ms",
               "relay_oldest_unpublished_age_s", "consumer_heartbeat_ms", "consumer_lag",
               "consumer_state", "layout", "scored_lag_broker", "consumer_stalled_s")


def stat_bucket(entity_id: str, buckets: int) -> int:
    """Stable across processes and languages (unlike Python's hash())."""
    return zlib.crc32(entity_id.encode("utf-8")) % buckets


def parse_lifetime(value: str | bytes | None) -> tuple[int, int, int, int] | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode()
    total, last, cnt_last, prev = (int(x) for x in value.split("|"))
    return total, last, cnt_last, prev


def lifetime_before(stats: tuple[int, int, int, int] | None, step: int) -> tuple[float, int | None]:
    """(count of events with step < ``step``, newest such step). Raises on late queries."""
    if stats is None:
        return 0.0, None
    total, last, cnt_last, prev = stats
    if step > last:
        return float(total), last
    if step == last:
        return float(total - cnt_last), (None if prev < 0 else prev)
    raise FeatureStoreNotUsable("late_query", f"query step {step} is older than applied step {last}")


def window_primitives(window_hash: dict, step: int, windows: tuple[int, ...]) -> dict[str, float]:
    """Sum window buckets with bucket step in [step - W, step - 1] for each W."""
    buckets: dict[int, dict[str, float]] = {}
    for raw_field, raw_value in window_hash.items():
        field = raw_field.decode() if isinstance(raw_field, bytes) else raw_field
        s, stat = field.split(":")
        buckets.setdefault(int(s), {})[stat] = float(raw_value)
    out: dict[str, float] = {}
    for w in windows:
        totals = dict.fromkeys(STATS, 0.0)
        for s, values in buckets.items():
            if step - w <= s <= step - 1:
                for k in STATS:
                    totals[k] += values.get(k, 0.0)
        for name, k in zip(window_primitive_names("dest", w), STATS, strict=True):
            # counts are exact integers; float sums may carry rounding noise
            out[name] = float(round(totals[k])) if k in ("c", "t") else totals[k]
    return out


@dataclass(frozen=True)
class Freshness:
    usable: bool
    reason: str
    detail: str = ""


def assess_freshness(meta: dict[str, str | None], cfg: StreamingConfig, now_ms: int | None = None) -> Freshness:
    """Whether the store's data may be used right now (see module docstring)."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    if not meta.get("bootstrap_completed_at_ms"):
        return Freshness(False, "not_bootstrapped")
    if meta.get("layout") != cfg.namespace:
        return Freshness(False, "layout_mismatch", f"store layout {meta.get('layout')!r}")
    timeout_ms = cfg.heartbeat_timeout_s * 1000
    for who in ("relay", "consumer"):
        hb = meta.get(f"{who}_heartbeat_ms")
        if hb is None or now_ms - int(hb) > timeout_ms:
            return Freshness(False, f"{who}_heartbeat_stale")
    if meta.get("consumer_state") not in (None, "running"):
        return Freshness(False, "consumer_not_running", str(meta.get("consumer_state")))
    age = meta.get("relay_oldest_unpublished_age_s")
    if age is not None and float(age) > cfg.max_pipeline_delay_s:
        return Freshness(False, "relay_backlog", f"oldest unpublished event {float(age):.1f}s old")
    # Self-reported lag, then broker-measured lag (works even when the consumer is dead).
    for field in ("consumer_lag", "scored_lag_broker"):
        lag = meta.get(field)
        if lag is not None and int(lag) > cfg.max_consumer_lag_messages:
            return Freshness(False, "consumer_lag", f"{lag} messages ({field})")
    stalled = meta.get("consumer_stalled_s")
    if stalled is not None and float(stalled) > cfg.max_consumer_stall_s:
        return Freshness(False, "consumer_stalled", f"lagging without progress for {float(stalled):.1f}s")
    return Freshness(True, "ok")


class RedisFeatureStore:
    def __init__(self, client: redis.Redis, cfg: StreamingConfig, windows: tuple[int, ...]) -> None:
        self.r, self.cfg = client, cfg
        self.windows = tuple(sorted(windows))
        self.max_window = self.windows[-1]
        self.ns = cfg.namespace
        self._apply = self.r.register_script(APPLY_LUA)

    @classmethod
    def from_url(cls, url: str, cfg: StreamingConfig, windows: tuple[int, ...], *,
                 request_path: bool = False) -> RedisFeatureStore:
        kwargs = {"decode_responses": True, "health_check_interval": 30}
        if request_path:
            kwargs |= {"socket_timeout": cfg.redis_socket_timeout_s,
                       "socket_connect_timeout": cfg.redis_connect_timeout_s}
        return cls(redis.Redis.from_url(url, **kwargs), cfg, windows)

    # ------------------------------------------------------------------ keys

    def k_window(self, dest_id: str) -> str:
        return f"{self.ns}:dw:{dest_id}"

    def k_dest_stats(self, dest_id: str) -> str:
        return f"{self.ns}:da:{stat_bucket(dest_id, self.cfg.stat_buckets)}"

    def k_orig_stats(self, orig_id: str) -> str:
        return f"{self.ns}:oa:{stat_bucket(orig_id, self.cfg.stat_buckets)}"

    @property
    def k_meta(self) -> str:
        return f"{self.ns}:meta"

    @property
    def k_steps(self) -> str:
        return f"{self.ns}:steps"

    def k_event(self, event_id: str) -> str:
        return f"{self.ns}:ev:{event_id}"

    # ------------------------------------------------------------------ writes

    def apply(self, *, event_id: str, dest_id: str, orig_id: str, step: int, amount: float,
              txn_type: str, now_ms: int | None = None) -> bool:
        """Apply one event atomically. Returns False for a duplicate event id.

        Raises ``FeatureStoreNotUsable("not_bootstrapped")`` while a bootstrap is
        running or before the first one: the event must be retried later, not dropped.
        """
        log_amount = math.log1p(amount)
        try:
            applied = self._apply(
                keys=[self.k_event(event_id), self.k_window(dest_id), self.k_dest_stats(dest_id),
                      self.k_orig_stats(orig_id), self.k_meta, self.k_steps],
                args=[self.cfg.dedupe_ttl_s, step, repr(float(amount)), repr(log_amount),
                      repr(log_amount * log_amount), 1 if txn_type == "TRANSFER" else 0, dest_id, orig_id,
                      self.max_window, self.cfg.window_key_ttl_s, now_ms or int(time.time() * 1000)],
            )
        except redis.RedisError as exc:
            raise FeatureStoreError(str(exc)) from exc
        if int(applied) == -1:
            raise FeatureStoreNotUsable("not_bootstrapped", "apply refused until bootstrap completes")
        return int(applied) == 1

    def write_meta(self, **fields: object) -> None:
        try:
            self.r.hset(self.k_meta, mapping={k: str(v) for k, v in fields.items()})
        except redis.RedisError as exc:
            raise FeatureStoreError(str(exc)) from exc

    def is_bootstrapped(self) -> bool:
        try:
            return self.r.hget(self.k_meta, "bootstrap_completed_at_ms") is not None
        except redis.RedisError as exc:
            raise FeatureStoreError(str(exc)) from exc

    # ------------------------------------------------------------------ reads

    def read_meta(self) -> dict[str, str | None]:
        try:
            values = self.r.hmget(self.k_meta, META_FIELDS)
        except redis.RedisError as exc:
            raise FeatureStoreError(str(exc)) from exc
        return dict(zip(META_FIELDS, values, strict=True))

    def primitives(self, dest_id: str, orig_id: str, step: int, *, check_freshness: bool = True
                   ) -> dict[str, float | int | None]:
        try:
            pipe = self.r.pipeline(transaction=False)
            pipe.hgetall(self.k_window(dest_id))
            pipe.hget(self.k_dest_stats(dest_id), dest_id)
            pipe.hget(self.k_orig_stats(orig_id), orig_id)
            pipe.hmget(self.k_meta, META_FIELDS)
            window_hash, dest_raw, orig_raw, meta_values = pipe.execute()
        except redis.RedisError as exc:
            raise FeatureStoreError(str(exc)) from exc
        meta = dict(zip(META_FIELDS, meta_values, strict=True))
        if check_freshness:
            fresh = assess_freshness(meta, self.cfg)
            if not fresh.usable:
                raise FeatureStoreNotUsable(fresh.reason, fresh.detail)
        elif not meta.get("bootstrap_completed_at_ms"):
            raise FeatureStoreNotUsable("not_bootstrapped")

        floor = meta.get("window_floor")
        if floor is not None and step - self.max_window < int(floor):
            raise FeatureStoreNotUsable("before_window_floor",
                                        f"step {step} needs buckets before the bootstrap floor {floor}")
        dest_stats = parse_lifetime(dest_raw)
        dest_cnt, dest_last = lifetime_before(dest_stats, step)
        orig_cnt, orig_last = lifetime_before(parse_lifetime(orig_raw), step)
        if dest_stats is not None and not window_hash and dest_stats[1] >= step - self.max_window:
            # lifetime says the account was active inside the window, but the buckets are gone
            raise FeatureStoreNotUsable("window_missing", "window hash expired or lost")

        out: dict[str, float | int | None] = window_primitives(window_hash, step, self.windows)
        out |= {"dest_p_cnt_all": dest_cnt, "dest_p_last_step": dest_last,
                "orig_p_cnt_all": orig_cnt, "orig_p_last_step": orig_last}
        assert set(out) == set(primitive_columns(self.windows))
        return out

    def applied_count(self, step: int) -> int:
        return int(self.r.hget(self.k_steps, str(step)) or 0)

    def status(self) -> dict:
        info = self.r.info("memory")
        meta = self.r.hgetall(self.k_meta)
        return {"namespace": self.ns, "meta": meta, "used_memory_human": info.get("used_memory_human"),
                "maxmemory_human": info.get("maxmemory_human"),
                "freshness": assess_freshness({k: meta.get(k) for k in META_FIELDS}, self.cfg).__dict__}

    def clear_namespace(self, batch: int = 5000) -> int:
        removed = 0
        cursor = 0
        while True:
            cursor, keys = self.r.scan(cursor=cursor, match=f"{self.ns}:*", count=batch)
            if keys:
                removed += self.r.unlink(*keys)
            if cursor == 0:
                return removed
