# RiskStream notes: Redis cold start and the faster bootstrap

## Q: After a laptop / Docker restart, do I have to run the Redis command again by hand?

No. The rebuild starts on its own.

What I saw after the restart:

- All 10 containers came back by themselves about 3 minutes after Docker
  started. They are set to `restart: unless-stopped`.
- Redis started empty, so the `feature-consumer` container began rebuilding it:
  the rebuild lock was held, 70,038 keys were written so far, and the store was
  not marked ready yet.
- After the rebuild the API switches to Redis features automatically. Until
  then, scoring works from PostgreSQL.

### How to watch it finish

```powershell
python -m riskstream.cli stream-status
```

- Look for `"usable": true`.
- Or open http://127.0.0.1:8000/health/ready and wait for `online_features: ok`.
- Or follow the consumer's log until it prints `Online store bootstrapped`:

```powershell
docker compose logs -f feature-consumer
```

### When you do need to act

1. **You stopped the containers before restarting** (`docker compose stop` or
   `down`). They stay stopped. Start them again, and the rebuild runs
   automatically:

   ```powershell
   docker compose --profile app --profile observability --profile mlflow up -d
   ```

2. **You run the processes from the venv instead of Docker** (as in the Phase 4
   live test). Then rebuild Redis yourself:

   ```powershell
   python -m riskstream.cli online-bootstrap
   ```
---

## Q: Why did a cold start with an empty Redis take about 9 minutes?

### Where the 562 seconds went (before the fix)

Reconstructed from Redis key counts sampled every 10–35 seconds during the
restart test, plus the bootstrap's own log (not timed per stage at that point).

| stage | time | what it does |
|---|---:|---|
| Detect the empty Redis, restart the container | ~15 s | the consumer notices on its next heartbeat and exits |
| Export 6.26M rows from PostgreSQL | ~33 s | from the bootstrap log |
| Destination lifetime stats, 2.68M accounts | ~45 s | compute per account, write 32,768 hashes |
| Origin lifetime stats, 6.25M accounts | ~2 min | same work, 2.3 times as many accounts |
| **Window hashes, 229,604 accounts, plus duplicate markers** | **~5.5 min** | **the slow part** |

### Why the window stage was slow

`_write_windows` in `src/riskstream/features/online_bootstrap.py` split the data
with `partition_by("dest_id")`. That created 229,604 tiny Polars DataFrames, one
per destination account, and looped over each one row by row in Python.
Creating a DataFrame costs about a millisecond regardless of size, so 229,604 of
them added up to minutes. The Redis writes themselves were pipelined and cheap.

The lifetime stages had a smaller version of the same problem: they went
through 8.9M accounts one at a time in Python to format strings like
`total|last|cnt_last|prev`.

### Two other factors

- **Data size:** PaySim has 8.9M accounts because 99.85% of senders appear only
  once. Each one still needs its stats stored for training/serving parity.
- **Docker is slower:** the same bootstrap took 276 s from the venv in Phase 4
  and 517 s inside the container. The container shares the 2.8 GB Docker VM
  with PostgreSQL and Redis while running at about 350% CPU. Which resource is
  the bottleneck was not confirmed.

### Options considered

1. **Make the bootstrap code faster** (chosen). Build Redis field strings and
   bucket groupings with Polars column operations instead of Python loops.
2. **Let Redis save to disk** so a restart loads a snapshot. Not done: the
   snapshot could be behind the Kafka offsets already committed, which would
   silently lose events unless offsets are stored inside Redis too.
3. **Leave it as is.** It only happens when Redis loses its data, and scoring
   stays correct (from PostgreSQL) during the rebuild.

---

## Faster bootstrap: results

The bootstrap is about 4 times faster. After a Redis restart, Redis features
come back in **199 seconds instead of 562**, still with no manual step, and the
rebuilt data is identical to before.

### Per stage (same snapshot, same Redis)

| stage | before | after |
|---|---:|---:|
| destination lifetime stats (2.68M accounts) | 25 s | 10 s |
| origin lifetime stats (6.26M accounts) | 68 s | 26 s |
| window hashes (232,762 accounts) | 74 s | 4 s |
| **all write stages** | **174 s** | **45 s** |

### Real recovery inside Docker

| measurement | before | after |
|---|---:|---:|
| bootstrap inside the `feature-consumer` container | 517 s | **121 s** |
| Redis restarted empty → `"usable": true` | 562 s | **199 s** |
| consumer peak memory | 876 MB | 851 MB |

The 199 s includes the 40-second export from PostgreSQL, about 15 s to notice
the empty Redis and restart the consumer, and a few seconds for the relay and
consumer to report in.

### What changed

- **Before:** each of the 9 million accounts was formatted one at a time in
  Python and handed to the Redis client library. Most of the time went there,
  not into Redis.
- **After:** the Redis commands are built as raw protocol text with Polars
  column operations, then streamed in 8 MB chunks over one connection (the way
  `redis-cli --pipe` loads data). Python handles one string per Redis command,
  not one per account.

### How correctness was checked

- **Same output as before:** on the real data, 2,800 sampled keys and 119,957
  fields matched the old code exactly. A new test also compares the two byte for
  byte on synthetic data.
- **After the real rebuild:** Redis and PostgreSQL agree for step 613, with
  0 mismatches.
- **Tests:** all 192 pass, including new ones for non-ASCII account IDs and for
  the lock behaviour below.

### Further speedups not done

About two thirds of the remaining 121 s is the 40-second PostgreSQL export and
the 50-second origin stage. Only Redis persistence would remove the rebuild
itself, and that needs offsets stored in Redis to stay correct (see option 2
above).

---

## Problems found along the way, and their status

| # | problem | fix | evidence |
|---|---|---|---|
| 1 | The first faster version ran out of memory in the container: it built commands for all 6.26M origin accounts at once, and Linux killed it at the 1.5 GB limit. | **Fixed.** Commands are built in 8 groups. | Real rebuild in the container: peak 851 MB of 1.5 GB, 0 restarts, not killed. |
| 2 | A killed rebuild blocked recovery for up to 30 minutes: the dead process kept its lock for the full 30-minute expiry, so the restarted container waited. | **Fixed.** The lock expires after 30 s and is renewed every 10 s while a rebuild runs. | Two new tests: a dead process's lock expires and the next rebuild proceeds; a long rebuild keeps its lock. |
| 3 | 34 leftover test topics filled Redpanda's partition limit, so a Kafka test hung instead of failing. | **Fully fixed.** Fast failure with a clear message, the per-run topic leak fixed, and automatic safe cleanup of killed runs (details below). | Two full runs: 209 passed, 0 topics left; killed-run leftovers removed automatically. |

### Problem 3, fully fixed: automatic leftover cleanup

**What it is.** Kafka integration tests create temporary topics (and Redis test
keys) and delete them at the end. A run killed partway through (Ctrl+C, crash)
skipped that cleanup, and the leftovers piled up until Redpanda's partition
limit was full (it holds roughly 8 test runs' worth on this single-core broker).

**A second cause, found while fixing it.** Even *normal* runs were leaking
12 topics each. The test deleted its topics with an `AdminClient` that was
garbage-collected before the delete request reached the broker. This is why the
partition limit filled up so fast. Fixed by keeping the client alive and
waiting for each delete to finish.

**How it works now.** Before integration tests start, a cleanup step
(`tests/integration/conftest.py` + `leftovers.py`) deletes leftovers from killed
runs. Safety rules, each of which alone protects real data:

1. **Exact names only.** Test topics are named
   `test.<unix seconds>.<8 hex>.<kind>`, test Redis keys `rs:test:<unix seconds>:<8 hex>:…`.
   Real `transactions.*` topics and the real `rs:v1:*` keys can never match;
   look-alike names (e.g. `test.foo`) are left alone.
2. **Age guard.** Only leftovers older than 10 minutes are deleted (the time is in
   the name), so a test run still in progress in another terminal is never touched.
3. **Redis database 0 is refused** outright: that is the real online store.

Cleanup failures only produce a warning; they never hide real test results.

**Verified.**

| check | result |
|---|---|
| 16 safety unit tests (real names never selected, look-alikes, newline trick, db 0 refused) | pass (one caught a real bug: `$` in a regex also matched a name ending in a newline; fixed with `fullmatch`) |
| two full test runs in a row | 209 passed each, **0 test topics left** (before: 12 leaked per run) |
| test run killed with `Stop-Process -Force` | left 4 topics + 134 Redis keys |
| next run immediately after | left them alone (younger than 10 min) |
| next run after 10 minutes | deleted all 4 topics and 134 keys; the 4 real `transactions.*` topics and the 318,783 real store keys untouched |

No manual cleanup command is needed any more.
