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

---

# Change log

Each entry: which phase it belongs to, what was there before, what problem that
caused, what changed, and how it was checked.

## 1. Faster Redis bootstrap

- **Phase:** 5 follow-up (the online store from Phase 4, running in the Phase 5 containers).
- **Before:** `online-bootstrap` formatted each of about 9 million accounts one at a
  time in Python and sent them through the Redis client library.
- **Problem:** after every Docker or laptop restart Redis came back empty, and Redis
  features were unusable for 562 s (about 9 minutes). The API stayed correct by falling
  back to PostgreSQL, but slower.
- **Change:** Redis commands are built as raw protocol text with Polars column operations
  and streamed in 8 MB chunks over one connection (like `redis-cli --pipe`).
  Files: `src/riskstream/features/online_bootstrap.py`, `scripts/bench_bootstrap.py`.
- **Result:** recovery 562 s → 199 s; bootstrap in the container 517 s → 121 s;
  peak memory 851 MB. Output identical to the old code (2,800 keys / 119,957 fields
  compared), 0 mismatches against PostgreSQL for step 613.

## 2. Out of memory in the first faster version

- **Phase:** 5 follow-up (part of change 1).
- **Before:** the new writer built the commands for all 6.26M origin accounts at once.
- **Problem:** the feature-consumer container was killed at its 1.5 GB limit (exit 137).
- **Change:** lifetime stats are built and sent in 8 bucket groups.
- **Result:** peak 851 MB, no restarts.

## 3. Rebuild lock held by a dead process

- **Phase:** 5 follow-up (part of change 1).
- **Before:** the rebuild lock in Redis expired after 30 minutes.
- **Problem:** if a rebuild was killed, the restarted container waited up to 30 minutes
  for the dead process's lock.
- **Change:** the lock is a 30 s lease renewed every 10 s while the rebuild runs.
- **Result:** two tests: a dead lock expires and the next rebuild proceeds; a long rebuild keeps its lock.

## 4. Leftover test topics filled Redpanda

- **Phase:** 4 test infrastructure (found during the Phase 5 follow-up).
- **Before:** Kafka integration tests deleted their topics with an `AdminClient` that was
  garbage-collected before the delete reached the broker, and killed runs skipped cleanup.
- **Problem:** 12 topics leaked per normal run; after a few runs Redpanda's partition limit
  was full and a Kafka test hung instead of failing.
- **Change:** the client is kept alive until each delete finishes; topic names carry a
  timestamp (`test.<unix seconds>.<8 hex>.<kind>`); a cleanup step before integration
  tests removes leftovers older than 10 minutes, matching exact patterns only, and refuses
  Redis database 0. Files: `tests/integration/conftest.py`, `tests/integration/leftovers.py`.
- **Result:** 209 passed twice in a row with 0 leftovers; a killed run's 4 topics and
  134 keys were removed automatically after 10 minutes; real data untouched.

## 5. Phase 6: monitoring, alerts, dashboards, runbooks

- **Phase:** 6.
- **Before:** Prometheus scraped the app and Redpanda, Grafana had only a data source.
  There were no alert rules, no dashboards, no Redis or PostgreSQL server metrics, and
  JSON logs did not say which service wrote them.
- **Problem:** nobody would notice a failure (Redis down, consumer stuck, 5xx errors)
  unless they were looking at logs, and there was no documented response.
- **Change:**
  - `infrastructure/prometheus/alerts.yml`: 4 recording rules and 22 alerts, each with a
    severity and a runbook link; promtool unit tests in `tests/alerts_test.yml`.
  - redis-exporter and postgres-exporter containers; PostgreSQL role `riskstream_monitor`
    (statistics only, no table access).
  - New API gauges: `riskstream_online_store_usable`, `riskstream_online_store_state{reason}`,
    `riskstream_db_pool_capacity`.
  - Three Grafana dashboards generated by `scripts/build_dashboards.py`;
    `scripts/check_dashboards.py` runs every query against the live Prometheus.
  - JSON logs now include `service`, `instance`, `environment`.
  - Docs: `docs/monitoring.md`, `docs/incident_runbooks.md`, ADR 0010.
- **Result:** 8/8 targets up, 26 rules healthy, 89 panel queries with 0 errors,
  3 dashboards provisioned; pausing Redis fired RedisDown (1 min) and OnlineStoreUnusable
  (10 min); stopping the relay fired ServiceDown; all resolved after recovery.
  232 tests pass.

### Problems found while verifying Phase 6

| # | before | problem | fix |
|---|---|---|---|
| 5a | `roles.sql` removes the right to connect to the database from everyone, then gives it back to the app role only | postgres-exporter could log in but not open the database, so `pg_up` was 0 and PostgresDown fired while PostgreSQL was healthy | `GRANT CONNECT ON DATABASE riskstream TO riskstream_monitor`; a test checks every login role has the grant |
| 5b | redis-exporter waited up to 15 s for Redis; Prometheus gives a scrape 5 s | with Redis paused the whole scrape timed out, `redis_up` vanished, and RedisDown never fired (only "exporter down") | exporter timeouts set to 2 s; the scrape now returns `redis_up 0` in about 2 s; a test checks the timeouts stay below 5 s |
| 5c | the 5xx-ratio and fallback-ratio rules divided "errors" by "all requests" | with no errors yet the error series does not exist, so the ratio was empty instead of 0 and its graph was blank | the rules use `or vector(0)` for the error side |
## 6. Grafana dashboard fixes after review

- **Phase:** 6 (found by checking dashboard screenshots).
- **Before:** stat panels read a range query and showed the last value seen in the
  hour; quantiles were shown however few requests there were; the decisions graph was
  stacked with an almost transparent fill.
- **Problem:** the API board showed `/v1/score p95 = 1.900 s` in red while idle. It came
  from only 8 requests, one of which took 1.1 s, and the panel kept showing it 10 minutes
  later. The 5xx ratio showed two boxes. The decisions graph looked like everything was
  REVIEW (really 798 APPROVE, 5 DECLINE, 0 REVIEW). Flat panels had axes like 10000%.
- **Change:** quantiles only with at least 20 requests in the window ("no traffic"
  otherwise); stats show the current value; `sum(...) or vector(0)` gives one series;
  decisions not stacked plus a separate "Declines and reviews" panel; fixed axis ranges;
  `ApiHighLatency` alert ignores fewer than 0.1 req/s.
  Files: `scripts/build_dashboards.py`, `infrastructure/prometheus/alerts.yml`,
  `infrastructure/prometheus/tests/alerts_test.yml`, `tests/unit/test_observability_config.py`.
- **Result:** after 6 requests the panels say "no traffic"; during 2,680 requests p95 is
  35 ms. 0 query errors, promtool SUCCESS, 238 tests pass.
- **Slow first request:** caused by swap on the 2.8 GB Docker VM after 40 idle minutes
  (every stage slow, not one dependency). Not a code bug; no change made.

---

# Q: How do I take RiskStream from PaySim to real-world use?

**Context.** The model is trained on PaySim, which is simulated data. The system
around it is built the way a production system would be, but a real deployment
would need training on a company's own transactions and real business costs in
the decision policy. This project is for learning, so the question is: how would
this scale for real-world use, how do you keep improving the model and the data,
and how do you handle many more users efficiently?

**Short answer.** It is not one change but three separate tracks: get better
data, improve the model in a continuous loop, and scale the system. Real fraud
teams work on all three at once, and each one can be practised in this project.

## 1. The data problem

**Why PaySim limits you:** its fraud is too easy to catch. The ablation showed
it: with balance columns the model scored PR-AUC 0.9999, without them only 0.62.
That gap comes from the way the simulator was built, not from a real fraud
pattern. Real data is messier in ways PaySim doesn't have:

| Real data has | What it means |
|---|---|
| Rich signals | device, IP address, merchant, card, location, login behaviour |
| Late and noisy labels | a chargeback arrives weeks later; some fraud is never reported |
| Selection bias | a declined payment never gets a label, so you never learn whether that decline was right |
| Adaptive fraudsters | when you block one pattern, they switch to another |
| Privacy rules | card numbers and personal data need protection (PCI DSS, GDPR) |

**What you can do as a learner:**

| Dataset | Why use it |
|---|---|
| **IEEE-CIS Fraud Detection** (Kaggle, from Vesta) | Real anonymised e-commerce payments: about 590k transactions, about 3.5% fraud, hundreds of features (card, device, email domain). **Best next step**: the adapter layer was built so IEEE-CIS can plug in as a second dataset. |
| **Bank Account Fraud (BAF)** (Feedzai, NeurIPS 2022) | Realistic synthetic account-opening fraud, with versions that include bias, so you can study fairness |
| **Credit Card Fraud** (Kaggle, ULB) | 284,807 real card transactions with 492 frauds; features are anonymised by PCA, so there is little to engineer |

Training on IEEE-CIS would show that the pipeline isn't tied to PaySim, and the
results would be believable numbers, not 0.9999.

## 2. Improving the model continuously

At a company the model is never "finished". It runs in a loop (Phases 7 and 8):

```text
score ──► collect labels ──► measure real performance + drift ──► retrain
  ▲                                                                  │
  └──── canary ◄── shadow ◄── challenger must beat champion ◄────────┘
```

What matters most in practice:

1. **Measure on labelled outcomes, not on the training score.** Precision,
   recall and money saved, tracked over time.
2. **Fix the selection-bias gap.** Send a small, controlled share of would-be
   declines to review instead. That share then gets labels, so you learn whether
   the declines were right.
3. **Backtest by time.** A new model is compared with the current one on recent
   data it never saw: the same chronological-split rule already used here.
4. **Better features matter more than new algorithms.** For example:
   - device and IP velocity;
   - graph features, such as how many accounts share a device, or whether this
     receiver is connected to known fraud;
   - merchant risk.
5. **Use real costs.** Finance supplies the cost of a missed fraud, a wrong
   decline and an analyst review. The policy file already has this structure;
   only the numbers would change.
6. **Explain decisions.** Reason codes (for example from SHAP) tell analysts and
   regulators why a payment was declined.

## 3. Scaling to more users

**Rule one: measure before scaling.** Phase 9 (load testing) finds the real
limits. Today we only know that one API instance gives about 35 ms p95 at the
low rates tested; its maximum throughput is not known yet.

How each part scales once the limits are known:

| Part | How it scales | Why it's possible here |
|---|---|---|
| **API** | More copies behind a load balancer; autoscale on CPU or latency | It is stateless, and idempotency by `transaction_id` makes retries across copies safe |
| **Model** | Usually not the bottleneck: inference takes a few milliseconds | Bigger models can get their own inference service later |
| **PostgreSQL** | Connection pooler (PgBouncer); split the predictions table by month; read replicas for analysts; archive old data | Writes are the usual first bottleneck |
| **Redis** | Redis Cluster, splitting keys by account across nodes | Keys are already per account |
| **Kafka** | More partitions plus more consumer copies | Consumers run in parallel up to the partition count, currently 6 |
| **Operations** | SLOs such as "99.9% of requests under 100 ms", on-call rotation, cost tracking | The Phase 6 monitoring is the base for this |

Beyond that, very large systems run in several regions and put a separate fast
rules engine (for example for blocklists) in front of the model.

## 4. Suggested path for this project

| Step | What | What you get |
|---|---|---|
| 1 | **Phase 7** (labels, drift, retraining) on PaySim, with a simulated delay before labels arrive | You learn the improvement loop |
| 2 | **Add IEEE-CIS** as a second dataset through the existing adapter design, and retrain | Realistic results and richer feature work |
| 3 | **Phase 9 load test** (k6 or Locust) | Real numbers on how far one laptop setup goes and where it breaks |
| 4 | **Phase 10 on AWS**, only if you want cloud experience and only with explicit approval (it costs money) | Experience running managed services |

---

## 7. IEEE-CIS added as a second dataset (offline)

- **Phase:** between Phase 6 and Phase 7 (repeats Phases 1-2 for real data).
- **Before:** only PaySim (simulated). Its fraud is a simulator artifact, so test PR-AUC
  was 0.9999: an impressive-looking number that says nothing about real fraud.
- **Problem:** the pipeline had never been shown to work on real data, and the results
  were not believable.
- **Change:**
  - adapter, validation, EDA and day-based split for IEEE-CIS
    (`src/riskstream/data/adapters/ieee_cis.py`, `src/riskstream/data/ieee_cis.py`);
  - 449 features: source columns, de-duplicated V columns (281 of 339), category
    encodings learned from training rows only, and point-in-time behaviour per card1 and
    per client (uid) (`src/riskstream/features/ieee_cis.py`, `configs/ieee_cis_features.yaml`);
  - per-dataset model config (`configs/model_ieee_cis.yaml`); training, calibration,
    cost policy and model bundles reused unchanged;
  - CLI: `--dataset ieee_cis` for prepare-data, build-features, train, tune-policy;
  - docs: `docs/ieee_cis.md`, ADR 0011; 15 new tests.
- **Problems found on the way:**
  - the identity join reordered rows (Polars does not keep row order in joins by default),
    so validation reported 2,332 time-order violations; fixed with `maintain_order="left"`,
    with a regression test;
  - 3 exact duplicate records in the source were quarantined.
- **Result:** test PR-AUC **0.530** (ROC-AUC 0.911) for the selected LightGBM; 61.8% of
  test fraud flagged while reviewing or declining 6.0% of payments. Full training run
  56 min, peak memory 2.5 GB, with Docker stopped.
- **Finding to act on:** without our card1 behaviour features, validation PR-AUC is
  *higher* (0.624 vs 0.595, same hyperparameters). They follow total traffic volume, which
  changes week to week. The next feature version should fix or drop them.
---

## 8. IEEE-CIS behaviour features fixed (feature version ieee-1.1.0)

- **Phase:** IEEE-CIS offline work, follow-up to entry 7.
- **Before (1.0.0):** 42 history features per card group (card1) and per client (uid).
  Removing them *raised* validation PR-AUC (0.624 vs 0.595).
- **Problem:** the features meant to describe past behaviour were making the model worse,
  and the first explanation (card1 counts follow traffic volume) was only a guess.
- **How it was investigated** (validation only; the test split was never read):
  1. a drift check (PSI, train vs validation) found 4 features that shift with elapsed
     time (e.g. `card1_txn_count_all`: median 219 in train, 695 in validation);
  2. removing those, capping "hours since last" at 30 days and dropping the first 30 days
     did **not** help (-0.005 and -0.012), so the volume explanation was wrong;
  3. testing each entity showed the **card1 features are the harmful part** (+0.013 when
     removed, 95% CI +0.009 to +0.018), with or without Vesta's V columns. card1 is a group
     of many clients, and patterns learned from groups did not hold in later weeks;
  4. new per-client "novelty" features (same email, device, browser as before?) were
     built and verified (0 mismatches in 2,100 values), but added nothing (+0.000).
- **Change:** card1 history features removed; uid history and novelty kept and measured;
  the feature set is now chosen on validation like a hyperparameter
  (`lightgbm` and `lightgbm_no_behavior` get the same 10 trials). Validation chose the
  model without our history features. Files: `configs/ieee_cis_features.yaml`,
  `configs/model_ieee_cis.yaml`, `src/riskstream/features/ieee_cis.py`,
  `scripts/ieee_behavior_experiment.py`, `reports/experiments/ieee_behavior_round{1,2,3}.md`.
- **Result:** test PR-AUC 0.530 -> **0.556**; fraud flagged on test 61.8% -> 65.7%;
  recall at 1% FPR 43.4% -> 47.8%. The test set has now been used twice (disclosed).
  256 tests pass (3 new).
- **Lesson:** Vesta's own columns (C counts, D time deltas, V) are already built from the
  full payment history we never see, so our history features had nothing to add here.
  They will matter in an online version, where those provider columns would not exist.