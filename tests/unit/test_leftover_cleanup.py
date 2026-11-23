"""Safety of the leftover cleanup: real topics and keys must never be selected."""

import pytest

from tests.integration import leftovers as lo

NOW = 2_000_000_000.0
OLD = int(NOW - lo.MAX_AGE_S - 10)
FRESH = int(NOW - 60)

REAL_TOPICS = ["transactions.ingested", "transactions.scored", "transactions.labeled", "transactions.dlq",
               "__consumer_offsets", "_schemas"]


def test_real_topics_are_never_selected_even_if_ancient():
    assert lo.stale_topics(REAL_TOPICS, now=NOW, max_age_s=0) == []


def test_only_old_test_topics_are_selected():
    old = list(lo.topic_names(f"{OLD}.0123abcd").values())
    fresh = list(lo.topic_names(f"{FRESH}.89abcdef").values())
    assert lo.stale_topics(REAL_TOPICS + old + fresh, now=NOW) == sorted(old)


@pytest.mark.parametrize("name", [
    "test.foo",                                   # loosely similar
    "test.56241500.scored",                       # old untimestamped format: no age, so never guessed
    f"test.{OLD}.0123abcd.other",                 # unknown kind
    f"test.{OLD}.0123ABCD.scored",                # uppercase hex
    f"test.{OLD}.0123abcd.scored.extra",          # trailing text
    f"xtest.{OLD}.0123abcd.scored",               # prefix
    f"transactions.scored.test.{OLD}.0123abcd",   # real-looking prefix
    f"test.{OLD}.0123abcd.scored\n",              # embedded newline
])
def test_lookalike_topic_names_are_left_alone(name):
    assert lo.stale_topics([name], now=NOW, max_age_s=0) == []


def test_real_namespace_with_test_suffix_is_never_selected():
    # A key in the real namespace that merely contains a test-looking part stays.
    assert lo.stale_keys([f"rs:v1:dw:rs:test:{OLD}:0123abcd:x"], now=NOW, max_age_s=0) == []


def test_real_redis_keys_are_never_selected():
    real = ["rs:v1:meta", "rs:v1:steps", "rs:v1:dw:C123", "rs:v1:da:42", "rs:v1:oa:42", "rs:v1:ev:abc",
            "rs:v1-lock:bootstrap", "rs:test", "rs:testing:1:a", "rs:test:foo:bar"]
    assert lo.stale_keys(real, now=NOW, max_age_s=0) == []


def test_only_old_test_namespace_keys_are_selected():
    old_ns, fresh_ns = lo.namespace(f"{OLD}.0123abcd"), lo.namespace(f"{FRESH}.89abcdef")
    old = [f"{old_ns}:meta", f"{old_ns}:dw:C1", f"{old_ns}-lock:bootstrap"]
    fresh = [f"{fresh_ns}:meta", f"{fresh_ns}-lock:bootstrap"]
    assert lo.stale_keys(old + fresh + ["rs:v1:meta"], now=NOW) == old


def test_database_zero_is_refused():
    for url in ("redis://:pw@127.0.0.1:6379/0", "redis://:pw@127.0.0.1:6379", "redis://127.0.0.1:6379/"):
        assert lo.redis_db(url) == 0
        with pytest.raises(ValueError, match="database 0"):
            lo.delete_stale_keys(url)
    assert lo.redis_db("redis://:pw@127.0.0.1:6379/15") == 15


def test_generated_names_match_the_patterns():
    tag = lo.new_tag(now=OLD)
    assert all(lo.stale_topics([t], now=NOW) == [t] for t in lo.topic_names(tag).values())
    assert lo.stale_keys([f"{lo.namespace(tag)}:meta"], now=NOW) == [f"{lo.namespace(tag)}:meta"]
