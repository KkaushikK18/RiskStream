from riskstream.config import StreamingConfig, get_settings
from riskstream.features.online_store import RedisFeatureStore, assess_freshness
from riskstream.features.registry import get_registry
cfg = StreamingConfig.load()
s = RedisFeatureStore.from_url(get_settings().redis_url, cfg, get_registry().config.windows)
m = s.read_meta()
print("meta:", {k: m[k] for k in ("consumer_state", "consumer_lag", "scored_lag_broker", "consumer_stalled_s")})
print("freshness:", assess_freshness(m, cfg).__dict__)
