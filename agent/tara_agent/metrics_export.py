from prometheus_client import (
    CollectorRegistry, Gauge, Counter, Histogram, generate_latest,
)

_LAT_BUCKETS = (0.3, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 3.0, 5.0)

class TaraMetrics:
    def __init__(self, registry: CollectorRegistry | None = None):
        self.registry = registry or CollectorRegistry()
        self.active_sessions = Gauge("active_sessions", "Live interviews on this pod", registry=self.registry)
        self.warm_pods_available = Gauge("warm_pods_available", "Warm spare pods", registry=self.registry)
        self.bucket_utilization = Gauge("bucket_utilization", "count/cap per bucket", ["bucket"], registry=self.registry)
        self.bucket_rejections = Counter("bucket_rejections", "Admission rejections by bucket", ["bucket"], registry=self.registry)
        self.lease_reclaims = Counter("lease_reclaims", "Lease reclaims by reason", ["reason"], registry=self.registry)
        self.false_reclaims = Counter("false_reclaims", "Real participants evicted mid-join (HARD GATE)", registry=self.registry)
        self.turn_latency = Histogram("turn_latency_seconds", "End-of-utterance to first audio", buckets=_LAT_BUCKETS, registry=self.registry)
        self.llm_ttft = Histogram("llm_ttft_seconds", "LLM time-to-first-token", buckets=_LAT_BUCKETS, registry=self.registry)
        self._last = {}  # absolute-value tracking for Redis-sourced counters

    def session_started(self): self.active_sessions.inc()
    def session_ended(self): self.active_sessions.dec()

    def observe_turn(self, total_s: float, llm_ttft_s: float):
        self.turn_latency.observe(total_s)
        self.llm_ttft.observe(llm_ttft_s)

    def _bump(self, counter, key, new):
        delta = new - self._last.get(key, 0)
        if delta > 0:
            counter.inc(delta)
        self._last[key] = new

    def sync_limiter(self, metrics: dict, caps: dict):
        for reason in ("no_show", "crash", "false"):
            self._bump(self.lease_reclaims.labels(reason=reason), f"rc_{reason}",
                       metrics.get(f"reclaim_{reason}", 0))
        self.false_reclaims  # ensure registered
        self._bump(self.false_reclaims, "false", metrics.get("reclaim_false", 0))
        for b in ("global", "gemini_tpm", "stt_streams", "tts_streams"):
            self._bump(self.bucket_rejections.labels(bucket=b), f"rej_{b}",
                       metrics.get(f"reject_{b}", 0))
            cap = (caps or {}).get(b, 0)
            if cap > 0:
                count = metrics.get(f"count_{b}", 0)
                self.bucket_utilization.labels(bucket=b).set(min(1.0, count / cap))

    def render(self) -> bytes:
        return generate_latest(self.registry)
