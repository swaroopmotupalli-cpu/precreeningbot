# agent/tests/test_config.py
import pytest
from tara_agent.config import Settings

def test_settings_load_from_env(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
    }.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.gemini_model == "gemini-3.1-flash-lite"
    assert s.interview_languages == ["en-IN", "en-US"]
    assert s.max_questions == 12
    assert s.end_debounce_seconds == 6.0
    assert s.say_timeout_seconds == 20.0

def test_settings_missing_required_raises(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(Exception):
        Settings(_env_file=None)

def test_admission_settings_defaults(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
    }.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.max_global_sessions == 110
    assert s.reservation_lease_ttl == 45
    assert s.heartbeat_interval == 15
    assert s.heartbeat_lease_ttl == 60

def test_metrics_settings_defaults(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
    }.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.metrics_port == 9091
    assert s.metric_scrape_interval == 5

def test_admission_settings_override(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
        "MAX_GLOBAL_SESSIONS": "200", "GEMINI_TPM_BUDGET": "42",
    }.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.max_global_sessions == 200
    assert s.gemini_tpm_budget == 42

def test_phase4_retry_defaults(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
    }.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.hot_retry_attempts == 2
    assert s.offpath_retry_attempts == 4

def test_reconnect_grace_default(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
    }.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.reconnect_grace_seconds == 25  # default, < reservation_lease_ttl (45)

def test_reconnect_grace_must_be_under_lease(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
        "RECONNECT_GRACE_SECONDS": "60", "RESERVATION_LEASE_TTL": "45",
    }.items():
        monkeypatch.setenv(k, v)
    with pytest.raises(Exception):
        Settings()

def test_stt_model_region_defaults(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
    }.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.stt_model == "chirp_3"
    assert s.stt_location == "asia-southeast1"
    assert s.interview_languages == ["en-IN", "en-US"]
