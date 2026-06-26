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
    assert s.say_timeout_seconds == 20.0

def test_settings_missing_required_raises(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(Exception):
        Settings(_env_file=None)
