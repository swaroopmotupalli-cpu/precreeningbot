import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from tara_agent.gemini import make_classify_fn

class _Settings:
    gemini_api_key = "k"; gemini_model = "gemini-3.1-flash-lite"

async def test_classify_returns_text():
    fake_resp = MagicMock(text="Python, SQL")
    fake_client = MagicMock()
    fake_client.aio.models.generate_content = AsyncMock(return_value=fake_resp)
    with patch("tara_agent.gemini.genai.Client", return_value=fake_client):
        classify = make_classify_fn(_Settings())
        out = await classify("which skills?")
    assert out == "Python, SQL"
    fake_client.aio.models.generate_content.assert_awaited_once()


class _Settings:
    gemini_api_key = "x"
    gemini_model = "gemini-3.1-flash-lite"
    offpath_retry_attempts = 4
    retry_base_delay_ms = 1
    retry_max_delay_ms = 2


async def test_classify_retries_transient(monkeypatch):
    calls = {"n": 0}

    class _Resp:
        text = "Python"

    class _Models:
        async def generate_content(self, **kw):
            calls["n"] += 1
            if calls["n"] < 2:
                raise asyncio.TimeoutError()
            return _Resp()

    class _Aio:
        models = _Models()

    class _Client:
        aio = _Aio()

    monkeypatch.setattr("tara_agent.gemini.genai.Client", lambda **kw: _Client())
    fn = make_classify_fn(_Settings())
    out = await fn("prompt")
    assert out == "Python"
    assert calls["n"] == 2  # retried once
