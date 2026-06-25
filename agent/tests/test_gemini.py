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
