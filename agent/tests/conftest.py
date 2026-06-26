# agent/tests/conftest.py
import pytest
import redislite
import redis.asyncio as aioredis

@pytest.fixture
async def real_redis(tmp_path):
    # redislite runs a REAL redis-server (Lua/EVAL supported), per-test isolated.
    rdb = redislite.Redis(str(tmp_path / "t.rdb"))
    sock = rdb.socket_file
    client = aioredis.Redis(unix_socket_path=sock, decode_responses=True)
    yield client
    await client.aclose()
    rdb.close()
