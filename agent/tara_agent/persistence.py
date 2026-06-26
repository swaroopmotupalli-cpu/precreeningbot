import asyncio
import logging
from tara_agent.retry import retry_async

log = logging.getLogger("tara.persistence")

async def end_interview(*, say_fn, transcript_store, mongo_write_fn, room,
                        contest_id, candidate_id, say_timeout, write_timeout,
                        on_finally,
                        offpath_retry_attempts: int = 4,
                        retry_base_delay_ms: int = 50,
                        retry_max_delay_ms: int = 400):
    try:
        try:
            await asyncio.wait_for(say_fn(), timeout=say_timeout)
        except (asyncio.TimeoutError, Exception) as e:
            log.warning("closing say failed/hung: %s", e)

        lines = await transcript_store.assemble()
        doc = {"room": room, "contestId": contest_id,
               "candidateId": candidate_id, "transcript": lines}
        try:
            async def _write():
                return await asyncio.wait_for(mongo_write_fn(doc), timeout=write_timeout)
            await retry_async(
                _write,
                attempts=offpath_retry_attempts,
                base_delay=retry_base_delay_ms / 1000.0,
                max_delay=retry_max_delay_ms / 1000.0,
            )
        except (asyncio.TimeoutError, Exception) as e:
            log.error("transcript persist failed/hung (recoverable from Redis): %s", e)
    finally:
        on_finally()
