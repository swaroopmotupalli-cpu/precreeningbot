import asyncio
import logging
from tara_agent.retry import retry_async

log = logging.getLogger("tara.persistence")

async def end_interview(*, say_fn, transcript_store, mongo_write_fn, room,
                        contest_id, candidate_id, say_timeout, write_timeout,
                        on_finally,
                        recruiter_id: str = "", js_id: str = "", enqueue_fn=None,
                        offpath_retry_attempts: int = 4,
                        retry_base_delay_ms: int = 50,
                        retry_max_delay_ms: int = 400):
    try:
        try:
            await asyncio.wait_for(say_fn(), timeout=say_timeout)
        except (asyncio.TimeoutError, Exception) as e:
            log.warning("closing say failed/hung: %s", e)

        lines = await transcript_store.assemble()
        from tara_agent.audit import audit_transcript
        problems = audit_transcript(lines)
        if problems:
            log.warning("transcript audit problems for room %s: %s", room, problems)
        doc = {"room": room, "contestId": contest_id,
               "candidateId": candidate_id, "recruiterId": recruiter_id,
               "jsId": js_id, "transcript": lines}
        wrote = False
        try:
            async def _write():
                return await asyncio.wait_for(mongo_write_fn(doc), timeout=write_timeout)
            await retry_async(
                _write,
                attempts=offpath_retry_attempts,
                base_delay=retry_base_delay_ms / 1000.0,
                max_delay=retry_max_delay_ms / 1000.0,
            )
            wrote = True
        except (asyncio.TimeoutError, Exception) as e:
            log.error("transcript persist failed/hung (recoverable from Redis): %s", e)
        # Best-effort: enqueue for async scoring only after a successful write.
        if wrote and enqueue_fn is not None:
            try:
                await enqueue_fn(room)
            except Exception as e:  # never let enqueue break teardown
                log.warning("score enqueue failed for %s (re-queue later): %s", room, e)
    finally:
        on_finally()
