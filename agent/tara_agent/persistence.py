import asyncio
import logging

log = logging.getLogger("tara.persistence")

async def end_interview(*, say_fn, transcript_store, mongo_write_fn, room,
                        contest_id, candidate_id, say_timeout, write_timeout,
                        on_finally):
    try:
        try:
            await asyncio.wait_for(say_fn(), timeout=say_timeout)
        except (asyncio.TimeoutError, Exception) as e:
            log.warning("closing say failed/hung: %s", e)

        lines = await transcript_store.assemble()
        doc = {"room": room, "contestId": contest_id,
               "candidateId": candidate_id, "transcript": lines}
        try:
            await asyncio.wait_for(mongo_write_fn(doc), timeout=write_timeout)
        except (asyncio.TimeoutError, Exception) as e:
            log.error("transcript persist failed/hung (recoverable from Redis): %s", e)
    finally:
        on_finally()
