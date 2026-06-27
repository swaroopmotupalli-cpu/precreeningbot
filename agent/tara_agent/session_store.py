import json
from dataclasses import dataclass

class SessionNotFound(Exception):
    pass

@dataclass(frozen=True)
class SessionBlob:
    contest_id: str
    candidate_id: str
    skills: list[str]
    resume_text: str
    jd_text: str
    max_questions: int
    recruiter_id: str = ""
    js_id: str = ""

async def load_session(redis, session_id: str) -> SessionBlob:
    raw = await redis.get(f"session:{session_id}")
    if raw is None:
        raise SessionNotFound(session_id)
    d = json.loads(raw)
    return SessionBlob(
        contest_id=d["contestId"], candidate_id=d["candidateId"],
        skills=d["skills"], resume_text=d["resumeText"],
        jd_text=d["jdText"], max_questions=d.get("maxQuestions", 12),
        recruiter_id=d.get("recruiterId", ""), js_id=d.get("jsId", ""),
    )
