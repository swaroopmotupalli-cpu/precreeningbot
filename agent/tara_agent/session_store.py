import json
from dataclasses import dataclass, field

class SessionNotFound(Exception):
    pass

@dataclass(frozen=True)
class SessionBlob:
    contest_id: str
    candidate_id: str
    skills: list[str]
    resume_text: str
    jd_text: str
    recruiter_id: str = ""
    js_id: str = ""
    # Asked about live too (system prompt) — no longer gates when the
    # interview ends (it always runs to Settings.max_questions), only what
    # Tara prioritizes asking about.
    good_to_have_skills: list[str] = field(default_factory=list)
    candidate_name: str = ""
    job_title: str = ""

async def load_session(redis, session_id: str) -> SessionBlob:
    raw = await redis.get(f"session:{session_id}")
    if raw is None:
        raise SessionNotFound(session_id)
    d = json.loads(raw)
    return SessionBlob(
        contest_id=d["contestId"], candidate_id=d["candidateId"],
        skills=d["skills"], resume_text=d["resumeText"],
        jd_text=d["jdText"],
        recruiter_id=d.get("recruiterId", ""), js_id=d.get("jsId", ""),
        good_to_have_skills=d.get("goodToHaveSkills", []),
        candidate_name=d.get("candidateName", ""),
        job_title=d.get("jobTitle", ""),
    )
