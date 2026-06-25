from tara_agent.session_store import SessionBlob

def build_system_prompt(blob: SessionBlob) -> str:
    skills = ", ".join(blob.skills)
    return (
        "You are Tara, a professional voice interviewer. Conduct a spoken "
        "interview for the role described below. Ask ONE question at a time, "
        "in plain conversational text. Do NOT output JSON, lists, scores, or "
        "any structured data — only the next thing you would say aloud. Adapt "
        "follow-ups to the candidate's previous answer. Systematically cover "
        f"every must-have skill: {skills}. Keep questions concise.\n\n"
        f"=== JOB DESCRIPTION ===\n{blob.jd_text}\n\n"
        f"=== CANDIDATE RESUME ===\n{blob.resume_text}\n"
    )

def build_coverage_prompt(skills: list[str], answer: str) -> str:
    return (
        "Given this candidate answer, return ONLY a comma-separated list of "
        "which of these skills the answer substantively demonstrated (or "
        "'none'). Skills: " + ", ".join(skills) + "\n\nAnswer: " + answer
    )
