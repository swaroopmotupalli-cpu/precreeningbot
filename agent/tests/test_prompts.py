from tara_agent.prompts import build_system_prompt, build_coverage_prompt
from tara_agent.session_store import SessionBlob

def test_system_prompt_includes_static_block():
    blob = SessionBlob("c", "u", ["Python", "SQL"], "RESUME_X", "JD_Y", 12)
    p = build_system_prompt(blob)
    assert "Python" in p and "SQL" in p
    assert "RESUME_X" in p and "JD_Y" in p
    assert "one question at a time" in p.lower()
    assert "do not output json" in p.lower()  # must instruct: do NOT output JSON
    assert "question mark" in p.lower()  # "?" is the real-question signal downstream

def test_system_prompt_mentions_good_to_have_skills_when_present():
    blob = SessionBlob("c", "u", ["Python"], "R", "J", 12, good_to_have_skills=["Docker", "Kubernetes"])
    p = build_system_prompt(blob)
    assert "Docker" in p and "Kubernetes" in p

def test_system_prompt_omits_good_to_have_line_when_absent():
    blob = SessionBlob("c", "u", ["Python"], "R", "J", 12)
    p = build_system_prompt(blob)
    assert "good-to-have" not in p.lower()

def test_coverage_prompt_lists_skills_and_answer():
    p = build_coverage_prompt(["Python", "SQL"], "I used pandas")
    assert "Python" in p and "SQL" in p and "I used pandas" in p
