from tara_agent.prompts import build_system_prompt, build_coverage_prompt
from tara_agent.session_store import SessionBlob

def test_system_prompt_includes_static_block():
    blob = SessionBlob("c", "u", ["Python", "SQL"], "RESUME_X", "JD_Y", 12)
    p = build_system_prompt(blob)
    assert "Python" in p and "SQL" in p
    assert "RESUME_X" in p and "JD_Y" in p
    assert "one question at a time" in p.lower()
    assert "json" in p.lower()  # must instruct: do NOT output JSON

def test_coverage_prompt_lists_skills_and_answer():
    p = build_coverage_prompt(["Python", "SQL"], "I used pandas")
    assert "Python" in p and "SQL" in p and "I used pandas" in p
