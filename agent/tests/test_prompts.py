from datetime import datetime, timedelta, timezone

from tara_agent.prompts import build_system_prompt, build_coverage_prompt, _first_name, _ist_greeting
from tara_agent.session_store import SessionBlob


class _FakeSettings:
    max_questions = 12


def test_system_prompt_includes_static_block():
    blob = SessionBlob("c", "u", ["Python", "SQL"], "RESUME_X", "JD_Y")
    p = build_system_prompt(blob, _FakeSettings())
    assert "Python" in p and "SQL" in p
    assert "RESUME_X" in p and "JD_Y" in p
    assert "one question" in p.lower()
    assert "question mark" in p.lower()  # "?" is the real-question signal downstream

def test_system_prompt_mentions_good_to_have_skills_when_present():
    blob = SessionBlob("c", "u", ["Python"], "R", "J", good_to_have_skills=["Docker", "Kubernetes"])
    p = build_system_prompt(blob, _FakeSettings())
    assert "Docker" in p and "Kubernetes" in p

def test_system_prompt_omits_good_to_have_block_when_absent():
    blob = SessionBlob("c", "u", ["Python"], "R", "J")
    p = build_system_prompt(blob, _FakeSettings())
    assert "GOOD-TO-HAVE SKILLS" not in p

def test_system_prompt_uses_candidate_first_name_and_role():
    blob = SessionBlob("c", "u", ["Python"], "R", "J", candidate_name="Ada Lovelace", job_title="Backend Engineer")
    p = build_system_prompt(blob, _FakeSettings())
    assert "Ada" in p
    assert "Lovelace" not in p  # only the first name is read aloud
    assert "Backend Engineer" in p

def test_system_prompt_states_exact_question_count():
    blob = SessionBlob("c", "u", ["Python"], "R", "J")
    p = build_system_prompt(blob, _FakeSettings())
    assert "exactly 12 questions" in p.lower()
    # no AI-decided ending — the code counts and cuts her off, never a tool call
    assert "end_interview" not in p

def test_coverage_prompt_lists_skills_and_answer():
    p = build_coverage_prompt(["Python", "SQL"], "I used pandas")
    assert "Python" in p and "SQL" in p and "I used pandas" in p

def test_first_name_takes_first_word_only():
    assert _first_name("Ada Lovelace") == "Ada"
    assert _first_name("") == "there"
    assert _first_name("   ") == "there"

def test_ist_greeting_bands():
    tz = timezone(timedelta(hours=5, minutes=30))
    assert _ist_greeting(datetime(2026, 1, 1, 9, 0, tzinfo=tz))[0] == "Good morning"
    assert _ist_greeting(datetime(2026, 1, 1, 14, 0, tzinfo=tz))[0] == "Good afternoon"
    assert _ist_greeting(datetime(2026, 1, 1, 20, 0, tzinfo=tz))[0] == "Good evening"
