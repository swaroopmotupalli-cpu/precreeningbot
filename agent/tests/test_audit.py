from tara_agent.audit import audit_transcript


def test_clean_transcript_has_no_problems():
    lines = [
        {"seq": 0, "speaker": "tara", "text": "Hi, first question?"},
        {"seq": 1, "speaker": "candidate", "text": "My answer."},
        {"seq": 2, "speaker": "tara", "text": "Follow up?"},
    ]
    assert audit_transcript(lines) == []


def test_detects_seq_gap():
    lines = [
        {"seq": 0, "speaker": "tara", "text": "q"},
        {"seq": 2, "speaker": "candidate", "text": "a"},  # missing seq 1
    ]
    probs = audit_transcript(lines)
    assert any("contiguous" in p or "gap" in p for p in probs)


def test_detects_bad_speaker():
    lines = [{"seq": 0, "speaker": "robot", "text": "q"}]
    assert any("speaker" in p for p in audit_transcript(lines))


def test_detects_candidate_first():
    lines = [
        {"seq": 0, "speaker": "candidate", "text": "a"},
        {"seq": 1, "speaker": "tara", "text": "q"},
    ]
    assert any("first" in p for p in audit_transcript(lines))


def test_empty_transcript_is_clean():
    assert audit_transcript([]) == []
