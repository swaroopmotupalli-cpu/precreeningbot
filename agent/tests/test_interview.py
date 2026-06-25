from tara_agent.interview import should_end

def test_ends_when_all_skills_covered():
    assert should_end({"python", "sql"}, ["python", "sql"], 3, 12) is True

def test_ends_when_cap_hit():
    assert should_end(set(), ["python"], 12, 12) is True

def test_continues_when_uncovered_and_under_cap():
    assert should_end({"python"}, ["python", "sql"], 5, 12) is False

def test_required_empty_means_cap_only():
    assert should_end(set(), [], 0, 12) is True  # nothing to cover
