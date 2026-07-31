from tara_agent.interview import should_end

def test_continues_under_cap():
    assert should_end(3, 12) is False

def test_ends_when_cap_hit():
    assert should_end(12, 12) is True

def test_ends_past_cap():
    assert should_end(13, 12) is True

def test_zero_cap_ends_immediately():
    assert should_end(0, 0) is True
