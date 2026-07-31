import logging

from tara_agent.log_context import session_log


class _Blob:
    contest_id = "c1"
    js_id = "j1"
    recruiter_id = "r1"


def test_session_log_prefixes_every_message_with_the_three_ids(caplog):
    base = logging.getLogger("test.session_log")
    adapted = session_log(base, _Blob())
    with caplog.at_level(logging.INFO, logger="test.session_log"):
        adapted.info("question %d/%d: %s", 3, 12, "hello")
    assert len(caplog.records) == 1
    msg = caplog.records[0].getMessage()
    assert "[contest=c1 js=j1 rec=r1]" in msg
    assert "question 3/12: hello" in msg
