import logging


class _SessionLogAdapter(logging.LoggerAdapter):
    """Prefixes every log line with the session's contest/jobseeker/recruiter
    ids, so console output can be traced back to a specific candidate/session
    at a glance, regardless of which module or function emitted the line."""

    def process(self, msg, kwargs):
        c = self.extra
        return f"[contest={c['contest_id']} js={c['js_id']} rec={c['recruiter_id']}] {msg}", kwargs


def session_log(base_logger: logging.Logger, blob) -> logging.LoggerAdapter:
    return _SessionLogAdapter(base_logger, {
        "contest_id": blob.contest_id,
        "js_id": blob.js_id,
        "recruiter_id": blob.recruiter_id,
    })
