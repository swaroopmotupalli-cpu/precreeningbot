from aggregate import SessionOutcome, p95, stage_report, false_reclaim_slope

def _ok(lat, tokens=1000):
    return SessionOutcome(True, None, lat, tokens, False, True, None)

def test_p95():
    assert p95([0.1*i for i in range(1, 101)]) == 9.5  # 95th of 0.1..10.0

def test_stage_report_rejection_and_false_reclaim():
    outs = [_ok([0.7, 0.8]) for _ in range(8)]
    outs.append(SessionOutcome(False, "gemini_tpm", [], 0, False, False, None))  # rejected
    # a REAL participant that got reclaimed = false reclaim (hard gate)
    outs.append(SessionOutcome(True, None, [0.9], 1000, False, True, "false"))
    r = stage_report("stage2", 10, outs)
    assert r["rejections_by_bucket"]["gemini_tpm"] == 1
    assert r["false_reclaims"] == 1
    assert 0.0 < r["rejection_rate"] < 1.0

def test_injected_noshow_is_not_a_false_reclaim():
    outs = [SessionOutcome(True, None, [], 0, True, False, "no_show")]  # injected no-show
    r = stage_report("s", 1, outs)
    assert r["false_reclaims"] == 0

def test_false_reclaim_slope_flat_is_zero():
    reports = [{"concurrency": 50, "false_reclaims": 0},
               {"concurrency": 120, "false_reclaims": 0},
               {"concurrency": 200, "false_reclaims": 0}]
    assert false_reclaim_slope(reports) == 0.0
