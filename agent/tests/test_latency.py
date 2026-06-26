import pytest
from tara_agent.latency import LatencyCollector

def test_total_is_eou_plus_ttft_plus_ttfb():
    c = LatencyCollector()
    c.record_turn(eou_delay=0.15, stt=0.12, llm_ttft=0.30, tts_ttfb=0.20)
    assert c._turns[0].total == pytest.approx(0.65)

def test_percentile_p50():
    c = LatencyCollector()
    for ttft in [0.1, 0.2, 0.3, 0.4, 0.5]:
        c.record_turn(0.0, 0.0, ttft, 0.0)
    assert c.percentile(50) == pytest.approx(0.3)

def test_breakdown_p50_reports_each_stage():
    c = LatencyCollector()
    c.record_turn(0.15, 0.12, 0.30, 0.20)
    c.record_turn(0.15, 0.12, 0.30, 0.20)
    b = c.breakdown_p50()
    assert set(b) == {"eou_delay", "stt", "llm_ttft", "tts_ttfb", "total"}
    assert b["total"] == pytest.approx(0.65)

def test_percentile_empty_raises():
    with pytest.raises(ValueError):
        LatencyCollector().percentile(50)
