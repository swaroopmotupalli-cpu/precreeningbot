from tara_agent.metrics_export import TaraMetrics

def test_active_sessions_inc_dec():
    m = TaraMetrics()
    m.session_started(); m.session_started(); m.session_ended()
    body = m.render().decode()
    assert "active_sessions 1.0" in body

def test_turn_latency_observed():
    m = TaraMetrics()
    m.observe_turn(total_s=0.7, llm_ttft_s=0.3)
    body = m.render().decode()
    assert "turn_latency_seconds_count 1.0" in body
    assert "llm_ttft_seconds_count 1.0" in body

def test_sync_limiter_sets_reclaim_and_reject():
    m = TaraMetrics()
    m.sync_limiter(
        {"reclaim_no_show": 2, "reclaim_crash": 1, "reclaim_false": 0,
         "reject_global": 5, "reject_gemini_tpm": 3,
         "reject_stt_streams": 0, "reject_tts_streams": 0,
         "count_global": 0, "count_gemini_tpm": 0,
         "count_stt_streams": 0, "count_tts_streams": 0},
        caps={"global": 10, "gemini_tpm": 6, "stt_streams": 10, "tts_streams": 10},
    )
    body = m.render().decode()
    assert 'lease_reclaims_total{reason="no_show"} 2.0' in body
    assert 'false_reclaims_total 0.0' in body
    assert 'bucket_rejections_total{bucket="gemini_tpm"} 3.0' in body

def test_sync_limiter_bucket_utilization():
    m = TaraMetrics()
    m.sync_limiter(
        {"reclaim_no_show": 0, "reclaim_crash": 0, "reclaim_false": 0,
         "reject_global": 0, "reject_gemini_tpm": 0,
         "reject_stt_streams": 0, "reject_tts_streams": 0,
         "count_global": 0, "count_gemini_tpm": 3,
         "count_stt_streams": 0, "count_tts_streams": 0},
        caps={"global": 10, "gemini_tpm": 6, "stt_streams": 10, "tts_streams": 10},
    )
    body = m.render().decode()
    assert 'bucket_utilization{bucket="gemini_tpm"} 0.5' in body

def test_false_reclaims_is_distinct_counter():
    m = TaraMetrics()
    m.sync_limiter({"reclaim_no_show": 0, "reclaim_crash": 0, "reclaim_false": 4,
                    "reject_global": 0, "reject_gemini_tpm": 0,
                    "reject_stt_streams": 0, "reject_tts_streams": 0}, caps={})
    body = m.render().decode()
    assert "false_reclaims_total 4.0" in body
