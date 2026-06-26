from dataclasses import dataclass

@dataclass
class SessionOutcome:
    admitted: bool
    rejected_bucket: str | None
    turn_latencies_s: list
    total_tokens: int
    was_injected_noshow: bool
    was_real_participant: bool
    reclaimed_reason: str | None

def p95(latencies):
    if not latencies:
        return 0.0
    s = sorted(latencies)
    # nearest-rank
    import math
    rank = max(1, math.ceil(0.95 * len(s)))
    return s[rank - 1]

def stage_report(stage_name, concurrency, outcomes):
    admitted = [o for o in outcomes if o.admitted]
    rejected = [o for o in outcomes if not o.admitted]
    rej_by_bucket = {}
    for o in rejected:
        rej_by_bucket[o.rejected_bucket] = rej_by_bucket.get(o.rejected_bucket, 0) + 1
    all_lat = [l for o in admitted for l in o.turn_latencies_s]
    # false reclaim = a REAL participant that got reclaimed (not an injected no-show)
    false_reclaims = sum(
        1 for o in outcomes
        if o.was_real_participant and o.reclaimed_reason == "false" and not o.was_injected_noshow
    )
    tps = sorted(o.total_tokens for o in admitted if o.total_tokens > 0)
    tps_p50 = tps[len(tps) // 2] if tps else 0
    return {
        "stage": stage_name, "concurrency": concurrency,
        "p95_turn_latency": p95(all_lat),
        "rejection_rate": (len(rejected) / len(outcomes)) if outcomes else 0.0,
        "rejections_by_bucket": rej_by_bucket,
        "false_reclaims": false_reclaims,
        "tokens_per_session_p50": tps_p50,
    }

def false_reclaim_slope(stage_reports):
    pts = [(r["concurrency"], r["false_reclaims"]) for r in stage_reports]
    n = len(pts)
    if n < 2:
        return 0.0
    sx = sum(x for x, _ in pts); sy = sum(y for _, y in pts)
    sxy = sum(x * y for x, y in pts); sxx = sum(x * x for x, _ in pts)
    denom = n * sxx - sx * sx
    return 0.0 if denom == 0 else (n * sxy - sx * sy) / denom

def calibrated_budgets(stage_reports, target_pod_sessions):
    # tokens-per-session p50 across stages × target throughput → TPM budget headroom.
    tps = max((r["tokens_per_session_p50"] for r in stage_reports), default=0)
    healthy = [r for r in stage_reports if r["p95_turn_latency"] <= 1.5 and r["false_reclaims"] == 0]
    max_ok_conc = max((r["concurrency"] for r in healthy), default=0)
    return {
        "gemini_tpm_budget_basis_tokens_per_session": tps,
        "sessions_per_pod_max_before_degrade": max_ok_conc,
        "target_pod_sessions_assumed": target_pod_sessions,
        "warm_pool_note": "set WARM_POOL_MIN_PODS if false_reclaim_slope climbs across stages",
    }
