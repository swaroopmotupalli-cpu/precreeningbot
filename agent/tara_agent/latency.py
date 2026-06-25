from dataclasses import dataclass
from statistics import quantiles

@dataclass
class TurnLatency:
    eou_delay: float
    stt: float
    llm_ttft: float
    tts_ttfb: float
    total: float

class LatencyCollector:
    def __init__(self):
        self._turns: list[TurnLatency] = []

    def record_turn(self, eou_delay, stt, llm_ttft, tts_ttfb):
        # Perceived time-to-first-audio: STT streams DURING speech, so it is
        # reported but not summed; the clock the candidate feels is
        # EOU delay + LLM first token + TTS first byte.
        total = eou_delay + llm_ttft + tts_ttfb
        self._turns.append(TurnLatency(eou_delay, stt, llm_ttft, tts_ttfb, total))

    def percentile(self, p: float) -> float:
        if not self._turns:
            raise ValueError("no turns recorded")
        vals = sorted(t.total for t in self._turns)
        if len(vals) == 1:
            return vals[0]
        # nearest-rank on the 100-quantile cut points
        cuts = quantiles(vals, n=100, method="inclusive")
        return cuts[int(p) - 1]

    def _stage_p50(self, attr) -> float:
        vals = sorted(getattr(t, attr) for t in self._turns)
        mid = len(vals) // 2
        return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2

    def breakdown_p50(self) -> dict:
        return {a: self._stage_p50(a)
                for a in ("eou_delay", "stt", "llm_ttft", "tts_ttfb", "total")}
