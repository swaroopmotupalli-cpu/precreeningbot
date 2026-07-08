from functools import lru_cache
from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore",
                                      env_parse_none_str="")
    gemini_api_key: str
    gemini_model: str = "gemini-3.1-flash-lite"
    google_application_credentials: str
    redis_url: str
    mongodb_uri: str
    livekit_url: str
    livekit_api_key: str
    livekit_api_secret: str
    interview_languages: list[str] = ["en-IN", "en-US"]
    # STT model/region. chirp_3 in asia-southeast1 is markedly more accurate on
    # Indian-accented English than latest_long, AND supports multi-language
    # (en-IN+en-US) — chirp models reject multi-language in us-central1, and
    # chirp_2/chirp_3 do not exist in "global". Tune stt_location to the region
    # nearest the deployment (chirp_3 multilingual regions incl. asia-southeast1).
    stt_model: str = "chirp_3"
    stt_location: str = "asia-southeast1"
    tts_voice: str = "en-IN-Chirp3-HD-Erinome"
    max_questions: int = 12
    # After the question cap (or full coverage) is reached, wait this long with
    # no new candidate speech before actually saying goodbye — so a candidate
    # still mid-answer isn't cut off. Each new candidate turn resets the wait.
    # Was 3.0s — too short: a candidate can easily pause longer than that while
    # still mid-thought, which fired the goodbye (and wrote the final report)
    # before they were actually done, permanently losing whatever they said next.
    end_debounce_seconds: float = 6.0
    transcript_ttl_seconds: int = 7200
    say_timeout_seconds: float = 20.0
    mongo_write_timeout_seconds: float = 10.0
    # admission caps / budgets (Phase 2) — load-test calibrates these (Phase 3)
    max_global_sessions: int = 110
    gemini_tpm_budget: int = 60
    stt_stream_budget: int = 100
    tts_stream_budget: int = 100
    # two-tier lease (seconds)
    reservation_lease_ttl: int = 45
    heartbeat_interval: int = 15
    heartbeat_lease_ttl: int = 60
    # Prometheus /metrics exporter (Phase 3)
    metrics_port: int = 9091
    metric_scrape_interval: int = 5
    # Phase 4 — bounded retry (calls we control)
    hot_retry_attempts: int = 2        # greeting/hot path: 1 retry max
    offpath_retry_attempts: int = 4    # coverage tagger + mongo write
    retry_base_delay_ms: int = 50
    retry_max_delay_ms: int = 400
    # Phase 4 — reconnect grace window (MUST be < reservation_lease_ttl)
    reconnect_grace_seconds: int = 25

    @model_validator(mode="after")
    def _grace_under_lease(self):
        if self.reconnect_grace_seconds >= self.reservation_lease_ttl:
            raise ValueError(
                "reconnect_grace_seconds must be < reservation_lease_ttl "
                "(a grace window past the Tier-1 lease risks the reaper reclaiming "
                "a slot we still intend to hold)"
            )
        return self

@lru_cache
def get_settings() -> Settings:
    return Settings()
