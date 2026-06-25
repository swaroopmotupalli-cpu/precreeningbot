from functools import lru_cache
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
    tts_voice: str = "en-IN-Chirp3-HD-Erinome"
    max_questions: int = 12
    transcript_ttl_seconds: int = 7200
    say_timeout_seconds: float = 20.0
    mongo_write_timeout_seconds: float = 10.0

@lru_cache
def get_settings() -> Settings:
    return Settings()
