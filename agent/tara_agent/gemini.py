from google import genai
from google.genai import types
from tara_agent.retry import retry_async

def make_classify_fn(settings):
    client = genai.Client(api_key=settings.gemini_api_key)
    async def classify(prompt: str) -> str:
        async def _call():
            resp = await client.aio.models.generate_content(
                model=settings.gemini_model,
                contents=prompt,
                config=types.GenerateContentConfig(max_output_tokens=64, temperature=0.0),
            )
            return (resp.text or "none").strip()
        return await retry_async(
            _call,
            attempts=getattr(settings, "offpath_retry_attempts", 4),
            base_delay=getattr(settings, "retry_base_delay_ms", 50) / 1000.0,
            max_delay=getattr(settings, "retry_max_delay_ms", 400) / 1000.0,
        )
    return classify
