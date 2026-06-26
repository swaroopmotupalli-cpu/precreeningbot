from google import genai
from google.genai import types

def make_classify_fn(settings):
    client = genai.Client(api_key=settings.gemini_api_key)
    async def classify(prompt: str) -> str:
        resp = await client.aio.models.generate_content(
            model=settings.gemini_model,
            contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=64, temperature=0.0),
        )
        return (resp.text or "none").strip()
    return classify
