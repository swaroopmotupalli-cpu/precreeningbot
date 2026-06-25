# Latency Gate Fixtures

The harness (`tools/latency_harness.py`) publishes these WAV files as synthetic
candidate answers during the Phase-1 gate run.  Place **3–6 short recordings**
here (one file per question the interviewer will ask).

---

## File format requirements

| Property       | Required value     |
|----------------|--------------------|
| Encoding       | 16-bit PCM (uncompressed) |
| Sample rate    | 48 000 Hz          |
| Channels       | 1 (mono)           |
| Duration       | 5–20 s each        |
| File name      | `01_answer.wav`, `02_answer.wav`, … (sorted alphabetically = turn order) |

The harness reads them via Python's `wave` module and rejects non-PCM16 files.

---

## How to create fixture WAVs

### Option A — Record real candidate answers

Use any DAW, Audacity, or the system recorder; export as 48 kHz mono PCM WAV.

### Option B — Generate via Google TTS (CLI example)

```bash
# Install Google Cloud SDK and authenticate:
gcloud auth application-default login

python - <<'EOF'
from google.cloud import texttospeech

client = texttospeech.TextToSpeechClient()
answers = [
    "I have five years of experience building backend services in Python, primarily using FastAPI and asyncio.",
    "I'm most comfortable with PostgreSQL and have tuned queries on tables with over a hundred million rows.",
    "I resolved a race condition in our order-processing pipeline by replacing a polling loop with asyncio events.",
    "I use pytest with fixtures and parameterize to keep tests deterministic and fast.",
    "I containerise everything with Docker and deploy through CI/CD pipelines on GitHub Actions.",
    "I'm keen to improve Tara's latency further and would love to work on the turn-detector configuration.",
]

for i, text in enumerate(answers, start=1):
    synthesis_input = texttospeech.SynthesisInput(text=text)
    voice = texttospeech.VoiceSelectionParams(
        language_code="en-US",
        ssml_gender=texttospeech.SsmlVoiceGender.NEUTRAL,
    )
    audio_config = texttospeech.AudioConfig(
        audio_encoding=texttospeech.AudioEncoding.LINEAR16,
        sample_rate_hertz=48000,
    )
    response = client.synthesize_speech(
        input=synthesis_input, voice=voice, audio_config=audio_config
    )
    path = f"tools/fixtures/{i:02d}_answer.wav"
    with open(path, "wb") as f:
        f.write(response.audio_content)
    print(f"wrote {path}")
EOF
```

### Option C — Convert an existing WAV with ffmpeg

```bash
ffmpeg -i input.wav -ar 48000 -ac 1 -acodec pcm_s16le tools/fixtures/01_answer.wav
```

---

## Running the three-process gate

You need three credentials sets active before running:

```
LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET   (LiveKit Cloud project)
GOOGLE_API_KEY  or  GOOGLE_APPLICATION_CREDENTIALS  (STT + TTS + Gemini)
REDIS_URL                                            (Redis for session store)
MONGODB_URI                                          (MongoDB for transcripts)
```

### Terminal 1 — backend

```bash
cd backend
LIVEKIT_URL=wss://your-project.livekit.cloud \
LIVEKIT_API_KEY=APIxxxxxxxx \
LIVEKIT_API_SECRET=... \
REDIS_URL=redis://localhost:6379 \
npm start
```

### Terminal 2 — worker

```bash
cd agent
source .venv/bin/activate
LIVEKIT_URL=wss://your-project.livekit.cloud \
LIVEKIT_API_KEY=APIxxxxxxxx \
LIVEKIT_API_SECRET=... \
REDIS_URL=redis://localhost:6379 \
MONGODB_URI=mongodb://localhost:27017 \
GOOGLE_API_KEY=... \
python -m tara_agent.worker dev
```

### Terminal 3 — harness (standalone mode)

```bash
cd /path/to/PreScreeningbot
source agent/.venv/bin/activate
BACKEND_URL=http://localhost:3000 python tools/latency_harness.py
```

After the interview ends the worker prints:

```
LATENCY_BREAKDOWN_P50={"eou_delay":0.180,"stt":0.045,"llm_ttft":0.260,"tts_ttfb":0.175,"total":0.615}
```

**PASS** if `total < 0.800`.  Record the breakdown in the commit message.

### Automated mode (harness launches worker itself)

```bash
python tools/latency_harness.py --subprocess
```

The harness starts the worker, runs the interview, captures
`LATENCY_BREAKDOWN_P50`, prints the gate verdict, and exits non-zero on
failure.  All credentials must be in the environment before running.

---

## Gate failure remediation

| Offending stage | Likely cause | Fix |
|-----------------|--------------|-----|
| `eou_delay` > 0.3 s | Turn-detector threshold too conservative | Lower `MultilingualModel` sensitivity or check agent config |
| `llm_ttft` > 0.3 s  | LLM cold-start; static prompt block not cached | Enable Gemini context caching for the static system-prompt block (Phase 4) |
| `tts_ttfb` > 0.2 s  | Batch TTS instead of streaming | Confirm `google.TTS(use_streaming=True)` is set in worker.py |

Do **not** proceed to Phase 2 until the gate exits 0.
