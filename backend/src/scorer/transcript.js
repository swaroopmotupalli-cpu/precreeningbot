// A Tara line only counts as a real question if it contains "?" — the same
// rule the live agent uses to count questions (see InterviewAgent.on_tara_line),
// so live counting and DB pairing always agree on what "a question" is. A
// short acknowledgment or barge-in fragment ("That sounds like", "That's
// good, moving on") never has a "?", so it's never mistaken for a new
// question — no need to guess from context or depend on interruption
// detection having fired correctly.
function isRealQuestion(text) {
  return (text || "").includes("?");
}

// Mirrors the live agent's rephrase detector (InterviewAgent._is_rephrase_of
// in interview_agent.py): a rephrase reuses most of the same key words as the
// question it's rephrasing, whereas a genuinely new question (different
// topic) shares few. The live agent already skips counting a rephrase toward
// its question cap, but it still SAVES the rephrased line to the transcript
// like any other Tara line — so buildQAPairs needs its own, independent
// check here, or a rephrase (asked after "can you repeat that?", or after
// the candidate goes off-topic instead of answering) silently inflates the
// pair count past however many questions were actually asked.
const REPHRASE_SIMILARITY_THRESHOLD = 0.4;
const WORD_RE = /[a-z']+/g;
// Regression: these appear in almost EVERY interview question regardless of
// topic ("Have you had experience working with X in your projects?") — left
// in, they drown out the actual topic words and made two UNRELATED questions
// (e.g. a MongoDB schema question and a later GraphQL question) look similar
// enough to be mistaken for a rephrase, silently dropping a real pair.
const GENERIC_INTERVIEW_WORDS = new Set([
  "have", "your", "with", "working", "worked", "work", "works",
  "projects", "project", "experience", "about", "tell", "using", "used",
  "would", "could", "please", "typically", "approach", "approached",
  "when", "what", "how", "had", "any", "been", "that", "this", "from",
  "into", "does", "you're", "you've", "these", "those",
]);
function contentWords(text) {
  const words = String(text || "").toLowerCase().match(WORD_RE) || [];
  return new Set(words.filter((w) => w.length > 3 && !GENERIC_INTERVIEW_WORDS.has(w)));
}
function isRephraseOf(newText, previousText) {
  if (!previousText) return false;
  const a = contentWords(newText);
  const b = contentWords(previousText);
  if (!a.size || !b.size) return false;
  let overlap = 0;
  for (const w of a) if (b.has(w)) overlap++;
  return overlap / Math.min(a.size, b.size) >= REPHRASE_SIMILARITY_THRESHOLD;
}

function buildQAPairs(lines) {
  const ordered = [...lines].sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));
  const pairs = [];
  let cur = null;
  for (const l of ordered) {
    if (l.speaker === "tara") {
      if (!isRealQuestion(l.text)) continue; // fragment/acknowledgment — not a question boundary
      if (cur && isRephraseOf(l.text, cur.question)) {
        // Same question, reworded (e.g. after a repeat/clarify request, or
        // after the candidate went off-topic instead of answering) — update
        // the wording in place rather than starting a new pair, so a
        // rephrase never inflates the question count in the final report.
        cur.question = l.text || "";
        continue;
      }
      if (cur) pairs.push(cur);
      cur = { question: l.text || "", answer: "" };
    } else if (l.speaker === "candidate" && cur) {
      cur.answer = cur.answer ? `${cur.answer} ${l.text || ""}`.trim() : (l.text || "");
    }
  }
  if (cur) pairs.push(cur);
  return pairs;
}

// Full conversation, cleaned up for display/audit: consecutive lines from the
// same speaker (a candidate answer split into several turns by pauses, or a
// Tara utterance split by a barge-in) are merged into one turn each — the
// same "one logical turn shouldn't be fragmented" idea as buildQAPairs,
// applied to the whole transcript rather than just Q&A pairing.
function mergeTurns(lines) {
  const ordered = [...(lines || [])].sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));
  const merged = [];
  for (const l of ordered) {
    const last = merged[merged.length - 1];
    const speaker = l.speaker === "candidate" ? "user" : "tara";
    if (last && last.speaker === speaker) {
      last.text = `${last.text} ${l.text || ""}`.trim();
    } else {
      merged.push({ speaker, text: l.text || "" });
    }
  }
  return merged;
}

async function loadInterview(db, sessionId) {
  return db.collection("aiInterview").findOne({ room: sessionId });
}

module.exports = { buildQAPairs, mergeTurns, loadInterview };
