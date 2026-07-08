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

function buildQAPairs(lines) {
  const ordered = [...lines].sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));
  const pairs = [];
  let cur = null;
  for (const l of ordered) {
    if (l.speaker === "tara") {
      if (!isRealQuestion(l.text)) continue; // fragment/acknowledgment — not a question boundary
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
