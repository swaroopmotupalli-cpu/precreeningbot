function buildQAPairs(lines) {
  const ordered = [...lines].sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));
  const pairs = [];
  let cur = null;
  for (const l of ordered) {
    if (l.speaker === "tara") {
      if (cur) pairs.push(cur);
      cur = { question: l.text || "", answer: "" };
    } else if (l.speaker === "candidate" && cur) {
      cur.answer = cur.answer ? `${cur.answer} ${l.text || ""}`.trim() : (l.text || "");
    }
  }
  if (cur) pairs.push(cur);
  return pairs;
}

async function loadInterview(db, sessionId) {
  return db.collection("aiInterview").findOne({ room: sessionId });
}

module.exports = { buildQAPairs, loadInterview };
