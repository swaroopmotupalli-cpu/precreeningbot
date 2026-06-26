"""Per-interview transcript audit — the record must be ordered, complete, and
match reality (truncated barge-ins + re-asked questions included). Pure, never
raises; returns a list of human-readable problems (empty = clean)."""
from __future__ import annotations

_VALID_SPEAKERS = {"tara", "candidate"}


def audit_transcript(lines: list[dict]) -> list[str]:
    problems: list[str] = []
    if not lines:
        return problems
    ordered = sorted(lines, key=lambda l: l.get("seq", -1))
    seqs = [l.get("seq") for l in ordered]
    if seqs != list(range(len(ordered))):
        problems.append(f"seqs not contiguous 0..n-1 (gap or duplicate): {seqs}")
    for l in ordered:
        if l.get("speaker") not in _VALID_SPEAKERS:
            problems.append(f"invalid speaker: {l.get('speaker')!r} at seq {l.get('seq')}")
        if not isinstance(l.get("text"), str):
            problems.append(f"non-string text at seq {l.get('seq')}")
    if ordered[0].get("speaker") != "tara":
        problems.append("first line is not tara (Tara must greet first)")
    return problems
