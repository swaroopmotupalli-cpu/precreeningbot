def should_end(covered: set[str], required: list[str],
               question_count: int, max_questions: int) -> bool:
    all_covered = all(s.lower() in {c.lower() for c in covered} for s in required)
    return all_covered or question_count >= max_questions
