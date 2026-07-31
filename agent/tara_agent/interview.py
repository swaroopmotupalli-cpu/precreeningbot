def should_end(question_count: int, max_questions: int) -> bool:
    """The interview always runs to exactly `max_questions` — never ends
    early just because every skill has been covered (Tara asks further,
    different-angle questions on the same skills to fill the budget instead).
    Skill coverage is the prompt's concern (what to ask), not this gate's."""
    return question_count >= max_questions
