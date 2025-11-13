"""Prompt helper for error analysis (placeholder)."""


def build_error_feedback(validation_details: str, attempt_sql: str) -> str:
	"""Feedback for LLM to correct SQL based on validation or execution errors."""

	return (
		"Your previous SQL attempt failed validation or execution."
		"\nDetails of the failure:"
		f"\n{validation_details}\n"
		"\nPrevious SQL attempt:"
		f"\n{attempt_sql}\n"
		"\nPlease revise the SQL to address the issues above, ensuring it is read-only, safe, and matches the schema."
		"\nReturn only the corrected SQL statement."
	)

