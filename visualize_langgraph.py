








"""Legacy helper for LangGraph visualization (deprecated)."""

from __future__ import annotations


def main() -> None:
	"""Inform callers that LangGraph has been removed from the project."""
	print(
		"LangGraph workflow has been removed. Use app.agent.chain for the current LangChain implementation.",
	)


if __name__ == "__main__":
	main()
