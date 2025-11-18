"""Unit tests to ensure BusinessIntentAgent receives current_date in prompts."""

from datetime import date

from app.agent.business_agent import BusinessIntentAgent
from app.models import BusinessQuerySpec


class DummyChain:
    def invoke(self, payload):
        # Assert that current_date is passed in the payload
        assert "current_date" in payload
        return BusinessQuerySpec(
            intent="dummy",
            entities=[],
            metrics=[],
            dimensions=[],
            filters=[],
        )


def test_business_agent_accepts_current_date():
    agent = BusinessIntentAgent()
    # Replace the chain with a dummy to avoid remote LLM calls
    agent.chain = DummyChain()

    # Explicit date string
    now = date.today().isoformat()
    spec = agent.analyze("How many dispenses?", ["dispense table"], current_date=now)

    assert isinstance(spec, BusinessQuerySpec)
    assert spec.intent == "dummy"
