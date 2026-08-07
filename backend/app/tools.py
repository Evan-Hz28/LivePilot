from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MockTravelAdapter:
    tool_name: str = "mock_travel"

    async def execute(self, tool_input: dict[str, Any]) -> dict[str, Any]:
        destination = str(tool_input.get("destination") or "未指定目的地")
        return {
            "mock": True,
            "tool": self.tool_name,
            "destination": destination,
            "recommendations": [
                {"name": f"{destination}历史街区", "category": "culture"},
                {"name": f"{destination}城市公园", "category": "outdoors"},
            ],
            "query": tool_input.get("query", ""),
        }


mock_travel_adapter = MockTravelAdapter()
