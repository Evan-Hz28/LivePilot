from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class ContextTurn(BaseModel):
    turn_id: UUID
    sequence_no: int
    kind: str
    status: str
    context_version: int
    content: dict | None = None


class ContextPacket(BaseModel):
    packet_id: UUID = Field(default_factory=uuid4)
    session_id: UUID
    turn_id: UUID
    context_version: int
    preference_version: int
    itinerary_version: int
    preference: dict
    recent_turns: list[ContextTurn]
    user_text: str


class TaskPlan(BaseModel):
    task_type: str
    payload: dict


class AgentDecision(BaseModel):
    context_version: int
    preference_version: int
    tasks: list[TaskPlan]


class ReplyContext(BaseModel):
    message: str
    context_version: int
    preference_version: int
    source_task_ids: list[UUID]
    tool_results: list[dict]
    itinerary_version: int | None = None
    source_tool_call_ids: list[UUID] = Field(default_factory=list)
