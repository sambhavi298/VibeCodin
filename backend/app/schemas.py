from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, EmailStr, Field


class RunMode(str, Enum):
    simulation = "simulation"
    live = "live"


class StepStatus(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    blocked = "blocked"




class ToolDefinition(BaseModel):
    name: str
    description: str
    required_args: List[str] = Field(default_factory=list)
    confirmation_required: bool = False
    safe_in_simulation: bool = True

class UploadPreview(BaseModel):
    file_id: str
    filename: str
    columns: List[str]
    row_count: int
    preview_rows: List[Dict[str, Any]]


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)


class UserResponse(BaseModel):
    id: int
    email: str
    role: str
    created_at: Optional[datetime] = None


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class RunRequest(BaseModel):
    query: str = Field(..., min_length=3)
    mode: RunMode = RunMode.simulation
    csv_file_id: Optional[str] = None
    context_text: Optional[str] = None
    recipients: List[str] = Field(default_factory=list)
    allow_external_side_effects: bool = False


class PlanStep(BaseModel):
    id: str
    title: str
    tool_name: str
    reason: str
    args: Dict[str, Any] = Field(default_factory=dict)
    depends_on: List[str] = Field(default_factory=list)
    confirmation_required: bool = False


class WorkflowPlan(BaseModel):
    planner_source: Literal["rules", "llm"]
    summary: str
    steps: List[PlanStep]
    warnings: List[str] = Field(default_factory=list)


class ToolResult(BaseModel):
    success: bool
    output: Dict[str, Any] = Field(default_factory=dict)
    message: str = ""
    requires_confirmation: bool = False


class StepRecord(BaseModel):
    id: int | None = None
    run_id: int
    step_id: str
    step_number: int
    title: str
    tool_name: str
    status: StepStatus
    input_payload: Dict[str, Any] = Field(default_factory=dict)
    output_payload: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class RunRecord(BaseModel):
    id: int | None = None
    user_id: int
    query: str
    mode: RunMode
    status: StepStatus
    final_response: Optional[str] = None
    planner_source: Optional[str] = None
    created_at: Optional[datetime] = None


class WorkflowRunResponse(BaseModel):
    run: RunRecord
    plan: WorkflowPlan
    steps: List[StepRecord]
    final_output: Dict[str, Any] = Field(default_factory=dict)
