from __future__ import annotations

from typing import Any, Dict

from .schemas import RunRequest, StepStatus, ToolResult, WorkflowPlan, WorkflowRunResponse
from .store import SQLiteStore
from .tools import analyze_csv, draft_email, schedule_meeting, send_email, summarize_text


def _build_text_source(request: RunRequest, context: Dict[str, Any]) -> str:
    if request.context_text:
        return request.context_text
    analysis_summary = context.get("analysis_summary")
    if analysis_summary:
        return analysis_summary
    return request.query


async def execute_plan(
    store: SQLiteStore,
    request: RunRequest,
    plan: WorkflowPlan,
    run_id: int,
) -> WorkflowRunResponse:
    context: Dict[str, Any] = {
        "query": request.query,
        "mode": request.mode.value,
        "recipients": request.recipients,
    }
    final_output: Dict[str, Any] = {}

    def safe_plan() -> WorkflowPlan:
        return WorkflowPlan.model_validate(store.protector.sanitize_payload(plan.model_dump(mode="python")))

    store.update_run(run_id, status=StepStatus.running, planner_source=plan.planner_source)

    for idx, step in enumerate(plan.steps, start=1):
        step_args = dict(step.args)
        record = store.create_step(
            run_id=run_id,
            step_id=step.id,
            step_number=idx,
            title=step.title,
            tool_name=step.tool_name,
            input_payload=step_args,
        )

        try:
            result = _execute_step(step.tool_name, step_args, request, context)
            if not result.success:
                store.update_step(
                    record.id,
                    status=StepStatus.failed,
                    output_payload=result.output,
                    error_message=result.message,
                )
                store.update_run(
                    run_id,
                    status=StepStatus.failed,
                    final_response=f"Workflow stopped at {step.title}: {result.message}",
                )
                run = store.get_run(run_id)
                steps = store.list_steps(run_id)
                return WorkflowRunResponse(
                    run=run,
                    plan=safe_plan(),
                    steps=steps,
                    final_output=store.protector.sanitize_payload(final_output),
                )

            _update_context_from_tool(step.tool_name, result, context, final_output)

            store.update_step(
                record.id,
                status=StepStatus.completed,
                output_payload={"message": result.message, **result.output},
            )
        except Exception as exc:
            store.update_step(
                record.id,
                status=StepStatus.failed,
                output_payload={},
                error_message=str(exc),
            )
            store.update_run(
                run_id,
                status=StepStatus.failed,
                final_response=f"Workflow crashed at {step.title}: {exc}",
            )
            run = store.get_run(run_id)
            steps = store.list_steps(run_id)
            return WorkflowRunResponse(
                run=run,
                plan=safe_plan(),
                steps=steps,
                final_output=store.protector.sanitize_payload(final_output),
            )

    final_message = _final_message(plan, final_output, request.mode.value)
    store.update_run(run_id, status=StepStatus.completed, final_response=final_message)

    run = store.get_run(run_id)
    steps = store.list_steps(run_id)
    return WorkflowRunResponse(
        run=run,
        plan=safe_plan(),
        steps=steps,
        final_output=store.protector.sanitize_payload(final_output),
    )


def _execute_step(
    tool_name: str,
    args: Dict[str, Any],
    request: RunRequest,
    context: Dict[str, Any],
) -> ToolResult:
    if tool_name == "analyze_csv":
        file_path = args.get("file_path")
        if not file_path:
            return ToolResult(success=False, message="No CSV file was supplied for analysis.")
        return analyze_csv(file_path)

    if tool_name == "summarize_text":
        source = args.get("source", "query")
        if source == "context_text" and request.context_text:
            text = request.context_text
        elif source == "previous_output":
            text = context.get("analysis_summary") or _build_text_source(request, context)
        else:
            text = _build_text_source(request, context)
        return summarize_text(text)

    if tool_name == "draft_email":
        summary = context.get("summary_text") or context.get("analysis_summary") or request.query
        recipients = args.get("recipients", request.recipients)
        return draft_email(summary, recipients)

    if tool_name == "send_email":
        draft = context.get("email_draft")
        if not draft:
            return ToolResult(success=False, message="There is no email draft to send.")
        return send_email(draft, live=request.mode.value == "live" and request.allow_external_side_effects)

    if tool_name == "schedule_meeting":
        query = args.get("query", request.query)
        recipients = args.get("recipients", request.recipients)
        return schedule_meeting(
            query,
            recipients,
            live=request.mode.value == "live" and request.allow_external_side_effects,
        )

    return ToolResult(success=False, message=f"Unknown tool: {tool_name}")


def _update_context_from_tool(
    tool_name: str,
    result: ToolResult,
    context: Dict[str, Any],
    final_output: Dict[str, Any],
) -> None:
    if tool_name == "analyze_csv":
        context["analysis_summary"] = result.output.get("summary", "")
        final_output["csv_analysis"] = result.output
    elif tool_name == "summarize_text":
        context["summary_text"] = result.output.get("summary", "")
        final_output["summary"] = result.output
    elif tool_name == "draft_email":
        context["email_draft"] = result.output
        final_output["email_draft"] = result.output
    elif tool_name == "send_email":
        final_output["email_delivery"] = result.output
    elif tool_name == "schedule_meeting":
        final_output["meeting"] = result.output


def _final_message(plan: WorkflowPlan, final_output: Dict[str, Any], mode: str) -> str:
    executed_tools = ", ".join(step.tool_name for step in plan.steps)
    return (
        f"Workflow completed in {mode} mode. "
        f"Executed tools: {executed_tools}. "
        f"Final outputs available for review in the trace."
    )
