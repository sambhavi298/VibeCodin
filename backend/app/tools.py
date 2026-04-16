from __future__ import annotations

import math
import re
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd

from .schemas import ToolDefinition, ToolResult


TOOL_REGISTRY: Dict[str, ToolDefinition] = {
    "analyze_csv": ToolDefinition(
        name="analyze_csv",
        description="Analyze a CSV file and extract high-level trends, quality issues, and outliers.",
        required_args=["file_path"],
        confirmation_required=False,
        safe_in_simulation=True,
    ),
    "summarize_text": ToolDefinition(
        name="summarize_text",
        description="Summarize reports, findings, or context text into concise key points.",
        required_args=["text"],
        confirmation_required=False,
        safe_in_simulation=True,
    ),
    "draft_email": ToolDefinition(
        name="draft_email",
        description="Draft a professional summary email from prior workflow outputs.",
        required_args=["summary", "recipients"],
        confirmation_required=False,
        safe_in_simulation=True,
    ),
    "send_email": ToolDefinition(
        name="send_email",
        description="Send an email through a mock adapter or real adapter later.",
        required_args=["draft"],
        confirmation_required=True,
        safe_in_simulation=True,
    ),
    "schedule_meeting": ToolDefinition(
        name="schedule_meeting",
        description="Schedule a meeting based on natural language timing preferences.",
        required_args=["query"],
        confirmation_required=True,
        safe_in_simulation=True,
    ),
}


def get_tools() -> List[ToolDefinition]:
    return list(TOOL_REGISTRY.values())


def extractive_summary(text: str, max_points: int = 3) -> List[str]:
    cleaned = " ".join(text.split())
    if not cleaned:
        return ["No content was provided."]
    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    sentences = [s.strip() for s in sentences if s.strip()]
    if not sentences:
        return [cleaned[:220]]

    word_counts = Counter(re.findall(r"\w+", cleaned.lower()))
    scored = []
    for sentence in sentences:
        words = re.findall(r"\w+", sentence.lower())
        score = sum(word_counts[w] for w in words) / max(len(words), 1)
        scored.append((score, sentence))
    top = [s for _, s in sorted(scored, reverse=True)[:max_points]]
    return top


def infer_time_slot(query: str) -> Dict[str, Any]:
    q = query.lower()
    now = datetime.utcnow()
    day = now
    if "tomorrow" in q:
        day = now + timedelta(days=1)
    elif "next week" in q:
        day = now + timedelta(days=7)

    if "afternoon" in q:
        hour = 15
    elif "morning" in q:
        hour = 10
    elif "evening" in q:
        hour = 18
    else:
        match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", q)
        if match:
            hour = int(match.group(1))
            minute = int(match.group(2) or "0")
            meridian = match.group(3)
            if meridian == "pm" and hour != 12:
                hour += 12
            if meridian == "am" and hour == 12:
                hour = 0
            start = day.replace(hour=hour, minute=minute, second=0, microsecond=0)
            end = start + timedelta(minutes=30)
            return {
                "start_time": start.isoformat(timespec="minutes"),
                "end_time": end.isoformat(timespec="minutes"),
                "confidence": "high",
            }
        hour = 15

    start = day.replace(hour=hour, minute=0, second=0, microsecond=0)
    end = start + timedelta(minutes=30)
    return {
        "start_time": start.isoformat(timespec="minutes"),
        "end_time": end.isoformat(timespec="minutes"),
        "confidence": "medium",
    }


def analyze_csv(file_path: str) -> ToolResult:
    try:
        df = pd.read_csv(file_path)
    except Exception as exc:
        return ToolResult(success=False, message=f"Could not read CSV: {exc}")

    if df.empty:
        return ToolResult(success=False, message="CSV was loaded but contains no rows.")

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = [c for c in df.columns if c not in numeric_cols]

    insights: List[str] = []
    quality_notes: List[str] = []
    outliers: List[Dict[str, Any]] = []

    missing = df.isna().sum()
    missing_cols = missing[missing > 0]
    if not missing_cols.empty:
        quality_notes.append(
            "Missing values found in: "
            + ", ".join(f"{col} ({int(count)})" for col, count in missing_cols.items())
        )
    else:
        quality_notes.append("No missing values detected.")

    if numeric_cols:
        for col in numeric_cols[:3]:
            series = df[col].dropna()
            if len(series) < 2:
                continue
            midpoint = len(series) // 2
            first_mean = float(series.iloc[:midpoint].mean()) if midpoint else float(series.mean())
            second_mean = float(series.iloc[midpoint:].mean())
            delta = second_mean - first_mean
            direction = "increased" if delta >= 0 else "decreased"
            pct = (abs(delta) / (abs(first_mean) + 1e-9)) * 100
            insights.append(
                f"{col} {direction} by about {pct:.1f}% between the first and second half of the dataset."
            )

            std = float(series.std()) if len(series) > 1 else 0.0
            mean = float(series.mean())
            if std > 0:
                z_scores = ((series - mean) / std).abs()
                flagged = z_scores[z_scores > 2.5]
                if not flagged.empty:
                    top_index = int(flagged.sort_values(ascending=False).index[0])
                    outliers.append(
                        {"column": col, "row_index": top_index, "value": float(df.loc[top_index, col])}
                    )

        corr = df[numeric_cols].corr(numeric_only=True)
        best_pair = None
        best_value = 0.0
        for i, c1 in enumerate(numeric_cols):
            for c2 in numeric_cols[i + 1:]:
                value = abs(float(corr.loc[c1, c2]))
                if value > best_value and not math.isnan(value):
                    best_value = value
                    best_pair = (c1, c2)
        if best_pair:
            insights.append(
                f"The strongest numeric relationship is between {best_pair[0]} and {best_pair[1]} "
                f"(absolute correlation {best_value:.2f})."
            )
    else:
        insights.append("No numeric columns were found, so quantitative trend analysis was limited.")

    if categorical_cols:
        cat = categorical_cols[0]
        counts = df[cat].astype(str).value_counts().head(3)
        label = ", ".join(f"{idx} ({int(val)})" for idx, val in counts.items())
        insights.append(f"Top categories in {cat}: {label}.")

    if not outliers:
        quality_notes.append("No strong outliers were detected using a simple z-score rule.")
    else:
        quality_notes.append(
            "Potential outliers: "
            + ", ".join(
                f"{o['column']} at row {o['row_index']} with value {o['value']}" for o in outliers[:3]
            )
        )

    top_insights = insights[:3] if insights else ["No major trends could be derived."]
    summary = " ".join(top_insights)

    return ToolResult(
        success=True,
        message="CSV analysis complete.",
        output={
            "shape": {"rows": int(df.shape[0]), "columns": int(df.shape[1])},
            "columns": df.columns.tolist(),
            "top_insights": top_insights,
            "quality_notes": quality_notes,
            "outliers": outliers[:5],
            "summary": summary,
        },
    )


def summarize_text(text: str) -> ToolResult:
    bullets = extractive_summary(text, max_points=3)
    summary = " ".join(bullets)
    return ToolResult(
        success=True,
        message="Summary generated.",
        output={"bullets": bullets, "summary": summary},
    )


def draft_email(summary: str, recipients: List[str]) -> ToolResult:
    recipient_line = ", ".join(recipients) if recipients else "team"
    body = (
        f"Hello {recipient_line},\n\n"
        f"Here is the summary from the workflow execution:\n\n"
        f"{summary}\n\n"
        "Key next step:\n"
        "- Review the findings and confirm any follow-up actions.\n\n"
        "Regards,\nFlowPilot"
    )
    return ToolResult(
        success=True,
        message="Email draft prepared.",
        output={
            "subject": "Workflow Summary and Next Steps",
            "body": body,
            "recipients": recipients,
        },
    )


def send_email(draft: Dict[str, Any], live: bool) -> ToolResult:
    if not live:
        return ToolResult(
            success=True,
            message="Simulation mode: email send skipped safely.",
            output={"simulated": True, "draft": draft},
            requires_confirmation=False,
        )
    return ToolResult(
        success=True,
        message="Live mode enabled, but email delivery is mocked for hackathon reliability.",
        output={"simulated": False, "delivery_status": "mock-sent", "draft": draft},
        requires_confirmation=False,
    )


def schedule_meeting(query: str, recipients: List[str], live: bool) -> ToolResult:
    slot = infer_time_slot(query)
    event = {
        "title": "Workflow Review Meeting",
        "attendees": recipients,
        "start_time": slot["start_time"],
        "end_time": slot["end_time"],
        "confidence": slot["confidence"],
    }
    if not live:
        return ToolResult(
            success=True,
            message="Simulation mode: meeting scheduling skipped safely.",
            output={"simulated": True, "event": event},
        )
    return ToolResult(
        success=True,
        message="Live mode enabled, but calendar creation is mocked for hackathon reliability.",
        output={"simulated": False, "event": event, "calendar_status": "mock-created"},
    )
