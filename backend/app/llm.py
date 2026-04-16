from __future__ import annotations

import json
from typing import Any, Dict, Optional

import httpx

from .config import get_settings


settings = get_settings()


def llm_enabled() -> bool:
    return bool(settings.openai_api_key and settings.openai_model)


async def generate_plan_json(prompt: str) -> Optional[Dict[str, Any]]:
    if not llm_enabled():
        return None

    payload = {
        "model": settings.openai_model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a workflow planning engine. Produce only valid JSON. "
                    "Return an object with keys: summary, warnings, steps. "
                    "Each step must contain id, title, tool_name, reason, args, depends_on, confirmation_required."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    }

    headers = {
        "Authorization": f"Bearer {settings.openai_api_key}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(f"{settings.openai_base_url.rstrip('/')}/chat/completions", headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

    content = data["choices"][0]["message"]["content"]
    if isinstance(content, list):
        content = "".join(part.get("text", "") for part in content if isinstance(part, dict))
    return json.loads(content)
