from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _parse_allowed_origins(value: str) -> list[str]:
    return [origin.strip() for origin in value.split(",") if origin.strip()]


@dataclass(frozen=True)
class Settings:
    database_path: str
    upload_dir: str
    logs_dir: str
    jwt_secret: str
    encryption_key: str
    openai_api_key: str
    openai_base_url: str
    openai_model: str
    allowed_origins: list[str]


def get_settings() -> Settings:
    database_path = os.getenv("DATABASE_PATH", "./flowpilot.db")
    upload_dir = os.getenv("UPLOAD_DIR", "./uploads")
    logs_dir = os.getenv("LOGS_DIR", "./logs")
    Path(upload_dir).mkdir(parents=True, exist_ok=True)
    Path(logs_dir).mkdir(parents=True, exist_ok=True)
    return Settings(
        database_path=database_path,
        upload_dir=upload_dir,
        logs_dir=logs_dir,
        jwt_secret=os.getenv("JWT_SECRET", "change-me-for-production"),
        encryption_key=os.getenv("ENCRYPTION_KEY", ""),
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        openai_base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        allowed_origins=_parse_allowed_origins(os.getenv("ALLOWED_ORIGINS", "http://localhost:5173")),
    )
