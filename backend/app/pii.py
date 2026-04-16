from __future__ import annotations

import base64
import hashlib
import json
import re
from typing import Any

from cryptography.fernet import Fernet

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def _derive_fernet_key(secret: str) -> bytes:
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


class PIIProtector:
    def __init__(self, secret: str) -> None:
        seed = secret or "temporary-dev-encryption-key"
        self._fernet = Fernet(_derive_fernet_key(seed))

    @staticmethod
    def normalize_email(email: str) -> str:
        return email.strip().lower()

    def hash_email(self, email: str) -> str:
        normalized = self.normalize_email(email)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def mask_email(self, email: str) -> str:
        normalized = self.normalize_email(email)
        if "@" not in normalized:
            return "[redacted]"
        local, domain = normalized.split("@", 1)
        if len(local) <= 2:
            masked_local = local[0] + "*" * max(1, len(local) - 1)
        else:
            masked_local = local[:2] + "*" * max(4, len(local) - 2)
        return f"{masked_local}@{domain}"

    def encrypt_text(self, value: str) -> str:
        return self._fernet.encrypt(value.encode("utf-8")).decode("utf-8")

    def decrypt_text(self, value: str) -> str:
        return self._fernet.decrypt(value.encode("utf-8")).decode("utf-8")

    def encrypt_json(self, value: Any) -> str:
        return self.encrypt_text(json.dumps(value))

    def decrypt_json(self, value: str) -> Any:
        return json.loads(self.decrypt_text(value))

    def sanitize_text(self, value: str) -> str:
        return EMAIL_RE.sub(lambda match: self.mask_email(match.group(0)), value)

    def sanitize_payload(self, payload: Any) -> Any:
        if payload is None:
            return None
        if isinstance(payload, dict):
            clean: dict[str, Any] = {}
            for key, value in payload.items():
                lower = str(key).lower()
                if lower in {"path", "file_path"}:
                    clean[key] = "[server-file-path-hidden]"
                else:
                    clean[key] = self.sanitize_payload(value)
            return clean
        if isinstance(payload, list):
            return [self.sanitize_payload(item) for item in payload]
        if isinstance(payload, str):
            return self.sanitize_text(payload)
        return payload
