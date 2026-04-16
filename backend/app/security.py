from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Dict

from jose import JWTError, jwt
from passlib.context import CryptContext

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 12
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def create_access_token(data: Dict[str, Any], secret: str, expires_hours: int = ACCESS_TOKEN_EXPIRE_HOURS) -> str:
    to_encode = data.copy()
    expire = datetime.now(UTC) + timedelta(hours=expires_hours)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, secret, algorithm=ALGORITHM)


def decode_access_token(token: str, secret: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, secret, algorithms=[ALGORITHM])
    except JWTError as exc:
        raise ValueError("Invalid or expired token") from exc
