from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path

import pandas as pd
from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .config import get_settings
from .executor import execute_plan
from .logging_config import configure_logging
from .pii import PIIProtector
from .planner import build_plan
from .schemas import LoginRequest, RegisterRequest, RunRequest, TokenResponse, UploadPreview, UserResponse, WorkflowRunResponse
from .security import create_access_token, decode_access_token, hash_password, verify_password
from .store import SQLiteStore
from .tools import get_tools

settings = get_settings()
logger = configure_logging(settings.logs_dir)
protector = PIIProtector(settings.encryption_key)
store = SQLiteStore(db_path=settings.database_path, upload_dir=settings.upload_dir, protector=protector)
security_scheme = HTTPBearer(auto_error=False)

app = FastAPI(title="FlowPilot API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("Unhandled request failure on %s %s", request.method, request.url.path)
        raise
    duration_ms = (time.perf_counter() - start) * 1000
    logger.info("%s %s -> %s in %.1fms", request.method, request.url.path, response.status_code, duration_ms)
    return response


def _token_response_for_user(user: UserResponse) -> TokenResponse:
    token = create_access_token({"sub": str(user.id), "role": user.role}, settings.jwt_secret)
    return TokenResponse(access_token=token, user=user)


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security_scheme)) -> UserResponse:
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    try:
        payload = decode_access_token(credentials.credentials, settings.jwt_secret)
        user_id = int(payload.get("sub"))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication token") from exc
    user = store.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user


@app.on_event("startup")
async def startup_event():
    Path(settings.upload_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.logs_dir).mkdir(parents=True, exist_ok=True)
    logger.info("FlowPilot backend started")
    if not settings.openai_api_key:
        logger.info("OPENAI_API_KEY is empty. Planner will fall back to rule-based mode.")


@app.get("/api/v1/health")
def health() -> dict:
    return {"status": "ok", "service": "flowpilot-backend"}


@app.post("/api/v1/auth/register", response_model=TokenResponse)
def register(request: RegisterRequest):
    existing = store.get_user_auth(request.email)
    if existing:
        raise HTTPException(status_code=409, detail="An account with that email already exists.")
    user = store.create_user(email=request.email, password_hash=hash_password(request.password))
    logger.info("Created user account %s", user.id)
    return _token_response_for_user(user)


@app.post("/api/v1/auth/login", response_model=TokenResponse)
def login(request: LoginRequest):
    auth_row = store.get_user_auth(request.email)
    if not auth_row or not verify_password(request.password, auth_row["password_hash"]):
        logger.warning("Failed login attempt for %s", protector.mask_email(request.email))
        raise HTTPException(status_code=401, detail="Invalid email or password")
    user = store.get_user_by_id(int(auth_row["id"]))
    return _token_response_for_user(user)


@app.get("/api/v1/auth/me", response_model=UserResponse)
def me(current_user: UserResponse = Depends(get_current_user)):
    return current_user


@app.get("/api/v1/tools")
def list_tools(current_user: UserResponse = Depends(get_current_user)):
    return {"tools": [tool.model_dump() for tool in get_tools()]}


@app.get("/api/v1/workflows")
def list_workflows(current_user: UserResponse = Depends(get_current_user)):
    runs = store.list_runs(current_user.id)
    return {"runs": [run.model_dump(mode="json") for run in runs]}


@app.get("/api/v1/workflows/{run_id}")
def get_workflow(run_id: int, current_user: UserResponse = Depends(get_current_user)):
    run = store.get_run(run_id)
    if not run or run.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="Run not found")
    steps = store.list_steps(run_id)
    return {
        "run": run.model_dump(mode="json"),
        "steps": [step.model_dump(mode="json") for step in steps],
    }


@app.post("/api/v1/uploads/csv", response_model=UploadPreview)
async def upload_csv(file: UploadFile = File(...), current_user: UserResponse = Depends(get_current_user)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV uploads are supported.")

    file_id = str(uuid.uuid4())
    safe_name = Path(file.filename).name
    path = Path(settings.upload_dir) / f"{file_id}_{safe_name}"
    contents = await file.read()
    path.write_bytes(contents)

    try:
        df = pd.read_csv(path)
    except Exception as exc:
        path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {exc}") from exc

    preview_rows = df.head(5).fillna("").to_dict(orient="records")
    preview = store.save_upload(
        user_id=current_user.id,
        file_id=file_id,
        filename=safe_name,
        path=str(path.resolve()),
        columns=df.columns.tolist(),
        row_count=int(df.shape[0]),
        preview_rows=preview_rows,
    )
    logger.info("User %s uploaded CSV %s", current_user.id, safe_name)
    return preview


@app.post("/api/v1/workflows/run", response_model=WorkflowRunResponse)
async def run_workflow(request: RunRequest, current_user: UserResponse = Depends(get_current_user)):
    upload_meta = store.get_upload(request.csv_file_id, current_user.id) if request.csv_file_id else None
    if request.csv_file_id and not upload_meta:
        raise HTTPException(status_code=404, detail="CSV upload not found for this user")

    plan = await build_plan(request, upload_meta)
    run = store.create_run(user_id=current_user.id, query=request.query, mode=request.mode, planner_source=plan.planner_source)
    logger.info("User %s started workflow %s", current_user.id, run.id)
    response = await execute_plan(store=store, request=request, plan=plan, run_id=run.id)
    return response
