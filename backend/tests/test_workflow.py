from pathlib import Path
import os
import sys

TEST_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_PATH", str(TEST_ROOT / "test_flowpilot.db"))
os.environ.setdefault("UPLOAD_DIR", str(TEST_ROOT / "test_uploads"))
os.environ.setdefault("LOGS_DIR", str(TEST_ROOT / "test_logs"))
os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("ENCRYPTION_KEY", "test-encryption-secret")

if str(TEST_ROOT) not in sys.path:
    sys.path.insert(0, str(TEST_ROOT))

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def auth_headers(email: str = "tester@example.com", password: str = "password123"):
    response = client.post("/api/v1/auth/register", json={"email": email, "password": password})
    if response.status_code not in {200, 409}:
        raise AssertionError(response.text)
    login = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert login.status_code == 200
    token = login.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def test_health():
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_csv_upload_and_run():
    headers = auth_headers()
    csv_path = Path(__file__).resolve().parents[2] / "sample_data" / "sales_sample.csv"
    with open(csv_path, "rb") as handle:
        upload = client.post(
            "/api/v1/uploads/csv",
            headers=headers,
            files={"file": ("sales_sample.csv", handle, "text/csv")},
        )
    assert upload.status_code == 200
    file_id = upload.json()["file_id"]

    response = client.post(
        "/api/v1/workflows/run",
        headers=headers,
        json={
            "query": "Analyze this CSV, summarize it, and email it to team@example.com",
            "mode": "simulation",
            "csv_file_id": file_id,
            "recipients": ["team@example.com"],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["run"]["status"] == "completed"
    assert len(data["steps"]) >= 3
    assert "csv_analysis" in data["final_output"]
    dump = str(data)
    assert "team@example.com" not in dump
    assert "te****@example.com" in dump or "te*****@example.com" in dump or "te******@example.com" in dump
