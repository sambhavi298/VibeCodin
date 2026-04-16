import { useEffect, useMemo, useState } from "react";

const API_BASE = (import.meta.env.VITE_API_BASE_URL || "http://localhost:8000").replace(/\/$/, "");
const TOKEN_KEY = "flowpilot_token";
const USER_KEY = "flowpilot_user";

const SAMPLE_PROMPTS = [
  { label: "CSV Trends", prompt: "Analyze this CSV and provide the top trends" },
  { label: "Report Summary", prompt: "Summarize today's report and email it to leadership@example.com" },
  { label: "Full Workflow", prompt: "Analyze this CSV, summarize the findings, schedule a meeting tomorrow afternoon, and email the recap to team@example.com" },
  { label: "Meeting Scheduler", prompt: "Schedule a meeting tomorrow at 3 PM with analytics@example.com" },
];

function authHeaders(token, extra = {}) {
  return {
    ...extra,
    Authorization: `Bearer ${token}`,
  };
}

async function apiRequest(path, { token, method = "GET", headers = {}, body } = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    method,
    headers: token ? authHeaders(token, headers) : headers,
    body,
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  if (!response.ok) {
    throw new Error(data.detail || "Request failed");
  }
  return data;
}

function LoginScreen({ onAuthed }) {
  const [mode, setMode] = useState("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function submit(e) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const data = await apiRequest(`/api/v1/auth/${mode}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
      localStorage.setItem(TOKEN_KEY, data.access_token);
      localStorage.setItem(USER_KEY, JSON.stringify(data.user));
      onAuthed(data.access_token, data.user);
    } catch (err) {
      setError(err.message || "Authentication failed.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="auth-page">
      <div className="auth-card panel">
        <p className="eyebrow">Round-1 compliant hackathon backend</p>
        <h1>FlowPilot Secure Ops Agent</h1>
        <p className="subtle">
          Login first. Humanity insists on access control when emails and schedules are involved.
        </p>

        <div className="toggle-row">
          <button className={mode === "login" ? "sample-btn active" : "sample-btn"} onClick={() => setMode("login")}>
            Login
          </button>
          <button className={mode === "register" ? "sample-btn active" : "sample-btn"} onClick={() => setMode("register")}>
            Register
          </button>
        </div>

        <form onSubmit={submit} className="auth-form">
          <label>Email</label>
          <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required />
          <label>Password</label>
          <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} minLength={8} required />
          <button className="run-btn" type="submit" disabled={busy}>
            {busy ? "Working..." : mode === "login" ? "Login" : "Create account"}
          </button>
        </form>

        {error ? <p className="error">{error}</p> : null}
      </div>
    </div>
  );
}

function Dashboard({ token, user, onLogout }) {
  const [query, setQuery] = useState(SAMPLE_PROMPTS[0].prompt);
  const [mode, setMode] = useState("simulation");
  const [contextText, setContextText] = useState("");
  const [recipients, setRecipients] = useState("");
  const [csvFile, setCsvFile] = useState(null);
  const [csvPreview, setCsvPreview] = useState(null);
  const [runData, setRunData] = useState(null);
  const [runHistory, setRunHistory] = useState([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const parsedRecipients = useMemo(
    () => recipients.split(",").map((s) => s.trim()).filter(Boolean),
    [recipients]
  );

  async function refreshRuns() {
    try {
      const data = await apiRequest("/api/v1/workflows", { token });
      setRunHistory(data.runs ?? []);
    } catch (err) {
      setError(err.message || "Could not load history.");
    }
  }

  useEffect(() => {
    refreshRuns();
  }, []);

  async function uploadCsvIfNeeded() {
    if (!csvFile) return null;
    const formData = new FormData();
    formData.append("file", csvFile);
    const data = await apiRequest("/api/v1/uploads/csv", {
      token,
      method: "POST",
      body: formData,
    });
    setCsvPreview(data);
    return data.file_id;
  }

  async function loadRun(runId) {
    setBusy(true);
    setError("");
    try {
      const data = await apiRequest(`/api/v1/workflows/${runId}`, { token });
      setRunData(data);
    } catch (err) {
      setError(err.message || "Could not load workflow.");
    } finally {
      setBusy(false);
    }
  }

  async function runWorkflow() {
    setBusy(true);
    setError("");
    try {
      const csvFileId = await uploadCsvIfNeeded();
      const data = await apiRequest("/api/v1/workflows/run", {
        token,
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query,
          mode,
          csv_file_id: csvFileId,
          context_text: contextText || null,
          recipients: parsedRecipients,
          allow_external_side_effects: false,
        }),
      });
      setRunData(data);
      refreshRuns();
    } catch (err) {
      const message = err.message || "Something broke, which is rude but unsurprising.";
      setError(message);
      if (message.toLowerCase().includes("authentication")) {
        onLogout();
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="page">
      <header className="hero">
        <div className="hero-content">
          <p className="eyebrow">AI Workflow Automation Agent</p>
          <h1>FlowPilot</h1>
          <p className="subtitle">
            Turn natural language into secure, traceable business workflows across CSV analysis, report summarization, and scheduling.
          </p>
          <div className="trust-strip">
            <span className="trust-badge">Authenticated access</span>
            <span className="trust-badge">Masked PII</span>
            <span className="trust-badge">Encrypted storage</span>
            <span className="trust-badge">Execution trace enabled</span>
          </div>
        </div>
        <div className="hero-actions">
          <button className="ghost-btn" onClick={onLogout}>Logout</button>
        </div>
      </header>

      <div className="kpi-row">
        <div className="kpi-card">
          <label>Planner Mode</label>
          <div className="value">{runData?.plan?.planner_source || "Rule-based"}</div>
        </div>
        <div className="kpi-card">
          <label>Recent Runs</label>
          <div className="value">{runHistory.length}</div>
        </div>
        <div className="kpi-card">
          <label>Execution Mode</label>
          <div className="value">{mode === "live" ? "Live" : "Simulation"}</div>
        </div>
      </div>

      <main className="grid">
        <section className="panel input-panel">
          <h2>Command Center</h2>
          <h3>Try a demo workflow</h3>
          <div className="sample-list">
            {SAMPLE_PROMPTS.map((item) => (
              <button key={item.label} className="sample-btn" onClick={() => setQuery(item.prompt)}>
                {item.label}
              </button>
            ))}
          </div>

          <label>Workflow request</label>
          <textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            rows={5}
            placeholder="Analyze this CSV, summarize the findings, email the team, and schedule a meeting tomorrow afternoon."
          />

          <div className="row">
            <div>
              <label>Mode</label>
              <div className="pill-toggle">
                <button
                  className={mode === "simulation" ? "active" : ""}
                  onClick={() => setMode("simulation")}
                >
                  Simulation
                </button>
                <button
                  className={mode === "live" ? "active" : ""}
                  onClick={() => setMode("live")}
                >
                  Live
                </button>
              </div>
            </div>

            <div>
              <label>Recipients</label>
              <input
                value={recipients}
                onChange={(e) => setRecipients(e.target.value)}
                placeholder="team@example.com, ops@example.com"
              />
            </div>
          </div>

          <label>Optional report text</label>
          <textarea
            value={contextText}
            onChange={(e) => setContextText(e.target.value)}
            rows={4}
            placeholder="Paste a report or notes here for summarization."
          />

          <label>Optional CSV upload</label>
          <div className="file-upload-box">
            <input type="file" id="csv-upload" accept=".csv" onChange={(e) => setCsvFile(e.target.files?.[0] ?? null)} />
            <label htmlFor="csv-upload" className="file-label">
              {csvFile ? `Selected: ${csvFile.name}` : "Click to select CSV"}
            </label>
          </div>

          <button className="run-btn" onClick={runWorkflow} disabled={busy}>
            {busy ? "Running workflow..." : "Run Workflow"}
          </button>

          {error ? <p className="error">{error}</p> : null}

          {csvPreview ? (
            <div className="preview-card">
              <h3>Uploaded CSV</h3>
              <p>
                <strong>{csvPreview.filename}</strong> · {csvPreview.row_count} rows
              </p>
              <p>Columns: {csvPreview.columns.join(", ")}</p>
            </div>
          ) : null}
        </section>

        <section className="panel output-panel">
          <h2>Execution Trace</h2>
          {runData ? (
            <>
              <div className="summary-box">
                <p><strong>Planner:</strong> {runData.plan.planner_source}</p>
                <p><strong>Status:</strong> {runData.run.status}</p>
                <p><strong>Plan summary:</strong> {runData.plan.summary}</p>
                {runData.plan.warnings?.length ? (
                  <ul>
                    {runData.plan.warnings.map((warning) => <li key={warning}>{warning}</li>)}
                  </ul>
                ) : null}
              </div>

              <div className="trace-list">
                {runData.steps.map((step) => (
                  <div key={step.id} className={`trace-item ${step.status}`}>
                    <div className="trace-head">
                      <span>{step.step_number}. {step.title}</span>
                      <span className={`status ${step.status}`}>{step.status}</span>
                    </div>
                    <p className="muted">{step.reason || `Tool: ${step.tool_name}`}</p>
                    {step.error_message ? (
                      <p className="error">{step.error_message}</p>
                    ) : (
                      <details>
                        <summary>View raw step output</summary>
                        <pre>{JSON.stringify(step.output_payload, null, 2)}</pre>
                      </details>
                    )}
                  </div>
                ))}
              </div>

              <div className="final-box">
                <div className="final-head">
                  <h3>Final Result</h3>
                  <div className="muted small">Run ID: {runData.run.id} · Status: {runData.run.status}</div>
                </div>
                <div className="final-response">
                  {runData.final_output?.message || runData.final_output?.summary || "Workflow completed."}
                </div>
                <details>
                  <summary>View raw final output</summary>
                  <pre>{JSON.stringify(runData.final_output, null, 2)}</pre>
                </details>
              </div>
            </>
          ) : (
            <div className="empty-state">
              <p>Run a workflow to see the plan, tool calls, masked outputs, and execution history.</p>
            </div>
          )}
        </section>
      </main>

      <section className="panel history-panel">
        <div className="history-head">
          <h2>Recent Runs</h2>
          <button className="ghost-btn" onClick={refreshRuns}>Refresh</button>
        </div>
        {runHistory.length ? (
          <div className="history-list">
            {runHistory.map((run) => (
              <div key={run.id} className="history-item">
                <div className="history-info">
                  <p><strong>#{run.id}</strong> · {run.status}</p>
                  <p className="muted small">{new Date(run.created_at).toLocaleString()} · {run.planner_source}</p>
                  <p className="history-query">{run.query}</p>
                </div>
                <button className="ghost-btn small" onClick={() => loadRun(run.id)}>Open</button>
              </div>
            ))}
          </div>
        ) : (
          <p className="muted">No runs yet.</p>
        )}
      </section>
    </div>
  );
}

export default function App() {
  const [token, setToken] = useState(localStorage.getItem(TOKEN_KEY));
  const [user, setUser] = useState(() => {
    const raw = localStorage.getItem(USER_KEY);
    return raw ? JSON.parse(raw) : null;
  });

  function handleAuthed(nextToken, nextUser) {
    setToken(nextToken);
    setUser(nextUser);
  }

  function handleLogout() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
    setToken(null);
    setUser(null);
  }

  return token && user ? (
    <Dashboard token={token} user={user} onLogout={handleLogout} />
  ) : (
    <LoginScreen onAuthed={handleAuthed} />
  );
}
