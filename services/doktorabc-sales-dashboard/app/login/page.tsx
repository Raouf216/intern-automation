"use client";

import { FormEvent, useState } from "react";
import { ChartCandlestick, Cross, LockKeyhole } from "lucide-react";

export default function LoginPage() {
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function submitLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setLoading(true);

    try {
      const response = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password }),
      });
      const payload = (await response.json().catch(() => ({}))) as { error?: string };

      if (!response.ok) {
        setError(payload.error || "Login fehlgeschlagen.");
        return;
      }

      window.location.href = "/";
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="login-shell">
      <section className="login-panel" aria-label="DoktorABC Sales Login">
        <div className="brand-lock">
          <div className="brand-mark">
            <Cross className="brand-pharmacy-mark" size={42} />
            <ChartCandlestick className="brand-chart-mark" size={30} />
          </div>
          <div>
            <p>Rats-Apotheke Blieskastel</p>
            <h1>DoktorABC Sales</h1>
          </div>
        </div>

        <form onSubmit={submitLogin} className="login-form">
          <label>
            <span>Dashboard-Passwort</span>
            <div className="password-field">
              <LockKeyhole size={18} />
              <input
                autoComplete="current-password"
                autoFocus
                onChange={(event) => setPassword(event.target.value)}
                placeholder="Passwort eingeben"
                type="password"
                value={password}
              />
            </div>
          </label>

          {error ? <p className="form-error">{error}</p> : null}

          <button disabled={loading || !password} type="submit">
            {loading ? "Pruefen..." : "Dashboard oeffnen"}
          </button>
        </form>
      </section>
    </main>
  );
}
