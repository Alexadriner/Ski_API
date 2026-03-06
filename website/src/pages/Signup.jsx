import { useState } from "react";
import { signup } from "../api/auth";
import { useAuth } from "../context/AuthContext";
import { useNavigate } from "react-router-dom";

export default function Signup() {
  const [username, setUsername] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [apiKey, setApiKey] = useState(null);
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const { login } = useAuth();
  const navigate = useNavigate();

  async function handleSubmit(e) {
    e.preventDefault();
    setError("");
    setSubmitting(true);

    try {
      const authData = await signup(username, email, password);
      login(authData);
      setApiKey(authData.api_key);
    } catch (err) {
      setError(err.message || "Sign up failed");
    } finally {
      setSubmitting(false);
    }
  }

  if (apiKey) {
    return (
      <div className="page-container">
        <h1>Important</h1>
        <p>Save your API key now:</p>
        <code>{apiKey}</code>
        <p>This key will not be shown again for security reasons.</p>
        <button onClick={() => navigate("/user")}>Continue to Dashboard</button>
      </div>
    );
  }

  return (
    <div className="page-container">
      <h1>Sign Up</h1>

      <form onSubmit={handleSubmit}>
        <input
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          placeholder="Name"
          autoComplete="name"
          required
        />

        <input
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          placeholder="Email"
          autoComplete="email"
          required
        />

        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password"
          autoComplete="new-password"
          required
        />

        <button disabled={submitting}>{submitting ? "Signing up..." : "Sign Up"}</button>
      </form>

      {error && <p style={{ color: "crimson" }}>{error}</p>}
    </div>
  );
}
