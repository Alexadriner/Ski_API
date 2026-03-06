import "../stylesheets/base.css";
import { useAuth } from "../context/AuthContext";
import { useNavigate } from "react-router-dom";

function maskApiKey(key) {
  if (!key) {
    return "Not available";
  }
  if (key.length <= 10) {
    return "**********";
  }
  return `${key.slice(0, 6)}...${key.slice(-4)}`;
}

export default function UserDashboard() {
  const { user, apiKey, logout } = useAuth();
  const navigate = useNavigate();

  function handleLogout() {
    logout();
    navigate("/login");
  }

  return (
    <div className="page-container">
      <h1>User Dashboard</h1>

      <section style={{ marginBottom: "2rem" }}>
        <h2>Profile</h2>
        <p>
          <strong>Name:</strong> {user?.name ?? "Unknown"}
          <br />
          <strong>Email:</strong> {user?.email ?? "Unknown"}
          <br />
          <strong>Subscription:</strong> {user?.subscription ?? "Unknown"}
          <br />
          <strong>Role:</strong> {user?.is_admin ? "Admin" : "User"}
        </p>
      </section>

      <section style={{ marginBottom: "2rem" }}>
        <h2>API Key</h2>
        <p>Active key: <code>{maskApiKey(apiKey)}</code></p>
        <p style={{ color: "#555" }}>A new API key is issued at each login.</p>
      </section>

      <section>
        <h2>Account</h2>
        <button onClick={handleLogout} style={{ color: "red", borderColor: "red" }}>
          Logout
        </button>
      </section>
    </div>
  );
}
