export default function UserDashboard() {
  return (
    <div style={{ padding: "2rem", maxWidth: "800px" }}>
      <h1>Benutzer-Dashboard</h1>

      <section style={{ marginBottom: "2rem" }}>
        <h2>Profil</h2>
        <p>
          <strong>Name:</strong> Platzhalter Name
          <br />
          <strong>E-Mail:</strong> user@example.com
          <br />
          <strong>Abonnement:</strong> Free
        </p>
      </section>

      <section style={{ marginBottom: "2rem" }}>
        <h2>API-Keys</h2>

        <ul>
          <li>
            <code>sk_test_123456</code> – aktiv – Rate Limit: 1000 req/Tag
          </li>
          <li>
            <code>sk_test_abcdef</code> – deaktiviert
          </li>
        </ul>

        <button>Neuen API-Key erstellen</button>
      </section>

      <section>
        <h2>Account-Einstellungen</h2>
        <button>Passwort ändern</button>{" "}
        <button>E-Mail ändern</button>{" "}
        <button style={{ color: "red" }}>Konto löschen</button>
      </section>
    </div>
  );
}
