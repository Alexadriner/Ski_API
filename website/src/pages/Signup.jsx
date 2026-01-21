export default function Signup() {
  return (
    <div style={{ padding: "2rem", maxWidth: "400px" }}>
      <h1>Registrieren</h1>

      <p>Erstelle ein neues Benutzerkonto.</p>

      <form>
        <div style={{ marginBottom: "1rem" }}>
          <label>
            Name
            <br />
            <input
              type="text"
              placeholder="Max Mustermann"
              style={{ width: "100%" }}
            />
          </label>
        </div>

        <div style={{ marginBottom: "1rem" }}>
          <label>
            E-Mail
            <br />
            <input
              type="email"
              placeholder="name@example.com"
              style={{ width: "100%" }}
            />
          </label>
        </div>

        <div style={{ marginBottom: "1rem" }}>
          <label>
            Passwort
            <br />
            <input
              type="password"
              placeholder="********"
              style={{ width: "100%" }}
            />
          </label>
        </div>

        <button type="submit">Konto erstellen</button>
      </form>

      <p style={{ marginTop: "1rem" }}>
        Bereits registriert? <a href="/login">Zum Login</a>
      </p>
    </div>
  );
}