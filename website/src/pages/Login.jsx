export default function Login() {
  return (
    <div style={{ padding: "2rem", maxWidth: "400px" }}>
      <h1>Login</h1>

      <p>Melde dich mit deinem Benutzerkonto an.</p>

      <form>
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

        <button type="submit">Login</button>
      </form>

      <p style={{ marginTop: "1rem" }}>
        Noch kein Konto? <a href="/signup">Jetzt registrieren</a>
      </p>
    </div>
  );
}
