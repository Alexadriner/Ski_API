export default function Contact() {
  return (
    <div style={{ padding: "2rem", maxWidth: "700px" }}>
      <h1>Kontakt</h1>

      <p>
        Du hast Fragen zur SkiAPI, wünschst dir neue Features oder brauchst
        Support? Melde dich gerne bei uns.
      </p>

      <h2>Kontaktinformationen</h2>
      <ul>
        <li>
          <strong>Support:</strong>{" "}
          <a href="mailto:support@skiapi.example">
            support@skiapi.example
          </a>
        </li>
        <li>
          <strong>Business:</strong>{" "}
          <a href="mailto:business@skiapi.example">
            business@skiapi.example
          </a>
        </li>
      </ul>

      <h2>Feedback & Wünsche</h2>
      <p>
        Wir freuen uns über Feedback, Feature-Wünsche oder allgemeine Anfragen.
        Ein Kontaktformular folgt in einer späteren Version.
      </p>

      <div
        style={{
          marginTop: "1.5rem",
          padding: "1rem",
          border: "1px dashed #aaa",
          borderRadius: "8px",
          background: "#f9f9f9",
        }}
      >
        <p>
          📬 <em>Kontaktformular (Coming Soon)</em>
        </p>
      </div>
    </div>
  );
}