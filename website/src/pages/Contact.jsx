import "../stylesheets/base.css";

export default function Contact() {
  return (
    <div className="page-container">
      <h1>Contact</h1>

      <p>
        Do you have questions about SkiAPI, want new features, or need support?
        Feel free to reach out.
      </p>

      <h2>Contact Information</h2>
      <ul>
        <li>
          <strong>Support:</strong>{" "}
          <a href="mailto:support@skiapi.example">support@skiapi.example</a>
        </li>
        <li>
          <strong>Business:</strong>{" "}
          <a href="mailto:business@skiapi.example">business@skiapi.example</a>
        </li>
      </ul>

      <h2>Feedback & Requests</h2>
      <p>
        We appreciate feedback, feature requests, and general inquiries.
        A contact form will be added in a future version.
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
          <em>Contact form (Coming Soon)</em>
        </p>
      </div>
    </div>
  );
}
