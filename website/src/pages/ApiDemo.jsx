export default function ApiDemo() {
  return (
    <div>
      <h1>API Demo</h1>
      <p>
        Teste hier unsere SkiAPI direkt im Browser.
      </p>

      <label>
        Endpoint:
        <select>
          <option>/api/v1/resorts</option>
          <option>/api/v1/resorts/:id</option>
          <option>/api/v1/search</option>
        </select>
      </label>

      <br /><br />

      <button>Request senden</button>

      <h3>Response</h3>
      <pre>
        {"{ }"}
      </pre>
    </div>
  );
}