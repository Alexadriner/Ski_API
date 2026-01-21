export default function Api() {
  return (
    <div>
      <h1>Ski API</h1>
      <p>
        Unsere API liefert strukturierte Skigebietsdaten für Entwickler:innen.
      </p>

      <h2>Beispiel-Endpunkte</h2>
      <pre>
        GET /api/v1/resorts{"\n"}
        GET /api/v1/resorts/:id{"\n"}
        GET /api/v1/search
      </pre>
    </div>
  );
}