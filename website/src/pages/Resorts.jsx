import { useEffect, useState } from "react";
import { apiFetch } from "../api/client";
import { Link } from "react-router-dom";

export default function Resorts() {
  const [resorts, setResorts] = useState([]);

  useEffect(() => {
    apiFetch("/resorts").then(setResorts);
  }, []);

  return (
    <div>
      <h1>Skigebiete</h1>
      <ul>
        {resorts.map(r => (
          <li key={r.id}>
            <Link to={`/resorts/${r.id}`}>
              {r.name} – {r.land}
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
}