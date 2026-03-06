import { useEffect, useState } from "react";
import { apiFetch } from "../api/client";
import { Link } from "react-router-dom";
import "../stylesheets/base.css";

export default function Resorts() {
  const [resorts, setResorts] = useState([]);

  useEffect(() => {
    apiFetch("/resorts").then(setResorts);
  }, []);

  return (
    <div className="page-container">
      <h1>Resorts</h1>
      <ul>
        {resorts.map((r) => (
          <li key={r.id}>
            <Link to={`/resorts/${r.id}`}>
              {r.name} - {r.country ?? "N/A"}
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
}
