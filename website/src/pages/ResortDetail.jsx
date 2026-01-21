import { useParams } from "react-router-dom";
import { useEffect, useState } from "react";
import { apiFetch } from "../api/client";

export default function ResortDetail() {
  const { id } = useParams();
  const [resort, setResort] = useState(null);

  useEffect(() => {
    apiFetch(`/resorts/${id}`).then(setResort);
  }, [id]);

  if (!resort) return <p>Lade...</p>;

  return (
    <div>
      <h1>{resort.name}</h1>
      <p>{resort.land} – {resort.region}</p>
      <p>Höhe: {resort.höhe} m</p>
    </div>
  );
}