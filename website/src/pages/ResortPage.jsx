import { useParams } from "react-router-dom";
import { useEffect, useState } from "react";
import { apiFetch } from "../api/client";

import "../stylesheets/resort-page.css";
import "../stylesheets/base.css";

function normalizeResortName(value) {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[\u2010-\u2015]/g, "-")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function safeDecode(value) {
  try {
    return decodeURIComponent(value ?? "");
  } catch {
    return value ?? "";
  }
}

export default function ResortPage() {
  const { name } = useParams();

  const [resort, setResort] = useState(null);
  const [isNotFound, setIsNotFound] = useState(false);

  useEffect(() => {
    async function loadData() {
      try {
        setIsNotFound(false);
        setResort(null);

        const resortsData = await apiFetch("resorts?summary=true");
        const targetName = normalizeResortName(safeDecode(name));
        const foundResort = resortsData.find(
          (r) => normalizeResortName(r.name) === targetName
        );

        if (!foundResort?.id) {
          setIsNotFound(true);
          return;
        }

        const resortDetail = await apiFetch(`/resorts/${foundResort.id}`);
        setResort(resortDetail);
      } catch (err) {
        console.error("API error:", err);
        setIsNotFound(true);
      }
    }

    loadData();
  }, [name]);

  if (isNotFound) return <p>Resort not found.</p>;
  if (!resort) return <p>Loading resort...</p>;

  const country = resort.geography?.country ?? resort.country ?? "N/A";
  const region = resort.geography?.region ?? resort.region;
  const continent = resort.geography?.continent ?? resort.continent;

  const latitude = resort.geography?.coordinates?.latitude ?? resort.latitude;
  const longitude = resort.geography?.coordinates?.longitude ?? resort.longitude;

  const villageAltitude = resort.altitude?.village_m ?? resort.village_altitude_m;
  const minAltitude = resort.altitude?.min_m ?? resort.min_altitude_m;
  const maxAltitude = resort.altitude?.max_m ?? resort.max_altitude_m;

  const skiAreaName = resort.ski_area?.name ?? resort.ski_area_name;
  const skiAreaType = resort.ski_area?.area_type ?? resort.ski_area_type;

  const slopes = resort.slopes ?? [];
  const lifts = resort.lifts ?? [];
  const liveStatus = resort.live_status ?? {};

  const getLiftType = (lift) => lift.lift_type ?? lift.display?.lift_type ?? "N/A";
  const getLiftStatus = (lift) =>
    lift.status?.operational_status ?? lift.operational_status ?? "unknown";

  const getSlopeDifficulty = (slope) => slope.difficulty ?? slope.display?.difficulty ?? "N/A";
  const getSlopeStatus = (slope) =>
    slope.status?.operational_status ?? slope.operational_status ?? "unknown";
  const getSlopeGrooming = (slope) =>
    slope.status?.grooming_status ?? slope.grooming_status ?? "unknown";

  const formatStatusLabel = (value) =>
    String(value ?? "unknown")
      .replace(/_/g, " ")
      .replace(/\b\w/g, (m) => m.toUpperCase());

  return (
    <div className="page-container resort-page">
      <div className="resort-header">
        <h1>{resort.name}</h1>

        <p>
          {country}
          {region ? ` - ${region}` : ""}
        </p>

        <div className="resort-info-grid">
          <p>Continent: {continent ?? "N/A"}</p>
          <p>Village altitude: {villageAltitude ?? "N/A"} m</p>
          <p>Min altitude: {minAltitude ?? "N/A"} m</p>
          <p>Max altitude: {maxAltitude ?? "N/A"} m</p>
          <p>Ski area: {skiAreaName ?? "N/A"}</p>
          <p>Ski area type: {skiAreaType ?? "N/A"}</p>
          <p>Total slopes: {slopes.length}</p>
          <p>Total lifts: {lifts.length}</p>
          <p>Open lifts: {liveStatus.lifts_open_count ?? "N/A"}</p>
          <p>Open slopes: {liveStatus.slopes_open_count ?? "N/A"}</p>
          <p>
            Coordinates:{" "}
            {latitude != null && longitude != null ? `${latitude}, ${longitude}` : "N/A"}
          </p>
        </div>
      </div>

      <div className="tables-container">
        <div className="table-box">
          <h2>Slopes</h2>

          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Difficulty</th>
                <th>Status</th>
                <th>Grooming</th>
              </tr>
            </thead>

            <tbody>
              {slopes.map((slope) => (
                <tr key={slope.id}>
                  <td>{slope.name ?? "Unknown"}</td>
                  <td className={`difficulty ${getSlopeDifficulty(slope)}`}>
                    {getSlopeDifficulty(slope)}
                  </td>
                  <td>
                    <span className={`status-badge ${String(getSlopeStatus(slope)).toLowerCase()}`}>
                      {formatStatusLabel(getSlopeStatus(slope))}
                    </span>
                  </td>
                  <td>
                    <span
                      className={`status-badge grooming ${String(
                        getSlopeGrooming(slope)
                      ).toLowerCase()}`}
                    >
                      {formatStatusLabel(getSlopeGrooming(slope))}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="table-box">
          <h2>Lifts</h2>

          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Type</th>
                <th>Status</th>
              </tr>
            </thead>

            <tbody>
              {lifts.map((lift) => (
                <tr key={lift.id}>
                  <td>{lift.name ?? "Unnamed"}</td>
                  <td>{getLiftType(lift)}</td>
                  <td>
                    <span className={`status-badge ${String(getLiftStatus(lift)).toLowerCase()}`}>
                      {formatStatusLabel(getLiftStatus(lift))}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
