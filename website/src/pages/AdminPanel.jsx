import { useEffect, useMemo, useState } from "react";
import { apiFetch } from "../api/client";
import "../stylesheets/base.css";
import "../stylesheets/admin.css";

function normalizeName(value) {
  if (!value) return "";
  return String(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function resortToPayload(resort) {
  return {
    name: resort.name ?? "",
    country: resort.geography?.country ?? resort.country ?? "",
    region: resort.geography?.region ?? resort.region ?? null,
    continent: resort.geography?.continent ?? resort.continent ?? null,
    latitude: resort.geography?.coordinates?.latitude ?? resort.latitude ?? null,
    longitude: resort.geography?.coordinates?.longitude ?? resort.longitude ?? null,
    village_altitude_m: resort.altitude?.village_m ?? resort.village_altitude_m ?? null,
    min_altitude_m: resort.altitude?.min_m ?? resort.min_altitude_m ?? null,
    max_altitude_m: resort.altitude?.max_m ?? resort.max_altitude_m ?? null,
    ski_area_name: resort.ski_area?.name ?? resort.ski_area_name ?? null,
    ski_area_type: resort.ski_area?.area_type ?? resort.ski_area_type ?? "alpine",
    official_website: resort.sources?.official_website ?? resort.official_website ?? null,
    lift_status_url: resort.sources?.lift_status_url ?? resort.lift_status_url ?? null,
    slope_status_url: resort.sources?.slope_status_url ?? resort.slope_status_url ?? null,
    snow_report_url: resort.sources?.snow_report_url ?? resort.snow_report_url ?? null,
    weather_url: resort.sources?.weather_url ?? resort.weather_url ?? null,
    status_provider: resort.sources?.status_provider ?? resort.status_provider ?? null,
    status_last_scraped_at:
      resort.live_status?.last_scraped_at ?? resort.status_last_scraped_at ?? null,
    lifts_open_count: resort.live_status?.lifts_open_count ?? resort.lifts_open_count ?? null,
    slopes_open_count: resort.live_status?.slopes_open_count ?? resort.slopes_open_count ?? null,
    snow_depth_valley_cm:
      resort.live_status?.snow_depth_valley_cm ?? resort.snow_depth_valley_cm ?? null,
    snow_depth_mountain_cm:
      resort.live_status?.snow_depth_mountain_cm ?? resort.snow_depth_mountain_cm ?? null,
    new_snow_24h_cm: resort.live_status?.new_snow_24h_cm ?? resort.new_snow_24h_cm ?? null,
    temperature_valley_c:
      resort.live_status?.temperature_valley_c ?? resort.temperature_valley_c ?? null,
    temperature_mountain_c:
      resort.live_status?.temperature_mountain_c ?? resort.temperature_mountain_c ?? null,
  };
}

function liftToPayload(lift, resortId) {
  return {
    resort_id: resortId ?? lift.resort_id ?? "",
    name: lift.name ?? null,
    lift_type: lift.display?.lift_type ?? lift.lift_type ?? "chairlift",
    capacity_per_hour: lift.specs?.capacity_per_hour ?? lift.capacity_per_hour ?? null,
    seats: lift.specs?.seats ?? lift.seats ?? null,
    bubble: lift.specs?.bubble ?? lift.bubble ?? false,
    heated_seats: lift.specs?.heated_seats ?? lift.heated_seats ?? false,
    year_built: lift.specs?.year_built ?? lift.year_built ?? null,
    altitude_start_m: lift.specs?.altitude_start_m ?? lift.altitude_start_m ?? null,
    altitude_end_m: lift.specs?.altitude_end_m ?? lift.altitude_end_m ?? null,
    lat_start: lift.geometry?.start?.latitude ?? lift.lat_start ?? null,
    lon_start: lift.geometry?.start?.longitude ?? lift.lon_start ?? null,
    lat_end: lift.geometry?.end?.latitude ?? lift.lat_end ?? null,
    lon_end: lift.geometry?.end?.longitude ?? lift.lon_end ?? null,
    source_system: lift.source?.system ?? lift.source_system ?? "osm",
    source_entity_id: lift.source?.entity_id ?? lift.source_entity_id ?? null,
    name_normalized: lift.display?.normalized_name ?? lift.name_normalized ?? normalizeName(lift.name),
    operational_status: lift.status?.operational_status ?? lift.operational_status ?? "unknown",
    operational_note: lift.status?.note ?? lift.operational_note ?? null,
    planned_open_time: lift.status?.planned_open_time ?? lift.planned_open_time ?? null,
    planned_close_time: lift.status?.planned_close_time ?? lift.planned_close_time ?? null,
    status_updated_at: lift.status?.updated_at ?? lift.status_updated_at ?? null,
    status_source_url: lift.source?.source_url ?? lift.status_source_url ?? null,
  };
}

function slopeToPayload(slope, resortId) {
  return {
    resort_id: resortId ?? slope.resort_id ?? "",
    name: slope.name ?? null,
    difficulty: slope.display?.difficulty ?? slope.difficulty ?? "blue",
    length_m: slope.specs?.length_m ?? slope.length_m ?? null,
    vertical_drop_m: slope.specs?.vertical_drop_m ?? slope.vertical_drop_m ?? null,
    average_gradient: slope.specs?.average_gradient ?? slope.average_gradient ?? null,
    max_gradient: slope.specs?.max_gradient ?? slope.max_gradient ?? null,
    snowmaking: slope.specs?.snowmaking ?? slope.snowmaking ?? false,
    night_skiing: slope.specs?.night_skiing ?? slope.night_skiing ?? false,
    family_friendly: slope.specs?.family_friendly ?? slope.family_friendly ?? false,
    race_slope: slope.specs?.race_slope ?? slope.race_slope ?? false,
    lat_start: slope.geometry?.start?.latitude ?? slope.lat_start ?? null,
    lon_start: slope.geometry?.start?.longitude ?? slope.lon_start ?? null,
    lat_end: slope.geometry?.end?.latitude ?? slope.lat_end ?? null,
    lon_end: slope.geometry?.end?.longitude ?? slope.lon_end ?? null,
    source_system: slope.source?.system ?? slope.source_system ?? "osm",
    source_entity_id: slope.source?.entity_id ?? slope.source_entity_id ?? null,
    name_normalized:
      slope.display?.normalized_name ?? slope.name_normalized ?? normalizeName(slope.name),
    operational_status: slope.status?.operational_status ?? slope.operational_status ?? "unknown",
    grooming_status: slope.status?.grooming_status ?? slope.grooming_status ?? "unknown",
    operational_note: slope.status?.note ?? slope.operational_note ?? null,
    status_updated_at: slope.status?.updated_at ?? slope.status_updated_at ?? null,
    status_source_url: slope.source?.source_url ?? slope.status_source_url ?? null,
  };
}

function defaultNewPayload(entityType, resortId) {
  if (entityType === "resorts") {
    return {
      id: "",
      name: "",
      country: "",
      region: null,
      continent: null,
      latitude: null,
      longitude: null,
      village_altitude_m: null,
      min_altitude_m: null,
      max_altitude_m: null,
      ski_area_name: null,
      ski_area_type: "alpine",
      official_website: null,
      lift_status_url: null,
      slope_status_url: null,
      snow_report_url: null,
      weather_url: null,
      status_provider: null,
      status_last_scraped_at: null,
      lifts_open_count: null,
      slopes_open_count: null,
      snow_depth_valley_cm: null,
      snow_depth_mountain_cm: null,
      new_snow_24h_cm: null,
      temperature_valley_c: null,
      temperature_mountain_c: null,
    };
  }

  if (entityType === "lifts") {
    return {
      resort_id: resortId || "",
      name: "",
      lift_type: "chairlift",
      capacity_per_hour: null,
      seats: null,
      bubble: false,
      heated_seats: false,
      year_built: null,
      altitude_start_m: null,
      altitude_end_m: null,
      lat_start: null,
      lon_start: null,
      lat_end: null,
      lon_end: null,
      source_system: "website",
      source_entity_id: null,
      name_normalized: null,
      operational_status: "unknown",
      operational_note: null,
      planned_open_time: null,
      planned_close_time: null,
      status_updated_at: null,
      status_source_url: null,
    };
  }

  return {
    resort_id: resortId || "",
    name: "",
    difficulty: "blue",
    length_m: null,
    vertical_drop_m: null,
    average_gradient: null,
    max_gradient: null,
    snowmaking: false,
    night_skiing: false,
    family_friendly: false,
    race_slope: false,
    lat_start: null,
    lon_start: null,
    lat_end: null,
    lon_end: null,
    source_system: "website",
    source_entity_id: null,
    name_normalized: null,
    operational_status: "unknown",
    grooming_status: "unknown",
    operational_note: null,
    status_updated_at: null,
    status_source_url: null,
  };
}

export default function AdminPanel() {
  const [resorts, setResorts] = useState([]);
  const [resortQuery, setResortQuery] = useState("");
  const [selectedResortId, setSelectedResortId] = useState("");

  const [entityType, setEntityType] = useState("resorts");
  const [entityQuery, setEntityQuery] = useState("");
  const [items, setItems] = useState([]);
  const [selectedItemId, setSelectedItemId] = useState("");
  const [editorText, setEditorText] = useState("{}");

  const [editMode, setEditMode] = useState(false);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const [manualMethod, setManualMethod] = useState("GET");
  const [manualPath, setManualPath] = useState("/resorts");
  const [manualBody, setManualBody] = useState("{}");
  const [manualResponse, setManualResponse] = useState("");

  useEffect(() => {
    loadResorts();
  }, []);

  useEffect(() => {
    setItems([]);
    setSelectedItemId("");
    setEditorText("{}");
    if (!selectedResortId && entityType !== "resorts") return;
    loadEntityData();
  }, [selectedResortId, entityType]);

  async function loadResorts() {
    try {
      const data = await apiFetch("/resorts");
      setResorts(Array.isArray(data) ? data : []);
      if (!selectedResortId && Array.isArray(data) && data.length > 0) {
        setSelectedResortId(data[0].id);
      }
    } catch (e) {
      setError(e.message || "Could not load resorts");
    }
  }

  async function loadEntityData() {
    setError("");
    setMessage("");
    try {
      if (entityType === "resorts") {
        if (!selectedResortId) return;
        const resort = await apiFetch(`/resorts/${selectedResortId}`);
        const payload = resortToPayload(resort);
        const item = {
          id: selectedResortId,
          label: `${resort.name ?? selectedResortId} (${selectedResortId})`,
          payload,
        };
        setItems([item]);
        setSelectedItemId(item.id);
        setEditorText(JSON.stringify(item.payload, null, 2));
        return;
      }

      const rows = await apiFetch(`/${entityType}/by_resort/${selectedResortId}`);
      const normalized = (Array.isArray(rows) ? rows : []).map((row) => ({
        id: String(row.id),
        label: `${row.name || "Unnamed"} (#${row.id})`,
        payload:
          entityType === "lifts"
            ? liftToPayload(row, selectedResortId)
            : slopeToPayload(row, selectedResortId),
      }));
      setItems(normalized);
      if (normalized.length > 0) {
        setSelectedItemId(normalized[0].id);
        setEditorText(JSON.stringify(normalized[0].payload, null, 2));
      }
    } catch (e) {
      setError(e.message || `Could not load ${entityType}`);
    }
  }

  const visibleResorts = useMemo(() => {
    const q = resortQuery.trim().toLowerCase();
    if (!q) return resorts;
    return resorts.filter((r) =>
      `${r.name || ""} ${r.id || ""} ${r.country || ""}`.toLowerCase().includes(q)
    );
  }, [resorts, resortQuery]);

  const visibleItems = useMemo(() => {
    const q = entityQuery.trim().toLowerCase();
    if (!q) return items;
    return items.filter((i) => i.label.toLowerCase().includes(q));
  }, [items, entityQuery]);

  function selectItem(item) {
    setSelectedItemId(item.id);
    setEditorText(JSON.stringify(item.payload, null, 2));
    setError("");
    setMessage("");
  }

  function newItem() {
    const payload = defaultNewPayload(entityType, selectedResortId);
    setSelectedItemId("__new__");
    setEditorText(JSON.stringify(payload, null, 2));
    setMessage(`New ${entityType.slice(0, -1)} template loaded.`);
    setError("");
  }

  async function saveItem() {
    if (!editMode) return;
    setBusy(true);
    setError("");
    setMessage("");

    try {
      const payload = JSON.parse(editorText);
      const isNew = selectedItemId === "__new__";

      if (entityType === "resorts") {
        if (isNew) {
          await apiFetch("/resorts", { method: "POST", body: JSON.stringify(payload) });
          setMessage(`Resort created: ${payload.id}`);
          setSelectedResortId(payload.id);
          await loadResorts();
          await loadEntityData();
        } else {
          await apiFetch(`/resorts/${selectedResortId}`, {
            method: "PUT",
            body: JSON.stringify(payload),
          });
          setMessage(`Resort updated: ${selectedResortId}`);
          await loadResorts();
          await loadEntityData();
        }
      } else {
        const pathBase = `/${entityType}`;
        if (isNew) {
          await apiFetch(pathBase, { method: "POST", body: JSON.stringify(payload) });
          setMessage(`${entityType.slice(0, -1)} created.`);
        } else {
          await apiFetch(`${pathBase}/${selectedItemId}`, {
            method: "PUT",
            body: JSON.stringify(payload),
          });
          setMessage(`${entityType.slice(0, -1)} updated (#${selectedItemId}).`);
        }
        await loadEntityData();
      }
    } catch (e) {
      setError(e.message || "Save failed");
    } finally {
      setBusy(false);
    }
  }

  async function deleteItem() {
    if (!editMode || selectedItemId === "__new__" || !selectedItemId) return;
    if (!window.confirm("Delete selected entry?")) return;

    setBusy(true);
    setError("");
    setMessage("");
    try {
      if (entityType === "resorts") {
        await apiFetch(`/resorts/${selectedResortId}`, { method: "DELETE" });
        setMessage(`Resort deleted: ${selectedResortId}`);
        setSelectedResortId("");
        await loadResorts();
      } else {
        await apiFetch(`/${entityType}/${selectedItemId}`, { method: "DELETE" });
        setMessage(`${entityType.slice(0, -1)} deleted (#${selectedItemId}).`);
        await loadEntityData();
      }
    } catch (e) {
      setError(e.message || "Delete failed");
    } finally {
      setBusy(false);
    }
  }

  async function runManualRequest() {
    if (!editMode) return;
    setBusy(true);
    setError("");
    setMessage("");
    setManualResponse("");
    try {
      const options = { method: manualMethod };
      if (manualMethod !== "GET" && manualMethod !== "DELETE") {
        options.body = manualBody.trim() ? JSON.stringify(JSON.parse(manualBody)) : "{}";
      }
      const result = await apiFetch(manualPath, options);
      setManualResponse(
        typeof result === "string" ? result : JSON.stringify(result, null, 2)
      );
      setMessage(`Manual request ${manualMethod} ${manualPath} succeeded.`);
    } catch (e) {
      setError(e.message || "Manual request failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="page-container admin-page">
      <h1>Admin Panel</h1>
      <p>Nur f&uuml;r Admins. Suche nach Skigebieten und bearbeite Daten direkt per API.</p>

      <label className="admin-edit-toggle">
        <input
          type="checkbox"
          checked={editMode}
          onChange={(e) => setEditMode(e.target.checked)}
        />
        Bearbeitungsmodus aktivieren (POST / PUT / DELETE)
      </label>

      {message && <p className="admin-ok">{message}</p>}
      {error && <p className="admin-error">{error}</p>}

      <section className="admin-grid">
        <div className="admin-column">
          <h2>Skigebiete</h2>
          <input
            value={resortQuery}
            onChange={(e) => setResortQuery(e.target.value)}
            placeholder="Resort suchen..."
          />
          <div className="admin-list">
            {visibleResorts.map((r) => (
              <button
                key={r.id}
                type="button"
                className={`admin-list-item ${selectedResortId === r.id ? "active" : ""}`}
                onClick={() => setSelectedResortId(r.id)}
              >
                {r.name} ({r.id})
              </button>
            ))}
          </div>
        </div>

        <div className="admin-column">
          <h2>Datenquelle</h2>
          <div className="admin-type-row">
            <button type="button" onClick={() => setEntityType("resorts")}>Resort</button>
            <button type="button" onClick={() => setEntityType("lifts")}>Lifte</button>
            <button type="button" onClick={() => setEntityType("slopes")}>Pisten</button>
          </div>
          <input
            value={entityQuery}
            onChange={(e) => setEntityQuery(e.target.value)}
            placeholder={`${entityType} filtern...`}
          />
          <div className="admin-type-row">
            <button type="button" onClick={loadEntityData}>Neu laden</button>
            <button type="button" onClick={newItem}>Neu erstellen</button>
          </div>
          <div className="admin-list">
            {visibleItems.map((item) => (
              <button
                key={item.id}
                type="button"
                className={`admin-list-item ${selectedItemId === item.id ? "active" : ""}`}
                onClick={() => selectItem(item)}
              >
                {item.label}
              </button>
            ))}
          </div>
        </div>

        <div className="admin-column admin-editor-col">
          <h2>JSON Editor</h2>
          <textarea
            className="admin-json"
            value={editorText}
            onChange={(e) => setEditorText(e.target.value)}
            readOnly={!editMode}
          />
          <div className="admin-type-row">
            <button type="button" disabled={!editMode || busy} onClick={saveItem}>
              {selectedItemId === "__new__" ? "POST erstellen" : "PUT speichern"}
            </button>
            <button type="button" disabled={!editMode || busy} onClick={deleteItem}>
              DELETE
            </button>
          </div>
        </div>
      </section>

      <section className="admin-manual">
        <h2>Manuelle Requests</h2>
        <div className="admin-type-row">
          <select value={manualMethod} onChange={(e) => setManualMethod(e.target.value)}>
            <option>GET</option>
            <option>POST</option>
            <option>PUT</option>
            <option>DELETE</option>
          </select>
          <input
            value={manualPath}
            onChange={(e) => setManualPath(e.target.value)}
            placeholder="/resorts/kreuzberg"
          />
          <button type="button" disabled={!editMode || busy} onClick={runManualRequest}>
            Request senden
          </button>
        </div>
        {(manualMethod === "POST" || manualMethod === "PUT") && (
          <textarea
            className="admin-json"
            value={manualBody}
            onChange={(e) => setManualBody(e.target.value)}
            readOnly={!editMode}
          />
        )}
        {manualResponse && (
          <pre className="admin-response">{manualResponse}</pre>
        )}
      </section>
    </div>
  );
}

