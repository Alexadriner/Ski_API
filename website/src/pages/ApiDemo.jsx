import { useEffect, useMemo, useState } from "react";
import { apiFetch } from "../api/client";
import "../stylesheets/base.css";
import "../stylesheets/api-demo.css";

const API_CONFIG = {
  resorts: {
    label: "Resorts API",
    endpoint: "resorts",
    idLabel: "Resort-ID",
    idPlaceholder: "zermatt",
  },
  lifts: {
    label: "Lifts API",
    endpoint: "lifts",
    idLabel: "Lift-ID",
    idPlaceholder: "12",
  },
  slopes: {
    label: "Slopes API",
    endpoint: "slopes",
    idLabel: "Slope-ID",
    idPlaceholder: "18",
  },
};

const PREVIEW_LIMIT = 5;

function buildPreviewData(data) {
  if (Array.isArray(data)) {
    return {
      shownItems: data.slice(0, PREVIEW_LIMIT),
      totalItems: data.length,
      isTruncated: data.length > PREVIEW_LIMIT,
    };
  }

  return {
    shownItems: data,
    totalItems: 1,
    isTruncated: false,
  };
}

export default function ApiDemo() {
  const [selectedApi, setSelectedApi] = useState("resorts");
  const [inputValue, setInputValue] = useState("");
  const [allIds, setAllIds] = useState([]);
  const [responseData, setResponseData] = useState(null);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const config = API_CONFIG[selectedApi];
  const trimmedInput = inputValue.trim();

  const suggestions = useMemo(() => {
    if (!trimmedInput) {
      return [];
    }

    const lower = trimmedInput.toLowerCase();
    return allIds.filter((id) => id.toLowerCase().includes(lower)).slice(0, 10);
  }, [allIds, trimmedInput]);

  const requestPath = trimmedInput
    ? `${config.endpoint}/${encodeURIComponent(trimmedInput)}`
    : config.endpoint;

  const requestUrl = `http://localhost:8080/${requestPath}?api_key=<API_KEY>`;

  useEffect(() => {
    const controller = new AbortController();

    async function runRequest() {
      setLoading(true);
      setError("");

      try {
        if (!trimmedInput) {
          const list = await apiFetch(config.endpoint, { signal: controller.signal });
          const ids = Array.isArray(list)
            ? list
                .map((item) => String(item?.id || "").trim())
                .filter((id) => id.length > 0)
            : [];
          const preview = buildPreviewData(list);

          setAllIds(ids);
          setResponseData(preview.shownItems);
          setMeta({
            isList: true,
            totalItems: preview.totalItems,
            isTruncated: preview.isTruncated,
          });
          return;
        }

        const detail = await apiFetch(
          `${config.endpoint}/${encodeURIComponent(trimmedInput)}`,
          { signal: controller.signal },
        );

        setMeta({
          isList: false,
          totalItems: 1,
          isTruncated: false,
        });
        setResponseData(detail);
      } catch (requestError) {
        if (requestError?.name === "AbortError") {
          return;
        }

        setResponseData(null);
        setMeta(null);
        setError(requestError?.message || "API request failed.");
      } finally {
        setLoading(false);
      }
    }

    runRequest();

    return () => controller.abort();
  }, [config.endpoint, trimmedInput]);

  return (
    <div className="page-container api-demo-page">
      <section className="api-demo-hero">
        <h1>API Demo</h1>
        <p>
          Test Resorts, Lifts, and Slopes directly in your browser with live URL preview and
          formatted JSON responses.
        </p>
      </section>

      <section className="api-demo-card">
        <div className="api-demo-controls">
          <label htmlFor="api-select">API</label>
          <select
            id="api-select"
            value={selectedApi}
            onChange={(e) => {
              setSelectedApi(e.target.value);
              setInputValue("");
              setError("");
            }}
          >
            {Object.entries(API_CONFIG).map(([value, item]) => (
              <option key={value} value={value}>
                {item.label}
              </option>
            ))}
          </select>

          <div className="api-demo-input-block">
            <label htmlFor="api-id-input">{config.idLabel}</label>
            <input
              id="api-id-input"
              type="text"
              value={inputValue}
              placeholder={config.idPlaceholder}
              onChange={(e) => setInputValue(e.target.value)}
            />

            {suggestions.length > 0 && (
              <ul className="api-demo-suggestions">
                {suggestions.map((suggestion) => (
                  <li key={suggestion}>
                    <button type="button" onClick={() => setInputValue(suggestion)}>
                      {suggestion}
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>

          <div className="api-demo-url-block">
            <p className="api-demo-url-label">Generated URL</p>
            <code>{requestUrl}</code>
          </div>
        </div>
      </section>

      <section className="api-demo-response-card">
        <div className="api-demo-response-head">
          <h2>Response</h2>
          {loading && <span>Loading...</span>}
          {!loading && meta?.isList && (
            <span>
              Showing first {Math.min(PREVIEW_LIMIT, meta.totalItems)} of {meta.totalItems} items
            </span>
          )}
        </div>

        {error ? (
          <p className="api-demo-error">{error}</p>
        ) : (
          <pre>{JSON.stringify(responseData, null, 2)}</pre>
        )}
      </section>
    </div>
  );
}
