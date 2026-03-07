const API_BASE = "http://localhost:8080";
const FALLBACK_API_KEY = "R3StTY4OfadeFJZurXdZ1pZMVbWB3zWuL6FnuPGIbvA";

function normalizePath(path) {
  if (!path) {
    return "/";
  }
  return path.startsWith("/") ? path : `/${path}`;
}

export async function apiFetch(path, options = {}) {
  const url = new URL(`${API_BASE}${normalizePath(path)}`);
  const apiKey = options.apiKey || localStorage.getItem("apiKey") || FALLBACK_API_KEY;
  const method = String(options.method || "GET").toUpperCase();

  if (apiKey && method === "GET") {
    url.searchParams.set("api_key", apiKey);
  }

  const { apiKey: _, ...fetchOptions } = options;
  const res = await fetch(url.toString(), {
    headers: {
      "Content-Type": "application/json",
      ...(apiKey && method !== "GET" ? { Authorization: `Bearer ${apiKey}` } : {}),
      ...(fetchOptions.headers || {}),
    },
    method,
    ...fetchOptions,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "API error");
  }

  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return res.json();
  }

  return res.text();
}
