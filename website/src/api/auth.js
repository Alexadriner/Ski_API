const API_BASE = "http://localhost:8080";

function getErrorMessage(text, fallback) {
  if (!text) {
    return fallback;
  }
  return text;
}

export async function signup(username, email, password) {
  const res = await fetch(`${API_BASE}/signup`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ username, email, password }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(getErrorMessage(text, "Signup failed"));
  }

  return res.json();
}

export async function signin(email, password) {
  const res = await fetch(`${API_BASE}/signin`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ email, password }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(getErrorMessage(text, "Signin failed"));
  }

  return res.json();
}

export async function getMe(apiKey) {
  if (!apiKey) {
    throw new Error("Missing API key");
  }

  const url = new URL(`${API_BASE}/me`);
  url.searchParams.set("api_key", apiKey);

  const res = await fetch(url.toString(), { method: "GET" });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(getErrorMessage(text, "Could not load user"));
  }

  return res.json();
}
