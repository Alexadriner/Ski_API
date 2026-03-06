import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { getMe } from "../api/auth";

const AuthContext = createContext(undefined);

export function AuthProvider({ children }) {
  const [apiKey, setApiKey] = useState(() => localStorage.getItem("apiKey"));
  const [user, setUser] = useState(() => {
    const raw = localStorage.getItem("user");
    if (!raw) {
      return null;
    }
    try {
      return JSON.parse(raw);
    } catch (_) {
      return null;
    }
  });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function bootstrap() {
      if (!apiKey) {
        setLoading(false);
        return;
      }

      try {
        const me = await getMe(apiKey);
        setUser(me);
        localStorage.setItem("user", JSON.stringify(me));
      } catch (_) {
        localStorage.removeItem("apiKey");
        localStorage.removeItem("user");
        setApiKey(null);
        setUser(null);
      } finally {
        setLoading(false);
      }
    }

    bootstrap();
  }, [apiKey]);

  function login(authData) {
    localStorage.setItem("apiKey", authData.api_key);
    localStorage.setItem("user", JSON.stringify(authData.user));
    setApiKey(authData.api_key);
    setUser(authData.user);
  }

  function logout() {
    localStorage.removeItem("apiKey");
    localStorage.removeItem("user");
    setApiKey(null);
    setUser(null);
  }

  const value = useMemo(
    () => ({
      apiKey,
      user,
      loading,
      loggedIn: Boolean(apiKey),
      login,
      logout,
    }),
    [apiKey, user, loading]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error("useAuth must be used inside <AuthProvider>");
  }
  return ctx;
}
