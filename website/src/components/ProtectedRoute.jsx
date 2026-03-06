import { Navigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext";

export default function ProtectedRoute({ children }) {
  const { loggedIn, loading } = useAuth();

  if (loading) {
    return <p>Loading user session...</p>;
  }

  if (!loggedIn) {
    return <Navigate to="/login" />;
  }

  return children;
}
