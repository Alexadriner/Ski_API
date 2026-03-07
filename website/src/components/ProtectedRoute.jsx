import { Navigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext";

export default function ProtectedRoute({ children, requireAdmin = false }) {
  const { loggedIn, loading, user } = useAuth();

  if (loading) {
    return <p>Loading user session...</p>;
  }

  if (!loggedIn) {
    return <Navigate to="/login" />;
  }

  if (requireAdmin && !user?.is_admin) {
    return <Navigate to="/user" />;
  }

  return children;
}
