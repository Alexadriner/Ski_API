import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext";
import "./stylesheets/navbar.css";

export default function Navbar() {
  const { loggedIn, logout } = useAuth();
  const navigate = useNavigate();

  function handleLogout() {
    logout();
    navigate("/login");
  }

  return (
    <nav className="navbar">
      <div className="nav-section">
        <Link className="nav-button" to="/">Home</Link>
        <Link className="nav-button" to="/resorts">Resorts</Link>
        <Link className="nav-button" to="/map">Ski Map</Link>
        <Link className="nav-button" to="/contact">Contact</Link>
      </div>

      <div className="nav-section">
        <Link className="nav-button" to="/api">API</Link>
        <Link className="nav-button" to="/api/demo">API-Demo</Link>
        {loggedIn ? (
          <>
            <Link className="nav-button" to="/user">Account</Link>
            <button className="nav-button" type="button" onClick={handleLogout}>
              Logout
            </button>
          </>
        ) : (
          <>
            <Link className="nav-button" to="/login">Login</Link>
            <Link className="nav-button" to="/signup">Sign Up</Link>
          </>
        )}
      </div>
    </nav>
  );
}
