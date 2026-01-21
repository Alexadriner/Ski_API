import { Link } from "react-router-dom";

export default function Navbar() {
  return (
    <nav>
      <Link to="/">Start</Link>
      <Link to="/resorts">Skigebiete</Link>
      <Link to="/map">Skimap</Link>
      <Link to="/api">API</Link>
      <Link to="/api/demo">Demo</Link>
      <Link to="/user">Benutzer</Link>
      <Link to="/contact">Kontakt</Link>
      <Link to="/login">Login</Link>
    </nav>
  );
}