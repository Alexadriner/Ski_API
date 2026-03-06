import SearchInputWithSuggestions from "../components/SearchInputWithSuggestions";
import { Link } from "react-router-dom";

import "../stylesheets/base.css";
import "../stylesheets/home.css";

export default function Home() {
  return (
    <div className="page-container">
      <h1>SkiAPI & Resorts</h1>
      <p>Find ski resorts or use our high-performance API.</p>

      <div>
        <SearchInputWithSuggestions placeholder="Search ski resort..." />
        <Link to="/api/demo" className="home-button">Try the API</Link>
        <Link to="/resorts" className="home-button">Explore Resorts</Link>
      </div>
    </div>
  );
}
