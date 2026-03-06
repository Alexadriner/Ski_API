import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { apiFetch } from "../api/client";
import "./stylesheets/searchInput.css";

import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faMagnifyingGlass } from "@fortawesome/free-solid-svg-icons";

export default function SearchInputWithSuggestions({ placeholder }) {
  const [query, setQuery] = useState("");
  const [suggestions, setSuggestions] = useState([]);
  const [allResorts, setAllResorts] = useState([]);
  const navigate = useNavigate();

  useEffect(() => {
    apiFetch("resorts")
      .then((data) => setAllResorts(data))
      .catch((err) => console.error(err));
  }, []);

  useEffect(() => {
    if (!query) {
      setSuggestions([]);
      return;
    }

    const filtered = allResorts
      .filter((r) => r.name.toLowerCase().includes(query.toLowerCase()))
      .slice(0, 10);

    setSuggestions(filtered);
  }, [query, allResorts]);

  const handleSelect = (resort) => {
    navigate(`/resort/${encodeURIComponent(resort.name)}`);
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (suggestions[0]) {
      handleSelect(suggestions[0]);
    }
  };

  return (
    <div className="search-wrapper" style={{ width: "340px", margin: "20px auto" }}>
      <form onSubmit={handleSubmit}>
        <input
          className="search-input"
          placeholder={placeholder}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <button className="search-button" aria-label="Search">
          <FontAwesomeIcon icon={faMagnifyingGlass} />
        </button>
      </form>

      {suggestions.length > 0 && (
        <ul className="suggestions-list">
          {suggestions.map((resort) => (
            <li
              key={resort.id}
              className="suggestion-item"
              onClick={() => handleSelect(resort)}
            >
              {resort.name}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
