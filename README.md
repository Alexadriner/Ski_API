# 🎿 Ski_API

> ⚠️ Disclaimer
>
> This project is **not intended for public use or deployment**. The backend relies on a private database that is not publicly accessible. The repository exists solely to showcase the project's architecture and code.

A full-stack application that aggregates, cleans, and serves ski resort data through a web interface with an interactive map. The project combines a Python-based data scraper, a Rust-powered backend server, and a JavaScript/HTML frontend to provide information about ski resorts.

Data is sourced from [OpenSkiMap](https://openskimap.org/).

---

## 📁 Project Structure

```
Ski_API/
├── scripts/              # Utility and helper scripts
├── server/               # Rust backend server (REST API)
├── testing/leaflet/      # Leaflet.js map testing/prototyping
├── website/              # Frontend (HTML, CSS, JavaScript)
├── ski_scraper.py        # Web scraper for ski resort data
├── cleanup_ski_data.py   # Data cleaning and normalization
├── cleanup_launcher.py   # Launcher for the cleanup pipeline
├── launcher.py           # Main application launcher
├── ski-resorts.csv       # Ski resort dataset
└── tmp_*.json / *.html   # Temporary scraped data files
```

---

## 🛠️ Tech Stack

| Layer       | Technology              |
|-------------|-------------------------|
| Backend     | Rust                    |
| Scraper     | Python                  |
| Frontend    | HTML, CSS, JavaScript   |
| Map         | Leaflet.js              |

---

## 🚀 Getting Started

### Prerequisites

- [Python 3.x](https://www.python.org/downloads/)
- [Rust & Cargo](https://www.rust-lang.org/tools/install)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Alexadriner/Ski_API.git
   cd Ski_API
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Build the Rust server:**
   ```bash
   cd server
   cargo build --release
   ```

---

## ▶️ Usage

### 1. Scrape Resort Data

`launcher.py` starts **20 parallel instances** of `ski_scraper.py` to speed up data collection:

```bash
python launcher.py
```

To run the scraper as a single instance instead:

```bash
python ski_scraper.py
```

### 2. Clean the Data

`cleanup_launcher.py` starts **20 parallel instances** of `cleanup_ski_data.py` to clean and normalize the scraped data:

```bash
python cleanup_launcher.py
```

To run the cleanup as a single instance instead:

```bash
python cleanup_ski_data.py
```

### 3. Start the Website

Navigate to the `website` folder and start the dev server:

```bash
cd website
npm run dev
```

---

## 🗺️ Features

- **Data Scraping** – Automatically fetches ski resort information (lift status, snow conditions, etc.) from external sources
- **Data Cleaning** – Normalizes and deduplicates resort data into a consistent CSV format
- **REST API** – High-performance Rust backend serves resort data as JSON
- **Interactive Map** – Leaflet.js-powered frontend visualizes resorts geographically
- **Web Interface** – Browse and explore ski resort details via a web UI

---

## 📄 Data

Resort data is sourced from [OpenSkiMap](https://openskimap.org/), a community-driven, open-source project based on OpenStreetMap data. The scraper fetches this data, which is then cleaned, normalized, and stored in a private database. The `ski-resorts.csv` in this repository serves as an example snapshot. Temporary files (prefixed with `tmp_`) are generated during the scraping process.

---

## ⚠️ Disclaimer

This project is **not intended for public use or deployment**. The backend relies on a private database that is not publicly accessible. The repository exists solely to showcase the project's architecture and code.
