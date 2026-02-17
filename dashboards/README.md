# Purple Wave Dashboards

Dashboard POC — HTML-based reporting with Tableau-quality filtering and visualization.

## Directory Structure

```
dashboards/
├── static/              ← Shared assets (DO NOT edit per-report)
│   ├── theme.css        ← Brand colors, typography, variables
│   ├── layout.css       ← Header, filters, KPIs, cards, tables
│   ├── pw-utils.js      ← Formatters, Chart.js config, helpers
│   └── pw-filters.js    ← Cascading geo filter component
├── reports/             ← Individual report HTML files
│   └── alv-september.html
├── server/              ← FastAPI server (optional, for live data)
│   └── app.py
└── README.md
```

## Quick Start (No Server)

Just open the HTML file in a browser:

```bash
cd ~/dbt-learning/dbt/dashboards
# Option 1: Direct file open
xdg-open reports/alv-september.html

# Option 2: Local HTTP server (needed for relative CSS/JS paths)
python3 -m http.server 8080
# Then open: http://localhost:8080/reports/alv-september.html
```

## With FastAPI Server (Live Data)

```bash
pip install fastapi uvicorn psycopg2-binary

cd ~/dbt-learning/dbt/dashboards
python server/app.py

# Open: http://localhost:8080/reports/alv-september.html
# API:  http://localhost:8080/api/tables
```

The server provides:
- `GET /api/query?table=itemv2&group_by=Item Region Id&filters={"Month...":"September"}`
- `GET /api/distinct?table=itemv2&column=Item Region Id`
- Static file serving for `/static/` and `/reports/`

## Creating a New Report

1. Copy `reports/alv-september.html`
2. Change only the **DATA** section and the **header text**
3. Everything else (theme, filters, chart styles) is inherited from `/static/`

The report file has three clear sections:
- **HTML template** — header, filters, chart containers (shared structure)
- **DATA block** — the JSON array of pre-aggregated rows (unique per report)  
- **Rendering logic** — uses `PW.*` utilities (mostly reusable)

## Design System

All reports share:
- Purple Wave brand colors (purple/gold)
- Dark theme with consistent surface hierarchy
- DM Sans for UI text, JetBrains Mono for numbers
- Green/red/amber status indicators at $10k goal threshold
- Cascading geographic filters (Region → District → Territory)
- KPI cards, bar charts with goal lines, doughnut charts, data tables
