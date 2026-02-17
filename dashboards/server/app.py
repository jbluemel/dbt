"""
Purple Wave Dashboard Server
Serves HTML dashboards and provides a query API to PostgreSQL.
Proxies chat requests to Claude API with tool use for database queries.

Usage:
  pip install fastapi uvicorn psycopg2-binary httpx
  cd ~/dbt-learning/dbt/dashboards
  python server/app.py
"""

import json
import os
from pathlib import Path
from contextlib import asynccontextmanager

import httpx
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# --- Config ---
DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5434")),
    "database": os.getenv("PG_DATABASE", "dbt_dev"),
    "user": os.getenv("PG_USER", "dbt_user"),
    "password": os.getenv("PG_PASSWORD", "dbt_password"),
}

ALLOWED_TABLES = {"itemv2"}

BASE_DIR = Path(__file__).resolve().parent.parent

# --- Tool definitions for Claude ---
CHAT_TOOLS = [
    {
        "name": "query_data",
        "description": "Query the Purple Wave auction database. Returns aggregated data grouped by the specified columns. Always available columns for grouping/filtering: 'Year of Auction Endtime - Fiscal Year', 'Month of Auction Endtime - Calendar Year', 'Quarter of Auction Endtime - Fiscal Year', 'Item Region Id', 'Item District', 'Item Territory Id', 'Item TM Name', 'Auction Category', 'Taxonomy Industry', 'Taxonomy Category', 'Taxonomy Family', 'Make', 'Item State', 'Item City'. The metric aggregated is Contract Price (returns lots count, avg_lot_value, total_revenue).",
        "input_schema": {
            "type": "object",
            "properties": {
                "group_by": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Columns to group by. Example: ['Month of Auction Endtime - Calendar Year'] or ['Item Region Id', 'Taxonomy Industry']"
                },
                "filters": {
                    "type": "object",
                    "description": "Column:value pairs to filter by. Example: {'Year of Auction Endtime - Fiscal Year': 'FY 2026', 'Item Region Id': '3.0'}"
                }
            },
            "required": ["group_by"]
        }
    },
    {
        "name": "get_distinct_values",
        "description": "Get all distinct values for a column. Useful for understanding what categories, regions, months, etc. exist in the data.",
        "input_schema": {
            "type": "object",
            "properties": {
                "column": {
                    "type": "string",
                    "description": "Column name to get distinct values for"
                },
                "filters": {
                    "type": "object",
                    "description": "Optional column:value pairs to filter before getting distinct values"
                }
            },
            "required": ["column"]
        }
    }
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    api_key = os.getenv("ANTHROPIC_API_KEY")
    print(f"Dashboard server starting...")
    print(f"Serving from: {BASE_DIR}")
    print(f"PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"Claude API: {'configured' if api_key else 'NOT SET - chat will not work'}")
    print(f"Open: http://localhost:8080")
    yield

app = FastAPI(title="PW Dashboards", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def run_query(group_by_cols, filters_dict=None, metric="Contract Price"):
    """Execute an aggregation query and return results."""
    where_clauses = []
    params = []

    if filters_dict:
        for col, val in filters_dict.items():
            where_clauses.append(f'"{col}" = %s')
            params.append(val)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    group_cols = [f'"{c.strip()}"' for c in group_by_cols]
    group_sql = ", ".join(group_cols)

    sql = f"""
        SELECT {group_sql},
            COUNT(*) as lots,
            ROUND(AVG("{metric}")::numeric, 2) as avg_lot_value,
            ROUND(SUM("{metric}")::numeric, 2) as total_revenue
        FROM itemv2
        {where_sql}
        GROUP BY {group_sql}
        ORDER BY {group_sql}
    """

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()

    for row in rows:
        for k, v in row.items():
            if hasattr(v, '__float__'):
                row[k] = float(v)

    return rows


def run_distinct(column, filters_dict=None):
    """Get distinct values for a column."""
    where_clauses = []
    params = []

    if filters_dict:
        for col, val in filters_dict.items():
            where_clauses.append(f'"{col}" = %s')
            params.append(val)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT DISTINCT "{column}"
        FROM itemv2
        {where_sql}
        ORDER BY "{column}"
    """

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    values = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()

    return values


def execute_tool(tool_name, tool_input):
    """Execute a tool call and return the result as a string."""
    try:
        if tool_name == "query_data":
            rows = run_query(
                group_by_cols=tool_input["group_by"],
                filters_dict=tool_input.get("filters")
            )
            return json.dumps({"data": rows, "count": len(rows)}, indent=2)

        elif tool_name == "get_distinct_values":
            values = run_distinct(
                column=tool_input["column"],
                filters_dict=tool_input.get("filters")
            )
            return json.dumps({"column": tool_input["column"], "values": values})

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    except Exception as e:
        return json.dumps({"error": str(e)})


# --- Data API Routes (for dashboard) ---

@app.get("/api/tables")
def list_tables():
    return {"tables": sorted(ALLOWED_TABLES)}


@app.get("/api/query")
def query_table(
    table: str = Query(..., description="Table name"),
    group_by: str = Query(None, description="Comma-separated columns to group by"),
    filters: str = Query(None, description="JSON object of column:value filters"),
    metric: str = Query("Contract Price", description="Column to aggregate"),
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(400, f"Table '{table}' not allowed. Available: {ALLOWED_TABLES}")

    where_clauses = []
    params = []

    if filters:
        try:
            filter_dict = json.loads(filters)
        except json.JSONDecodeError:
            raise HTTPException(400, "Invalid JSON in filters parameter")

        for col, val in filter_dict.items():
            where_clauses.append(f'"{col}" = %s')
            params.append(val)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    if group_by:
        group_cols = [f'"{c.strip()}"' for c in group_by.split(",")]
        group_sql = ", ".join(group_cols)
        select_cols = group_sql + ","

        sql = f"""
            SELECT {select_cols}
                COUNT(*) as lots,
                ROUND(AVG("{metric}")::numeric, 2) as avg_lot_value,
                ROUND(SUM("{metric}")::numeric, 2) as total_revenue
            FROM {table}
            {where_sql}
            GROUP BY {group_sql}
            ORDER BY {group_sql}
        """
    else:
        sql = f"""
            SELECT
                COUNT(*) as lots,
                ROUND(AVG("{metric}")::numeric, 2) as avg_lot_value,
                ROUND(SUM("{metric}")::numeric, 2) as total_revenue
            FROM {table}
            {where_sql}
        """

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()

        for row in rows:
            for k, v in row.items():
                if hasattr(v, '__float__'):
                    row[k] = float(v)

        return {"data": rows, "count": len(rows)}

    except Exception as e:
        raise HTTPException(500, f"Query error: {str(e)}")


@app.get("/api/distinct")
def distinct_values(
    table: str = Query(...),
    column: str = Query(...),
    filters: str = Query(None, description="JSON object of column:value filters"),
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(400, f"Table '{table}' not allowed.")

    where_clauses = []
    params = []

    if filters:
        try:
            filter_dict = json.loads(filters)
        except json.JSONDecodeError:
            raise HTTPException(400, "Invalid JSON in filters parameter")

        for col, val in filter_dict.items():
            where_clauses.append(f'"{col}" = %s')
            params.append(val)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT DISTINCT "{column}"
        FROM {table}
        {where_sql}
        ORDER BY "{column}"
    """

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(sql, params)
        values = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
        return {"column": column, "values": values}

    except Exception as e:
        raise HTTPException(500, f"Query error: {str(e)}")


# --- Chat Proxy with Tool Use Loop ---

@app.post("/api/chat")
async def chat_proxy(request: Request):
    """
    Proxy chat requests to Claude API with tool use.
    Handles the tool use loop: Claude requests tools, we execute them
    locally against PostgreSQL, and send results back until Claude
    produces a final text response.
    """
    body = await request.json()

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(500, "ANTHROPIC_API_KEY not set in environment")

    messages = list(body.get("messages", []))
    system = body.get("system", "")

    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }

    max_iterations = 5
    iteration = 0
    tool_calls_made = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        while iteration < max_iterations:
            iteration += 1

            claude_request = {
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1024,
                "system": system,
                "messages": messages,
                "tools": CHAT_TOOLS,
            }

            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=claude_request,
            )

            result = response.json()

            if response.status_code != 200:
                return JSONResponse(content=result, status_code=response.status_code)

            stop_reason = result.get("stop_reason", "")

            # If Claude is done (no more tool calls), return the response
            if stop_reason != "tool_use":
                # Attach tool call log so frontend can show what was queried
                result["_tool_calls"] = tool_calls_made
                return JSONResponse(content=result)

            # Claude wants to use tools - process them
            assistant_content = result.get("content", [])

            # Add Claude's response (with tool_use blocks) to messages
            messages.append({"role": "assistant", "content": assistant_content})

            # Execute each tool call and build tool_result messages
            tool_results = []
            for block in assistant_content:
                if block.get("type") == "tool_use":
                    tool_name = block["name"]
                    tool_input = block["input"]
                    tool_id = block["id"]

                    # Log the tool call
                    tool_calls_made.append({
                        "tool": tool_name,
                        "input": tool_input
                    })

                    print(f"  Tool call [{iteration}]: {tool_name}({json.dumps(tool_input, indent=None)})")

                    # Execute against local PostgreSQL
                    tool_result = execute_tool(tool_name, tool_input)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": tool_result,
                    })

            # Add tool results to messages
            messages.append({"role": "user", "content": tool_results})

    # If we hit max iterations, return what we have
    return JSONResponse(content={
        "content": [{"type": "text", "text": "I ran out of investigation steps. Here is what I found so far based on the queries I made."}],
        "_tool_calls": tool_calls_made,
    })


# --- Static File Serving ---

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
app.mount("/reports", StaticFiles(directory=str(BASE_DIR / "reports"), html=True), name="reports")


@app.get("/")
def root():
    return {"message": "PW Dashboards", "reports": "/reports/", "api": "/api/tables"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)