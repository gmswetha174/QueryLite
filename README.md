# QueryLite

QueryLite is a production-ready agentic Text-to-SQL app built with Streamlit, LangGraph, SQLite, and Groq.
It reads `products.csv`, creates `querylite.db` (only when the CSV changes), infers column types, routes each question through a strict three-node agentic workflow, and never executes SQL that has not been explicitly approved by the user.

## Features

- **CSV freshness check** — the SQLite database is rebuilt only when the CSV file changes (mtime + size fingerprint); schema extraction is skipped on unchanged reruns
- **Dynamic schema handling** — zero hardcoded table, column, category, or chart names; all identifiers are derived from the live database at runtime
- **Three-node LangGraph workflow**
  - **Node 1 — Classifier**: fail-closed relevance check; any malformed, partial, or ambiguous model output defaults to rejection; user question is wrapped in `<USER_QUESTION>` delimiters to neutralise prompt-injection attempts
  - **Node 2 — SQL Generator**: produces one retrieval-only SQLite query using only schema-derived identifiers; supports case-insensitive text filtering and decimal-safe numeric comparisons; the retry prompt tightens on each regeneration attempt
  - **Node 3 — Result Handler**: executes only approved, validated SQL; returns rows as a DataFrame; surfaces clear execution errors to the user
- **Human-in-the-loop SQL approval** — generated SQL is always shown before execution; the user must explicitly approve
- **Bounded regeneration** — regeneration is capped at `MAX_REGENERATIONS`; after the cap the user must revise the question or start over
- **Deep SQL validation** (`normalize_and_validate_sql`):
  - Normalises SQL to a single line and strips markdown and stray text
  - Blocks unsafe keywords case-insensitively: ALTER, CREATE, DELETE, DROP, EXEC, INSERT, INTO, OFFSET, PRAGMA, TRUNCATE, UPDATE, VACUUM, and others
  - Rejects multiple statements
  - Enforces `SELECT` or `WITH … SELECT` only
  - Extracts referenced tables (including CTE-aware nested references) and rejects anything outside the detected schema
  - Validates referenced columns against the live schema via SQLite's `EXPLAIN QUERY PLAN`, with distinct error messages for unknown tables and unknown columns
  - Strict `LIMIT` enforcement — see below
- **LIMIT policy**
  - If the user did not request a row count, the validator enforces exactly `DEFAULT_LIMIT` (any LLM-supplied LIMIT is replaced)
  - If the user explicitly requested a count (e.g. *top 5*, *show 10*), the SQL must carry that exact `LIMIT`; a mismatch is rejected
  - If the question contains conflicting row-count instructions, the system fails safely before any SQL is generated
- **Deterministic summaries** — row count, column names, numeric min/max/mean, and top categorical values are computed directly from the returned DataFrame with no LLM dependency
- **Conservative visualisation** — chart types are data-driven: Histogram for numeric columns; Scatter for two numeric columns; Bar and Line only when both a category column and a numeric column are present; visualisation disables itself cleanly for empty results or results with no numeric columns

## Workflow

```
User question
     │
     ▼
[Node 1: Classifier]  ── rejected ──▶  warning shown, workflow ends
     │ allowed
     ▼
[Node 2: SQL Generator]  ◀── Regenerate (up to MAX_REGENERATIONS)
     │ SQL shown to user
     ▼
[User approves SQL?]  ── no ──▶  regenerate or start over
     │ yes
     ▼
[Node 3: Result Handler]  ── execution error ──▶  error shown, no result
     │ rows returned
     ▼
[Summarize | Visualize]
```

## Project Structure

- `main.py` — Full Streamlit app, LangGraph workflow, and all helpers
- `test_main.py` — Focused unit tests for safety-critical paths
- `requirements.txt` — Python dependencies
- `products.csv` — Input data source
- `.env` — Environment variables (not committed)
- `querylite.db` — Auto-created SQLite database (rebuilt only on CSV change)

## Environment Variables

Set these in `.env`:

```env
GROQ_API_KEY=your_key_here
GROQ_MODEL=your_model_here
MAX_TOKENS=300
TEMPERATURE=0.1
```

## Setup (Use existing `.quel` environment)

### Windows PowerShell

```powershell
.\.quel\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run main.py
```

### Windows CMD

```cmd
.quel\Scripts\activate.bat
pip install -r requirements.txt
streamlit run main.py
```

## Running Tests

```powershell
python -m pytest test_main.py -v
# or
python -m unittest test_main.py -v
```

## Example Questions

- Show top 5 products by sales
- Which category has the highest revenue?
- List products with a rating above 4.5

## Result Actions

- **Summarize** — deterministic summary (row count, numeric stats, top text values) computed from the returned DataFrame; no LLM call
- **Visualize** — interactive chart via hvPlot/Bokeh; chart type is chosen from what the data supports; no hardcoded dataset mappings

## Safety Guarantees

| Scenario | Behaviour |
|---|---|
| LLM returns malformed classifier JSON | Denied by default |
| LLM returns extra JSON keys | Denied by default |
| SQL references a table not in the schema | Rejected before execution |
| SQL references a column not in the schema | Rejected before execution |
| SQL contains DDL / DML keywords | Rejected before execution |
| SQL contains multiple statements | Rejected before execution |
| SQL has wrong LIMIT (no user count) | Corrected to DEFAULT_LIMIT |
| SQL has wrong LIMIT (user count specified) | Rejected before execution |
| Question has conflicting row counts | Rejected before SQL generation |
| Regeneration cap reached | Button disabled; user must revise or start over |
| Result is empty | Summary and visualisation handle cleanly |

## Notes

- The app never hardcodes schema, table, or column names.
- The SQLite table name is derived from the CSV file name.
- Only read-only retrieval queries are executed.
- OFFSET is blocked; all results start from row 0.

- SQL must be approved before execution.
- If a query returns no rows, the app shows `No data found`.
