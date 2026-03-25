import csv
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from groq import Groq


# Constants and paths
APP_DIR = Path(__file__).resolve().parent
CSV_PATH = APP_DIR / "products.csv"
DB_PATH = APP_DIR / "querylite.db"
DEFAULT_LIMIT = 100
BLOCKED_KEYWORDS = {
    "drop",
    "delete",
    "update",
    "alter",
    "truncate",
    "insert",
    "replace",
    "attach",
    "detach",
    "pragma",
}


def sanitize_identifier(name: str) -> str:
    """Convert an arbitrary text into a safe SQLite identifier."""
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    clean = re.sub(r"_+", "_", clean).strip("_")
    if not clean:
        clean = "col"
    if clean[0].isdigit():
        clean = f"col_{clean}"
    return clean.lower()


def quote_identifier(identifier: str) -> str:
    """Quote an SQLite identifier safely."""
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def infer_sqlite_type(values: list[str]) -> str:
    """Infer INTEGER, REAL, or TEXT from observed CSV values."""
    non_empty = [v.strip() for v in values if v is not None and str(v).strip() != ""]
    if not non_empty:
        return "TEXT"

    int_ok = True
    real_ok = True

    for value in non_empty:
        try:
            int(value)
        except ValueError:
            int_ok = False

        try:
            float(value)
        except ValueError:
            real_ok = False

    if int_ok:
        return "INTEGER"
    if real_ok:
        return "REAL"
    return "TEXT"


def load_csv_and_create_db(csv_path: Path, db_path: Path) -> str:
    """Create a SQLite table dynamically from CSV data and inferred schema."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    table_name = sanitize_identifier(csv_path.stem)

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not reader.fieldnames:
        raise ValueError("CSV has no headers.")

    raw_headers = reader.fieldnames
    safe_headers = [sanitize_identifier(h) for h in raw_headers]

    # Keep headers unique after sanitization.
    seen: dict[str, int] = {}
    unique_headers = []
    for name in safe_headers:
        if name not in seen:
            seen[name] = 1
            unique_headers.append(name)
        else:
            seen[name] += 1
            unique_headers.append(f"{name}_{seen[name]}")

    columns_data: list[list[str]] = [[] for _ in raw_headers]
    for row in rows:
        for idx, col in enumerate(raw_headers):
            columns_data[idx].append(row.get(col, ""))

    inferred_types = [infer_sqlite_type(col_values) for col_values in columns_data]

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute(f"DROP TABLE IF EXISTS {quote_identifier(table_name)}")

        column_defs = ", ".join(
            f"{quote_identifier(col)} {col_type}"
            for col, col_type in zip(unique_headers, inferred_types)
        )
        cur.execute(
            f"CREATE TABLE {quote_identifier(table_name)} ({column_defs})"
        )

        placeholders = ", ".join(["?"] * len(unique_headers))
        insert_sql = (
            f"INSERT INTO {quote_identifier(table_name)} "
            f"({', '.join(quote_identifier(c) for c in unique_headers)}) "
            f"VALUES ({placeholders})"
        )

        records = []
        for row in rows:
            record: list[Any] = []
            for raw_col, col_type in zip(raw_headers, inferred_types):
                raw_val = row.get(raw_col, "")
                if raw_val is None or str(raw_val).strip() == "":
                    record.append(None)
                elif col_type == "INTEGER":
                    try:
                        record.append(int(raw_val))
                    except ValueError:
                        record.append(None)
                elif col_type == "REAL":
                    try:
                        record.append(float(raw_val))
                    except ValueError:
                        record.append(None)
                else:
                    record.append(str(raw_val))
            records.append(tuple(record))

        cur.executemany(insert_sql, records)
        conn.commit()

    return table_name


def extract_schema(db_path: Path) -> str:
    """Extract schema dynamically using sqlite_master and PRAGMA table_info."""
    schema_lines: list[str] = []

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name;
            """
        )
        tables = [r[0] for r in cur.fetchall()]

        for table in tables:
            schema_lines.append(f"Table: {table}")
            cur.execute(f"PRAGMA table_info({quote_identifier(table)})")
            columns = cur.fetchall()
            for col in columns:
                # PRAGMA result: cid, name, type, notnull, dflt_value, pk
                schema_lines.append(f"  - {col[1]} ({col[2] or 'TEXT'})")
            schema_lines.append("")

    return "\n".join(schema_lines).strip()


def build_prompt(user_question: str, schema_text: str) -> str:
    """Construct a strict prompt for generating SQLite SQL.

    All table names, column names, and LIMIT values used in the examples are
    derived dynamically from the injected schema — nothing is hardcoded.
    """
    # --- Parse table name from schema ---
    table_match = re.search(r"^Table:\s*(\S+)", schema_text, re.MULTILINE)
    tbl = table_match.group(1) if table_match else "data"

    # --- Parse columns and their types from schema ---
    col_matches = re.findall(r"^\s+-\s+(\w+)\s+\((\w+)\)", schema_text, re.MULTILINE)
    text_cols = [c for c, t in col_matches if t == "TEXT"]
    num_cols  = [c for c, t in col_matches if t in ("REAL", "INTEGER")]

    # Semantic hint-based picker: returns first column whose name contains a hint
    def _pick(pool: list[str], *hints: str) -> str:
        for hint in hints:
            for col in pool:
                if hint in col.lower():
                    return col
        return pool[0] if pool else "col"

    col_label    = _pick(text_cols, "title", "name", "label")
    col_category = _pick(text_cols, "category", "type", "group", "class")
    col_badge    = _pick(text_cols, "seller", "badge", "status", "flag")
    col_rating   = _pick(num_cols,  "rating", "score", "stars", "rank")
    col_price    = _pick(num_cols,  "price", "cost", "amount", "value")
    col_sales    = _pick(num_cols,  "sold", "sales", "purchased", "quantity", "count")
    col_discount = _pick(num_cols,  "discount", "off", "reduction", "percent")

    return f"""   
You are an expert SQLite query generator .

Database schema:
{schema_text}

User question:
{user_question}

Rules:
1. Return only one valid SQLite SQL query.
2. Use only table and column names from the schema.
3. Never use destructive operations: DROP, DELETE, UPDATE, ALTER, INSERT, TRUNCATE, REPLACE.
4. Query must be read-only (SELECT or WITH ... SELECT).
5. Always include LIMIT {DEFAULT_LIMIT} or lower.
6. If question implies top/best/highest, use ORDER BY with DESC appropriately.
7. Return plain SQL only. No markdown, no explanation.

Text filtering rules (MANDATORY):
- Always wrap text comparisons with LOWER() to make them case-insensitive.
- Never use exact equality (=) for TEXT columns such as {col_category}, {col_label},
  {col_badge}, or any other TEXT field.
- Always use LIKE with wildcard patterns: LOWER(column) LIKE '%value%'
  This automatically handles singular/plural and mixed-case variations.
  Example: LOWER({col_category}) LIKE '%laptop%' matches "Laptop", "laptops", "LAPTOPS".
- Numeric columns must use normal numeric operators (>, <, =, >=, <=).

Examples:
Question: Show top items by {col_rating}
SQL: SELECT {col_label}, {col_rating} FROM {tbl} ORDER BY {col_rating} DESC LIMIT {DEFAULT_LIMIT};

Question: Which {col_category} has highest revenue?
SQL: SELECT {col_category}, SUM(COALESCE({col_price}, 0) * COALESCE({col_sales}, 0)) AS revenue FROM {tbl} GROUP BY {col_category} ORDER BY revenue DESC LIMIT {DEFAULT_LIMIT};

Question: List items where {col_category} contains laptop and {col_rating} above 4.5
SQL: SELECT * FROM {tbl} WHERE LOWER({col_category}) LIKE '%laptop%' AND {col_rating} > 4.5 LIMIT {DEFAULT_LIMIT};

Question: Show items where {col_badge} contains best seller and {col_discount} over 10
SQL: SELECT * FROM {tbl} WHERE LOWER({col_badge}) LIKE '%best seller%' AND {col_discount} > 10 LIMIT {DEFAULT_LIMIT};

Question: Find items where {col_category} contains camera
SQL: SELECT * FROM {tbl} WHERE LOWER({col_category}) LIKE '%camera%' LIMIT {DEFAULT_LIMIT};
""".strip()


def get_sql_from_llm(user_question: str, schema_text: str) -> str:
    """Call Groq model to transform NL question to SQL."""
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    model = os.getenv("GROQ_MODEL", "").strip()
    max_tokens = int(os.getenv("MAX_TOKENS", "300"))
    temperature = float(os.getenv("TEMPERATURE", "0.1"))

    if not api_key:
        raise RuntimeError("Missing GROQ_API_KEY in .env")
    if not model:
        raise RuntimeError("Missing GROQ_MODEL in .env")

    client = Groq(api_key=api_key)
    prompt = build_prompt(user_question, schema_text)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "Generate only safe SQLite SQL."},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )

    content = (response.choices[0].message.content or "").strip()

    # Remove markdown fences if the model returns them.
    content = re.sub(r"^```(?:sql)?\s*", "", content, flags=re.IGNORECASE)
    content = re.sub(r"\s*```$", "", content)

    return content.strip()


def normalize_and_validate_sql(sql: str) -> str:
    """Enforce safety constraints and LIMIT policy for generated SQL."""
    if not sql:
        raise ValueError("LLM did not return SQL.")

    one_line = " ".join(sql.strip().split())
    one_line = one_line.rstrip(";")

    lowered = one_line.lower()

    # Block disallowed operations.
    for keyword in BLOCKED_KEYWORDS:
        if re.search(rf"\b{keyword}\b", lowered):
            raise ValueError(f"Blocked SQL keyword detected: {keyword.upper()}")

    # Only allow SELECT or CTE queries.
    if not (lowered.startswith("select ") or lowered.startswith("with ")):
        raise ValueError("Only read-only SELECT queries are allowed.")

    # Prevent stacked statements.
    if ";" in one_line:
        raise ValueError("Multiple SQL statements are not allowed.")

    # Ensure a row limit exists.
    if not re.search(r"\blimit\s+\d+\b", lowered):
        one_line = f"{one_line} LIMIT {DEFAULT_LIMIT}"

    return one_line + ";"


def execute_query(db_path: Path, sql: str) -> pd.DataFrame:
    """Execute SQL and return result as a DataFrame."""
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(sql, conn)
    return df


def main() -> None:
    load_dotenv(APP_DIR / ".env")

    st.set_page_config(page_title="QueryLite", page_icon="🔎", layout="wide")
    st.title("QueryLite: Text-to-SQL Chatbot")
    st.caption("Ask plain-English questions over your CSV-powered SQLite database.")

    try:
        table_name = load_csv_and_create_db(CSV_PATH, DB_PATH)
        schema_text = extract_schema(DB_PATH)
    except Exception as exc:
        st.error(f"Database setup failed: {exc}")
        st.stop()

    with st.expander("Detected Database Schema", expanded=False):
        st.code(schema_text, language="text")

    st.info(f"Loaded table: {table_name} | Database: {DB_PATH.name}")

    question = st.text_input(
        "Ask your question",
        placeholder="Example: Show top 5 products by sales",
    )

    if st.button("Run Query", type="primary"):
        if not question.strip():
            st.warning("Please enter a question.")
            st.stop()

        try:
            with st.spinner("Generating SQL with Groq..."):
                generated_sql = get_sql_from_llm(question, schema_text)
                safe_sql = normalize_and_validate_sql(generated_sql)

            st.subheader("Generated SQL")
            st.code(safe_sql, language="sql")

            with st.spinner("Executing query..."):
                result_df = execute_query(DB_PATH, safe_sql)

            st.subheader("Results")
            if result_df.empty:
                st.warning("No data found")
            else:
                st.dataframe(result_df, use_container_width=True)

        except Exception as exc:
            st.error(f"Query failed: {exc}")


if __name__ == "__main__":
    main()
