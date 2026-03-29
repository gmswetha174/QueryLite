import csv
import re
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_LIMIT = 25

BLOCKED_KEYWORDS = frozenset({
    "alter", "analyze", "attach", "begin", "commit", "create", "delete", "detach",
    "drop", "insert", "into", "load_extension", "offset", "pragma", "replace",
    "reindex", "release", "rollback", "savepoint", "truncate", "update", "vacuum",
})


# ── Identifier helpers ────────────────────────────────────────────────────────

def sanitize_identifier(name: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    clean = re.sub(r"_+", "_", clean).strip("_") or "col"
    if clean[0].isdigit():
        clean = f"col_{clean}"
    return clean.lower()


def quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


# ── CSV → SQLite ──────────────────────────────────────────────────────────────

def load_csv_and_create_db(csv_path: Path, db_path: Path) -> str:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    table_name = sanitize_identifier(csv_path.stem)

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        raw_headers = list(reader.fieldnames or [])

    if not raw_headers:
        raise ValueError("CSV has no headers.")

    seen: dict[str, int] = {}
    safe_headers: list[str] = []
    for h in raw_headers:
        name = sanitize_identifier(h)
        count = seen.get(name, 0)
        seen[name] = count + 1
        safe_headers.append(name if count == 0 else f"{name}_{count + 1}")

    from llm import infer_column_types
    inferred_types_map = infer_column_types(raw_headers, rows)
    inferred_types = [inferred_types_map.get(h, "TEXT") for h in raw_headers]

    with closing(sqlite3.connect(db_path)) as conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {quote_identifier(table_name)}")
        col_defs = ", ".join(
            f"{quote_identifier(c)} {t}" for c, t in zip(safe_headers, inferred_types)
        )
        cur.execute(f"CREATE TABLE {quote_identifier(table_name)} ({col_defs})")
        placeholders = ", ".join(["?"] * len(safe_headers))
        insert_sql = (
            f"INSERT INTO {quote_identifier(table_name)} "
            f"({', '.join(quote_identifier(c) for c in safe_headers)}) "
            f"VALUES ({placeholders})"
        )
        records: list[tuple[Any, ...]] = []
        for row in rows:
            record: list[Any] = []
            for raw_col, col_type in zip(raw_headers, inferred_types):
                val = row.get(raw_col, "")
                if val is None or str(val).strip() == "":
                    record.append(None)
                elif col_type == "INTEGER":
                    try:
                        record.append(int(val))
                    except ValueError:
                        record.append(None)
                elif col_type == "REAL":
                    try:
                        record.append(float(val))
                    except ValueError:
                        record.append(None)
                else:
                    record.append(str(val))
            records.append(tuple(record))
        cur.executemany(insert_sql, records)
        conn.commit()

    return table_name


# ── Schema extraction ─────────────────────────────────────────────────────────

def extract_schema(db_path: Path) -> tuple[str, dict[str, dict[str, str]]]:
    lines: list[str] = []
    metadata: dict[str, dict[str, str]] = {}
    with closing(sqlite3.connect(db_path)) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        for (table,) in cur.fetchall():
            lines.append(f"Table: {table}")
            cur.execute(f"PRAGMA table_info({quote_identifier(table)})")
            cols = cur.fetchall()
            metadata[table] = {str(col[1]): str(col[2] or "TEXT") for col in cols}
            for col in cols:
                lines.append(f"  - {col[1]} ({col[2] or 'TEXT'})")
            lines.append("")
    return "\n".join(lines).strip(), metadata


def extract_allowed_tables(schema_text: str) -> list[str]:
    return re.findall(r"^Table:\s*(\S+)", schema_text, re.MULTILINE)


def get_csv_signature(csv_path: Path) -> tuple[int, int]:
    stat = csv_path.stat()
    return stat.st_mtime_ns, stat.st_size


# ── SQL validation ────────────────────────────────────────────────────────────

def normalize_and_validate_sql(
    sql: str,
    db_path: Path,
    schema_metadata: dict[str, dict[str, str]],
) -> str:
    """Enforce retrieval-only safety and ensure a LIMIT clause is present."""
    if not sql:
        raise ValueError("LLM did not return SQL.")

    cleaned = re.sub(r"/\*.*?\*/", " ", sql.strip(), flags=re.DOTALL)
    cleaned = re.sub(r"--.*?$", " ", cleaned, flags=re.MULTILINE)

    start = re.search(r"\b(select|with)\b", cleaned, re.IGNORECASE)
    if not start:
        raise ValueError("Only read-only SELECT queries are allowed.")

    one_line = " ".join(cleaned[start.start():].split()).rstrip(";")
    lowered = one_line.lower()

    for kw in BLOCKED_KEYWORDS:
        if re.search(rf"\b{kw}\b", lowered):
            raise ValueError(f"Blocked SQL keyword detected: {kw.upper()}")

    if not (lowered.startswith("select ") or lowered.startswith("with ")):
        raise ValueError("Only read-only SELECT queries are allowed.")

    if ";" in one_line:
        raise ValueError("Multiple SQL statements are not allowed.")

    ctes = {
        m.lower()
        for m in re.findall(r"\b(?:with|,)\s*([a-zA-Z_]\w*)\s+as\s*\(", one_line, re.IGNORECASE)
    }
    referenced = {
        m.strip('"').lower()
        for m in re.findall(r"\b(?:from|join)\s+([\w\"]+)", one_line, re.IGNORECASE)
        if m.strip('"').lower() not in ctes
    }
    allowed = set(schema_metadata)
    if not referenced:
        raise ValueError("Query must reference at least one detected table.")
    unknown = referenced - allowed
    if unknown:
        raise ValueError(f"Query references unknown tables: {', '.join(sorted(unknown))}")

    if not re.search(r"\blimit\s+\d+\b", lowered):
        one_line = f"{one_line} LIMIT {DEFAULT_LIMIT}"

    try:
        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute(f"EXPLAIN QUERY PLAN {one_line}")
    except sqlite3.OperationalError as exc:
        msg = str(exc).lower()
        if "no such column" in msg:
            raise ValueError(f"SQL references an unknown column: {exc}") from exc
        if "no such table" in msg:
            raise ValueError(f"SQL references an unknown table: {exc}") from exc
        raise ValueError(f"SQL validation failed: {exc}") from exc
    except sqlite3.Error as exc:
        raise ValueError(f"SQL validation failed: {exc}") from exc

    return one_line + ";"


# ── Query execution ───────────────────────────────────────────────────────────

def execute_query(db_path: Path, sql: str) -> pd.DataFrame:
    if not sql or not sql.strip():
        return pd.DataFrame()
    with closing(sqlite3.connect(db_path)) as conn:
        try:
            result = pd.read_sql_query(sql, conn)
            return result
        except sqlite3.Error as exc:
            raise RuntimeError(f"Database query failed:\n{str(exc)}\n\nSQL was:\n{sql}") from exc
