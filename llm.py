import importlib
import json
import os
import re
from typing import Any

from prompts import (
    CHART_SYSTEM,
    CHART_TEMPLATE,
    CLASSIFIER_SYSTEM,
    CLASSIFIER_TEMPLATE,
    SQL_SYSTEM,
    SQL_TEMPLATE,
    SUMMARY_SYSTEM,
    SUMMARY_TEMPLATE,
    TYPE_SYSTEM,
    TYPE_TEMPLATE,
)

_SUMMARY_PREVIEW_LIMIT = 20


def _clean_output(content: str) -> str:
    cleaned = (content or "").strip()
    cleaned = re.sub(r"^```(?:json|sql)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {'"', "'"}:
        cleaned = cleaned[1:-1].strip()
    return cleaned


def _parse_json(raw: str) -> dict | None:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
    return None


def call_llm(system: str, user: str, *, temperature: float | None = None, max_tokens: int | None = None) -> str:
    try:
        Groq = importlib.import_module("groq").Groq
    except ImportError as exc:
        raise RuntimeError("groq is required. Install the packages in requirements.txt.") from exc
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    model = os.getenv("GROQ_MODEL", "").strip()
    if not api_key:
        raise RuntimeError("Missing GROQ_API_KEY in .env")
    if not model:
        raise RuntimeError("Missing GROQ_MODEL in .env")
    response = Groq(api_key=api_key).chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        temperature=temperature if temperature is not None else float(os.getenv("TEMPERATURE", "0.1")),
        max_tokens=max_tokens if max_tokens is not None else int(os.getenv("MAX_TOKENS", "300")),
    )
    return _clean_output(response.choices[0].message.content or "")


def classify_question(user_question: str, schema_text: str) -> dict[str, Any]:
    content = call_llm(
        CLASSIFIER_SYSTEM,
        CLASSIFIER_TEMPLATE.format(schema_text=schema_text, user_question=user_question),
        temperature=0.0,
        max_tokens=120,
    )
    parsed = _parse_json(content)
    _reject: dict[str, Any] = {"allow": False, "reason": "Only database-backed retrieval questions are allowed."}
    if (
        not isinstance(parsed, dict)
        or set(parsed.keys()) != {"allow", "reason"}
        or not isinstance(parsed.get("allow"), bool)
        or not isinstance(parsed.get("reason"), str)
    ):
        return _reject
    return {"allow": parsed["allow"], "reason": parsed["reason"].strip() or _reject["reason"]}


def generate_sql(user_question: str, schema_text: str, previous_sql: str = "") -> str:
    retry_section = (
        f"The previous SQL draft failed validation:\n{previous_sql}\nGenerate a more precise query. Do not repeat it.\n\n"
        if previous_sql else ""
    )
    draft = _clean_output(call_llm(
        SQL_SYSTEM,
        SQL_TEMPLATE.format(
            schema_text=schema_text,
            user_question=user_question,
            retry_section=retry_section,
            default_limit=int(os.getenv("DEFAULT_LIMIT", "25")),
        ),
        temperature=0.1,
        max_tokens=300,
    ))
    payload = _parse_json(draft)
    if isinstance(payload, dict):
        sql_value = payload.get("sql") or payload.get("query")
        if isinstance(sql_value, str) and sql_value.strip():
            return _clean_output(sql_value)
    inline_sql = re.search(r"\b(?:select|with)\b.*", draft, re.IGNORECASE | re.DOTALL)
    return _clean_output(inline_sql.group(0)) if inline_sql else draft


def infer_column_types(headers: list[str], rows: list[dict]) -> dict[str, str]:
    samples = "\n".join(
        f"Column '{h}': {[str(row.get(h, '')) for row in rows[:5]]}" for h in headers
    )
    result = _parse_json(
        call_llm(TYPE_SYSTEM, TYPE_TEMPLATE.format(samples=samples), temperature=0.0, max_tokens=200)
    ) or {}
    valid = {"INTEGER", "REAL", "TEXT"}
    return {
        h: (str(result.get(h, "TEXT")).upper() if str(result.get(h, "TEXT")).upper() in valid else "TEXT")
        for h in headers
    }


def recommend_chart(
    numeric_cols: list[str], category_cols: list[str], all_cols: list[str], user_question: str
) -> dict[str, str]:
    if not numeric_cols:
        return {}
    result = _parse_json(call_llm(
        CHART_SYSTEM,
        CHART_TEMPLATE.format(
            numeric_cols=numeric_cols,
            category_cols=category_cols,
            all_cols=all_cols,
            user_question=user_question,
        ),
        temperature=0.0,
        max_tokens=100,
    )) or {}
    if result.get("chart_type") not in {"Histogram", "Scatter", "Bar", "Line"}:
        return {"chart_type": "Histogram", "x": numeric_cols[0], "title": "Distribution"}
    return result


def summarize_results(user_question: str, df: Any) -> str:  # df: pd.DataFrame
    if df.empty:
        return "The query returned no rows."
    return call_llm(
        SUMMARY_SYSTEM,
        SUMMARY_TEMPLATE.format(
            user_question=user_question,
            row_count=len(df),
            preview_limit=_SUMMARY_PREVIEW_LIMIT,
            data_preview=df.head(_SUMMARY_PREVIEW_LIMIT).to_csv(index=False),
        ),
        temperature=0.3,
        max_tokens=300,
    )
