import importlib
import json
import os
import re
from typing import Any

from db import DEFAULT_LIMIT
from prompts import (
    CLASSIFIER_SYSTEM,
    CLASSIFIER_TEMPLATE,
    SQL_SYSTEM,
    SQL_TEMPLATE,
    SUMMARY_SYSTEM,
    SUMMARY_TEMPLATE,
)

_SUMMARY_PREVIEW_LIMIT = 20


def _clean_output(content: str) -> str:
    cleaned = (content or "").strip()
    cleaned = re.sub(r"^```(?:json|sql)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    # Some models wrap the whole answer in quotes; unwrap once so downstream parsing works.
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {'"', "'"}:
        cleaned = cleaned[1:-1].strip()
    return cleaned


def _get_client() -> tuple[Any, str, int, float]:
    try:
        Groq = importlib.import_module("groq").Groq
    except ImportError as exc:
        raise RuntimeError(
            "groq is required. Install the packages in requirements.txt."
        ) from exc
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    model = os.getenv("GROQ_MODEL", "").strip()
    max_tokens = int(os.getenv("MAX_TOKENS", "300"))
    temperature = float(os.getenv("TEMPERATURE", "0.1"))
    if not api_key:
        raise RuntimeError("Missing GROQ_API_KEY in .env")
    if not model:
        raise RuntimeError("Missing GROQ_MODEL in .env")
    return Groq(api_key=api_key), model, max_tokens, temperature


def call_llm(
    system: str,
    user: str,
    *,
    temperature: float | None = None,
    max_tokens: int | None = None,
) -> str:
    client, model, default_max, default_temp = _get_client()
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=default_temp if temperature is None else temperature,
        max_tokens=default_max if max_tokens is None else max_tokens,
    )
    return _clean_output(response.choices[0].message.content or "")


def classify_question(user_question: str, schema_text: str) -> dict[str, Any]:
    prompt = CLASSIFIER_TEMPLATE.format(schema_text=schema_text, user_question=user_question)
    content = call_llm(CLASSIFIER_SYSTEM, prompt, temperature=0.0, max_tokens=120)

    parsed: dict[str, Any] | None = None
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

    _reject: dict[str, Any] = {
        "allow": False,
        "reason": "Only database-backed retrieval questions are allowed.",
    }
    if (
        not isinstance(parsed, dict)
        or set(parsed.keys()) != {"allow", "reason"}
        or not isinstance(parsed.get("allow"), bool)
        or not isinstance(parsed.get("reason"), str)
    ):
        return _reject

    return {
        "allow": parsed["allow"],
        "reason": parsed["reason"].strip() or _reject["reason"],
    }


def generate_sql(user_question: str, schema_text: str, previous_sql: str = "") -> str:
    retry_section = (
        f"The previous SQL draft failed validation:\n{previous_sql}\n"
        "Generate a more precise query. Do not repeat it.\n\n"
        if previous_sql
        else ""
    )
    prompt = SQL_TEMPLATE.format(
        schema_text=schema_text,
        user_question=user_question,
        retry_section=retry_section,
        default_limit=DEFAULT_LIMIT,
    )
    draft = _clean_output(call_llm(SQL_SYSTEM, prompt, temperature=0.1, max_tokens=300))

    # Accept plain SQL, JSON-wrapped SQL, or inline backtick snippets from provider variants.
    json_match = re.search(r"\{.*\}", draft, re.DOTALL)
    if json_match:
        try:
            payload = json.loads(json_match.group(0))
            if isinstance(payload, dict):
                sql_value = payload.get("sql") or payload.get("query")
                if isinstance(sql_value, str) and sql_value.strip():
                    return _clean_output(sql_value)
        except json.JSONDecodeError:
            pass

    inline_sql = re.search(r"\b(?:select|with)\b.*", draft, re.IGNORECASE | re.DOTALL)
    if inline_sql:
        return _clean_output(inline_sql.group(0))

    return draft


def summarize_results(user_question: str, df: Any) -> str:  # df: pd.DataFrame
    if df.empty:
        return "The query returned no rows."
    preview = df.head(_SUMMARY_PREVIEW_LIMIT).to_csv(index=False)
    prompt = SUMMARY_TEMPLATE.format(
        user_question=user_question,
        row_count=len(df),
        preview_limit=_SUMMARY_PREVIEW_LIMIT,
        data_preview=preview,
    )
    return call_llm(SUMMARY_SYSTEM, prompt, temperature=0.3, max_tokens=300)
