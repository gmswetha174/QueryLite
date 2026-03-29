import importlib
from pathlib import Path
from typing import Any, TypedDict, cast

from db import execute_query, extract_allowed_tables, normalize_and_validate_sql
from llm import classify_question, generate_sql, summarize_results

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "querylite.db"
MAX_REGENERATIONS = 2


class WorkflowState(TypedDict, total=False):
    user_question: str
    schema_text: str
    schema_metadata: dict[str, dict[str, str]]
    allowed_tables: list[str]
    classifier_allowed: bool
    classifier_message: str
    generated_sql: str
    previous_sql: str
    regenerate_count: int
    approved_sql: bool
    result_rows: list[dict[str, Any]]
    result_columns: list[str] | None
    summary_text: str


def default_workflow_state(
    schema_text: str,
    schema_metadata: dict[str, dict[str, str]],
) -> WorkflowState:
    return {
        "user_question": "",
        "schema_text": schema_text,
        "schema_metadata": schema_metadata,
        "allowed_tables": extract_allowed_tables(schema_text),
        "classifier_allowed": None,
        "classifier_message": "",
        "generated_sql": "",
        "previous_sql": "",
        "regenerate_count": 0,
        "approved_sql": False,
        "result_rows": [],
        "result_columns": None,
        "summary_text": "",
    }


# ── Graph nodes ───────────────────────────────────────────────────────────────

def _classifier_node(state: WorkflowState) -> WorkflowState:
    if state.get("classifier_allowed") is not None:
        return {}
    result = classify_question(state["user_question"], state["schema_text"])
    update = {
        "classifier_allowed": result["allow"],
        "classifier_message": "" if result["allow"] else result["reason"],
    }
    return update


def _text_to_sql_node(state: WorkflowState) -> WorkflowState:
    if state.get("generated_sql") or state.get("approved_sql"):
        return {}
    raw_sql = generate_sql(
        state["user_question"],
        state["schema_text"],
        state.get("previous_sql", ""),
    )
    # Reject any placeholder or malformed outputs
    if not raw_sql or raw_sql.strip() in {"xyz", "placeholder", "..."} or len(raw_sql.strip()) < 10:
        raise ValueError(f"LLM returned invalid SQL: {raw_sql!r}")
    safe_sql = normalize_and_validate_sql(raw_sql, DB_PATH, state["schema_metadata"])
    return {"generated_sql": safe_sql}


def _result_handler_node(state: WorkflowState) -> WorkflowState:
    if not state.get("generated_sql"):
        return {"result_rows": [], "result_columns": None, "summary_text": "No SQL to execute."}
    try:
        df = execute_query(DB_PATH, state["generated_sql"])
        return {
            "result_rows": cast(list[dict[str, Any]], df.to_dict(orient="records")),
            "result_columns": list(df.columns),
            "summary_text": summarize_results(state["user_question"], df),
        }
    except Exception as exc:
        return {
            "result_rows": [],
            "result_columns": None,
            "summary_text": f"Query execution error: {str(exc)}",
        }



# ── Workflow builder ──────────────────────────────────────────────────────────

def compile_workflow() -> Any:
    try:
        graph_module = importlib.import_module("langgraph.graph")
    except ImportError as exc:
        raise RuntimeError(
            "LangGraph is required. Install the packages in requirements.txt."
        ) from exc

    END = graph_module.END
    StateGraph = graph_module.StateGraph

    builder = StateGraph(WorkflowState)
    builder.add_node("classifier", _classifier_node)
    builder.add_node("text_to_sql", _text_to_sql_node)
    builder.add_node("result_handler", _result_handler_node)
    builder.set_entry_point("classifier")
    builder.add_conditional_edges(
        "classifier",
        lambda s: "text_to_sql" if s.get("classifier_allowed") else "__end__",
    )
    builder.add_conditional_edges(
        "text_to_sql",
        lambda s: "result_handler" if (s.get("approved_sql") and s.get("generated_sql")) else "__end__",
    )
    builder.add_edge("result_handler", END)
    return builder.compile()
