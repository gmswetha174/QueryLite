import importlib
from pathlib import Path
from typing import cast

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from db import (
    DEFAULT_LIMIT,
    extract_allowed_tables,
    extract_schema,
    extract_schema_metadata,
    get_csv_signature,
    load_csv_and_create_db,
)
from graph import (
    DB_PATH,
    MAX_REGENERATIONS,
    WorkflowState,
    compile_workflow,
    default_workflow_state,
)

APP_DIR = Path(__file__).resolve().parent
CSV_PATH = APP_DIR / "products.csv"


# -- Workflow helpers ----------------------------------------------------------

@st.cache_resource
def _build_workflow():
    return compile_workflow()


def _run_workflow(state: WorkflowState) -> WorkflowState:
    return cast(WorkflowState, _build_workflow().invoke(state))


# -- Session-state helpers -----------------------------------------------------

def _init_session_state(schema_text: str, schema_metadata: dict) -> None:
    if "workflow_state" not in st.session_state:
        st.session_state.workflow_state = default_workflow_state(schema_text, schema_metadata)
    else:
        st.session_state.workflow_state.update({
            "schema_text": schema_text,
            "schema_metadata": schema_metadata,
            "allowed_tables": extract_allowed_tables(schema_text),
        })
    st.session_state.setdefault("question_input", "")
    st.session_state.setdefault("post_query_action", "Summarize")


def _reset_workflow(schema_text: str, schema_metadata: dict) -> None:
    st.session_state.workflow_state = default_workflow_state(schema_text, schema_metadata)
    st.session_state._clear_question_input = True
    st.session_state._reset_post_query_action = True


def _apply_deferred_resets() -> None:
    if st.session_state.pop("_clear_question_input", False):
        st.session_state.question_input = ""
    if st.session_state.pop("_reset_post_query_action", False):
        st.session_state.post_query_action = "Summarize"


# -- Visualization -------------------------------------------------------------

def _render_visualization(df: pd.DataFrame) -> None:
    try:
        importlib.import_module("hvplot.pandas")
        hv = importlib.import_module("holoviews")
        st_bokeh_module = importlib.import_module("streamlit_bokeh")
        streamlit_bokeh_fn = getattr(st_bokeh_module, "streamlit_bokeh")
    except ImportError as exc:
        raise RuntimeError(
            "hvPlot, HoloViews, and streamlit-bokeh are required for visualization. Install the packages in requirements.txt."
        ) from exc

    hv.extension("bokeh")

    if df.empty:
        st.info("No rows returned - visualization unavailable.")
        return

    numeric_cols = list(df.select_dtypes(include="number").columns)
    all_cols = list(df.columns)
    category_cols = [c for c in all_cols if c not in numeric_cols]

    chart_options: list[str] = []
    if numeric_cols:
        chart_options.append("Histogram")
    if len(numeric_cols) >= 2:
        chart_options.append("Scatter")
    if numeric_cols and category_cols:
        chart_options.extend(["Bar", "Line"])

    if not chart_options:
        st.info("No numeric columns available for visualization.")
        return

    chart_type = st.selectbox("Chart type", chart_options, key="chart_type")

    if chart_type == "Histogram":
        col = st.selectbox("Numeric column", numeric_cols, key="hist_col")
        chart = df[col].dropna().hvplot.hist(
            bins=min(30, max(10, len(df))), height=420, responsive=True, title=f"Distribution of {col}"
        )
    elif chart_type == "Scatter":
        x = st.selectbox("X-axis", numeric_cols, key="scatter_x")
        y_opts = [c for c in numeric_cols if c != x] or numeric_cols
        y = st.selectbox("Y-axis", y_opts, key="scatter_y")
        chart = df.hvplot.scatter(x=x, y=y, height=420, responsive=True, title=f"{y} vs {x}")
    elif chart_type == "Line":
        x_default = category_cols[0] if category_cols else all_cols[0]
        x = st.selectbox("X-axis", all_cols, index=all_cols.index(x_default), key="line_x")
        y = st.selectbox("Y-axis", numeric_cols, key="line_y")
        chart = df[[x, y]].dropna().hvplot.line(x=x, y=y, height=420, responsive=True, title=f"{y} by {x}")
    else:  # Bar
        x_default = category_cols[0] if category_cols else all_cols[0]
        x = st.selectbox("Category column", all_cols, index=all_cols.index(x_default), key="bar_x")
        y = st.selectbox("Value column", numeric_cols, key="bar_y")
        chart = df[[x, y]].dropna().head(DEFAULT_LIMIT).hvplot.bar(
            x=x, y=y, height=420, responsive=True, rot=45, title=f"{y} by {x}"
        )

    streamlit_bokeh_fn(hv.render(chart, backend="bokeh"), key="querylite_chart")


# -- Main app ------------------------------------------------------------------

def main() -> None:
    load_dotenv(APP_DIR / ".env")

    st.set_page_config(page_title="QueryLite", page_icon="magnifying glass", layout="wide")
    st.title("QueryLite: Agentic Text-to-SQL Workflow")
    st.caption("Classifier -> SQL draft -> approval -> retrieval -> summary or visualization")

    # -- Database setup --------------------------------------------------------
    try:
        csv_sig = get_csv_signature(CSV_PATH)
        if st.session_state.get("csv_signature") != csv_sig:
            with st.spinner("Preparing database and schema..."):
                table_name = load_csv_and_create_db(CSV_PATH, DB_PATH)
                schema_text = extract_schema(DB_PATH)
                schema_metadata = extract_schema_metadata(DB_PATH)
            st.session_state.update({
                "csv_signature": csv_sig,
                "table_name": table_name,
                "schema_text": schema_text,
                "schema_metadata": schema_metadata,
            })
            _reset_workflow(schema_text, schema_metadata)
        else:
            table_name = st.session_state["table_name"]
            schema_text = st.session_state["schema_text"]
            schema_metadata = st.session_state["schema_metadata"]
    except Exception as exc:
        st.error(f"Database setup failed: {exc}")
        st.stop()

    _init_session_state(schema_text, schema_metadata)
    _apply_deferred_resets()
    state = cast(WorkflowState, st.session_state.workflow_state)

    # -- Schema info -----------------------------------------------------------
    with st.expander("Detected Database Schema", expanded=False):
        st.code(schema_text, language="text")
    st.info(f"Loaded table: **{table_name}** | Database: {DB_PATH.name}")

    # -- Question input --------------------------------------------------------
    question = st.text_input(
        "Ask a database question",
        key="question_input",
        placeholder="e.g. Show the top 10 most expensive products",
    )

    action_col, reset_col = st.columns([1, 1])
    with action_col:
        if st.button("Classify and Draft SQL", type="primary"):
            if not question.strip():
                st.warning("Please enter a question.")
            else:
                try:
                    fresh = default_workflow_state(schema_text, schema_metadata)
                    fresh["user_question"] = question.strip()
                    with st.spinner("Classifying and drafting SQL..."):
                        st.session_state.workflow_state = _run_workflow(fresh)
                    state = cast(WorkflowState, st.session_state.workflow_state)
                except Exception as exc:
                    st.error(f"Workflow failed: {exc}")

    with reset_col:
        if st.button("Start Over"):
            _reset_workflow(schema_text, schema_metadata)
            st.rerun()

    state = cast(WorkflowState, st.session_state.workflow_state)

    # -- Classifier result -----------------------------------------------------
    if state.get("classifier_allowed") is False:
        st.warning(state.get("classifier_message") or "Only database-backed retrieval questions are allowed.")

    # -- SQL approval ----------------------------------------------------------
    if state.get("generated_sql") and not state.get("approved_sql"):
        st.subheader("Generated SQL")
        st.code(state["generated_sql"], language="sql")
        st.caption(f"Regenerations used: {state.get('regenerate_count', 0)} / {MAX_REGENERATIONS}")

        approve_col, regen_col = st.columns([1, 1])
        with approve_col:
            if st.button("Approve SQL", type="primary"):
                try:
                    with st.spinner("Executing approved SQL..."):
                        # Prepare invocation state with explicit field set
                        invoke_state = {**state, "approved_sql": True}
                        st.session_state.workflow_state = _run_workflow(cast(WorkflowState, invoke_state))
                    state = cast(WorkflowState, st.session_state.workflow_state)
                    # Debug: show what we got back
                    if state.get("result_columns") is None:
                        st.warning(f"⚠️ Query ran but returned no result columns. Summary: {state.get('summary_text', 'N/A')}")
                except Exception as exc:
                    st.error(f"Execution failed: {exc}")

        regen_disabled = state.get("regenerate_count", 0) >= MAX_REGENERATIONS
        with regen_col:
            if st.button("Regenerate SQL", disabled=regen_disabled):
                try:
                    with st.spinner("Regenerating SQL..."):
                        st.session_state.workflow_state = _run_workflow(
                            cast(WorkflowState, {
                                **state,
                                "previous_sql": state.get("generated_sql", ""),
                                "generated_sql": "",
                                "approved_sql": False,
                                "result_rows": [],
                                "result_columns": None,
                                "summary_text": "",
                                "regenerate_count": state.get("regenerate_count", 0) + 1,
                            })
                        )
                    state = cast(WorkflowState, st.session_state.workflow_state)
                except Exception as exc:
                    st.error(f"Regeneration failed: {exc}")

        if regen_disabled:
            st.info("Regeneration limit reached. Please revise your question or start over.")

    # -- Results ---------------------------------------------------------------
    if state.get("approved_sql") and state.get("result_columns") is not None:
        result_df = pd.DataFrame(
            state.get("result_rows", []),
            columns=state.get("result_columns", []),
        )
        st.subheader("Retrieved Rows")
        if result_df.empty:
            st.warning("No data found.")
        else:
            st.dataframe(result_df, width="stretch")

        st.subheader("Next Step")
        action = st.radio(
            "Choose how to inspect the data",
            ("Summarize", "Visualize"),
            horizontal=True,
            key="post_query_action",
        )
        if action == "Summarize":
            st.text(state.get("summary_text", "No summary available."))
        else:
            _render_visualization(result_df)


if __name__ == "__main__":
    main()
