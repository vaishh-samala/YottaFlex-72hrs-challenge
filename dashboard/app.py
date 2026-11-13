"""DataFlow monitoring & analytics dashboard (Streamlit).

    streamlit run dashboard/app.py

Every figure is queried live from the DuckDB warehouse through the SQL files in sql/.
The dashboard opens short-lived read-only connections and never writes to the database;
the "Run pipeline" button launches the real pipeline in a separate process.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.queries import fetch_many  # noqa: E402
from src.config import PROJECT_ROOT, Settings  # noqa: E402
from src.monitoring.trigger import is_running, trigger_pipeline  # noqa: E402
from src.warehouse.database import DatabaseBusyError  # noqa: E402

st.set_page_config(page_title="DataFlow", page_icon="◆", layout="wide")

SETTINGS = Settings()
DB_PATH = SETTINGS.db_path
ACCENT, GOOD, BAD, WARN, MUTED = "#38BDF8", "#34D399", "#F87171", "#FBBF24", "#8B95AD"
STATUS_COLORS = {"SUCCESS": GOOD, "FAILED": BAD, "SKIPPED": MUTED, "RUNNING": WARN}

st.markdown(
    """
    <style>
      .block-container {padding-top: 1.6rem; max-width: 1400px;}
      [data-testid="stMetric"] {background: #131A2E; border: 1px solid #1F2942; border-radius: 10px;
                                padding: 14px 16px;}
      [data-testid="stMetricLabel"] p {color: #8B95AD; font-size: 0.78rem; text-transform: uppercase;
                                       letter-spacing: .04em;}
      [data-testid="stMetricValue"] {font-size: 1.55rem;}
      .df-pill {display:inline-block; padding:2px 10px; border-radius:999px; font-size:.8rem; font-weight:600;}
      .df-sub {color:#8B95AD; font-size:.9rem; margin-top:-.6rem; margin-bottom:1rem;}
      h1, h2, h3 {letter-spacing: -.01em;}
    </style>
    """,
    unsafe_allow_html=True,
)


def themed(chart: alt.Chart, height: int = 280) -> alt.Chart:
    return (chart.properties(height=height, background="transparent")
            .configure_axis(labelColor=MUTED, titleColor=MUTED, gridColor="#1F2942", domainColor="#1F2942")
            .configure_legend(labelColor="#E5E9F2", titleColor=MUTED)
            .configure_view(strokeWidth=0))


def pill(status: str | None) -> str:
    status = status or "—"
    color = STATUS_COLORS.get(status, MUTED)
    return f'<span class="df-pill" style="background:{color}22;color:{color};border:1px solid {color}66">{status}</span>'


@st.cache_data(ttl=15, show_spinner=False)
def load(items: tuple[tuple[str, str], ...], db_mtime: float) -> dict[str, pd.DataFrame]:
    return fetch_many(DB_PATH, list(items))


def data(*items: tuple[str, str]) -> dict[str, pd.DataFrame] | None:
    """Query the warehouse; render a real (not fake) empty/busy state when unavailable."""
    if not DB_PATH.exists():
        st.info("No warehouse yet. Run the pipeline (Pipeline Runs page or `python -m src.pipeline`).")
        return None
    try:
        return load(tuple(items), DB_PATH.stat().st_mtime)
    except DatabaseBusyError:
        st.warning("The pipeline is writing to the warehouse right now. Refresh in a few seconds.")
        return None


def fmt_int(v) -> str:
    return "0" if pd.isna(v) else f"{int(v):,}"


def fmt_money(v) -> str:
    return "0.00" if pd.isna(v) else f"{float(v):,.2f}"


# ===================================================================================== pages
def page_overview() -> None:
    st.title("Pipeline Overview")
    st.markdown('<div class="df-sub">Run history and record flow, from <code>pipeline_runs</code>.</div>',
                unsafe_allow_html=True)
    d = data(("monitoring", "run_summary"), ("monitoring", "runs"), ("monitoring", "warehouse_counts"))
    if d is None:
        return
    s = d["run_summary"].iloc[0]
    runs = d["runs"]
    c = st.columns(4)
    c[0].metric("Total runs", fmt_int(s.total_runs))
    c[1].metric("Successful runs", fmt_int(s.successful_runs))
    c[2].metric("Failed runs", fmt_int(s.failed_runs))
    c[3].metric("Skipped (already processed)", fmt_int(s.skipped_runs))
    c = st.columns(4)
    c[0].metric("Records ingested", fmt_int(s.total_records_ingested))
    c[1].metric("Valid records", fmt_int(s.total_valid_records))
    c[2].metric("Rejected records", fmt_int(s.total_rejected_records))
    last = "—" if pd.isna(s.last_run_start) else pd.Timestamp(s.last_run_start).strftime("%b %d, %H:%M:%S")
    c[3].metric("Last execution (UTC)", last)
    st.markdown(f"**Latest run** `{s.last_run_id}` &nbsp; {pill(s.last_run_status)}", unsafe_allow_html=True)

    if runs.empty:
        return
    trend = runs.sort_values("start_time").copy()
    trend["run"] = trend["start_time"].dt.strftime("%m-%d %H:%M:%S")
    left, right = st.columns([3, 2])
    with left:
        st.subheader("Records per run")
        melted = trend.melt(id_vars=["run", "start_time", "run_id"], value_vars=["valid_rows", "rejected_rows"],
                            var_name="kind", value_name="rows")
        melted["kind"] = melted["kind"].map({"valid_rows": "Valid", "rejected_rows": "Rejected"})
        chart = alt.Chart(melted).mark_bar().encode(
            x=alt.X("run:N", sort=None, title=None, axis=alt.Axis(labelAngle=-40)),
            y=alt.Y("rows:Q", title="Rows"),
            color=alt.Color("kind:N", scale=alt.Scale(domain=["Valid", "Rejected"], range=[ACCENT, BAD]), title=None),
            tooltip=["run_id", "kind", "rows"])
        st.altair_chart(themed(chart), width="stretch")
    with right:
        st.subheader("Run duration")
        chart = alt.Chart(trend).mark_line(point=True, color=ACCENT).encode(
            x=alt.X("run:N", sort=None, title=None, axis=alt.Axis(labelAngle=-40)),
            y=alt.Y("duration_seconds:Q", title="Seconds"),
            color=alt.Color("execution_status:N", scale=alt.Scale(domain=list(STATUS_COLORS), range=list(STATUS_COLORS.values())),
                            title=None),
            tooltip=["run_id", "execution_status", "duration_seconds", "input_rows"])
        st.altair_chart(themed(chart), width="stretch")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Warehouse writes per run")
        w = trend.melt(id_vars=["run", "run_id"], value_vars=["facts_inserted", "facts_updated", "facts_unchanged"],
                       var_name="kind", value_name="rows")
        chart = alt.Chart(w).mark_bar().encode(
            x=alt.X("run:N", sort=None, title=None, axis=alt.Axis(labelAngle=-40)), y=alt.Y("rows:Q", title="Fact rows"),
            color=alt.Color("kind:N", scale=alt.Scale(range=[GOOD, WARN, "#475569"]), title=None),
            tooltip=["run_id", "kind", "rows"])
        st.altair_chart(themed(chart), width="stretch")
    with right:
        st.subheader("Warehouse tables")
        st.dataframe(d["warehouse_counts"], hide_index=True, width="stretch")


def page_quality() -> None:
    st.title("Data Quality")
    st.markdown('<div class="df-sub">Rejected records from the latest successful processing of each source file '
                '(<code>v_quarantine_current</code>).</div>', unsafe_allow_html=True)
    d = data(("monitoring", "rejection_categories"), ("monitoring", "rejection_reasons"),
             ("monitoring", "quarantine_current"), ("monitoring", "quarantine_all"),
             ("analytics", "10_rejected_record_pct"))
    if d is None:
        return
    cats = d["rejection_categories"].set_index("category")["failures"] if len(d["rejection_categories"]) else pd.Series(dtype=int)
    reasons = d["rejection_reasons"]
    pct = d["10_rejected_record_pct"]
    overall = pct[pct["source_type"] == "ALL SOURCES"]
    c = st.columns(3)
    c[0].metric("Validation failures", fmt_int(reasons["failures"].sum() if len(reasons) else 0),
                help="Rule violations; one record can violate several rules")
    c[1].metric("Rejected records", fmt_int(len(d["quarantine_current"])))
    c[2].metric("Business-rule violations", fmt_int(cats.get("Business rule violations", 0)))
    c = st.columns(4)
    c[0].metric("Missing values", fmt_int(cats.get("Missing values", 0)))
    c[1].metric("Duplicates", fmt_int(cats.get("Duplicate records", 0)))
    c[2].metric("Invalid references", fmt_int(cats.get("Invalid references", 0)))
    c[3].metric("Invalid types / malformed", fmt_int(cats.get("Invalid types / malformed", 0)))
    if len(overall):
        st.caption(f"Rejected-record rate: **{overall['rejected_pct'].iloc[0]}%** of "
                   f"{fmt_int(overall['input_rows'].iloc[0])} input rows "
                   f"(query `sql/analytics/10_rejected_record_pct.sql`).")

    if len(reasons):
        left, right = st.columns([3, 2])
        with left:
            st.subheader("Rejection reasons")
            chart = alt.Chart(reasons).mark_bar().encode(
                y=alt.Y("reason_code:N", sort="-x", title=None, axis=alt.Axis(labelLimit=260)),
                x=alt.X("failures:Q", title="Failures", axis=alt.Axis(tickMinStep=1, format="d")),
                color=alt.Color("source_type:N", scale=alt.Scale(range=[ACCENT, WARN]), title=None),
                tooltip=["reason_code", "source_type", "failures"])
            st.altair_chart(themed(chart, height=max(220, 26 * reasons["reason_code"].nunique())), width="stretch")
        with right:
            st.subheader("By category")
            cat_df = d["rejection_categories"]
            chart = alt.Chart(cat_df).mark_arc(innerRadius=60).encode(
                theta="failures:Q", color=alt.Color("category:N", title=None,
                                                    scale=alt.Scale(range=[BAD, WARN, ACCENT, "#A78BFA", GOOD])),
                tooltip=["category", "failures"])
            st.altair_chart(themed(chart), width="stretch")

    st.subheader("Rejected records")
    scope = st.radio("Scope", ["Current (latest processing per file)", "All runs (incl. failed batches)"],
                     horizontal=True, label_visibility="collapsed")
    table = d["quarantine_current"] if scope.startswith("Current") else d["quarantine_all"]
    f1, f2, f3 = st.columns([1, 1, 2])
    src = f1.multiselect("Source", sorted(table["source_type"].unique()) if len(table) else [])
    rsn = f2.multiselect("Primary reason", sorted(table["primary_reason"].unique()) if len(table) else [])
    text = f3.text_input("Search record id / file / reason")
    view = table
    if src:
        view = view[view["source_type"].isin(src)]
    if rsn:
        view = view[view["primary_reason"].isin(rsn)]
    if text:
        mask = view[["record_id", "source_file", "rejection_reasons", "source_line"]].astype(str).apply(
            lambda col: col.str.contains(text, case=False, regex=False)).any(axis=1)
        view = view[mask]
    st.dataframe(view[["source_file", "source_row_number", "record_id", "primary_reason", "source_line",
                       "rejection_reasons", "run_id", "rejected_at"]],
                 hide_index=True, width="stretch", height=420,
                 column_config={"source_row_number": st.column_config.NumberColumn("line", format="%d"),
                                "source_line": st.column_config.TextColumn("original CSV line", width="medium"),
                                "rejection_reasons": st.column_config.TextColumn(width="medium")})
    st.caption(f"{len(view):,} of {len(table):,} rows")


def page_business() -> None:
    st.title("Business Analytics")
    st.markdown('<div class="df-sub">Recognised revenue = order lines with status paid, shipped or delivered, '
                'valued at the transaction price.</div>', unsafe_allow_html=True)
    d = data(("analytics", "01_total_revenue"), ("analytics", "02_total_orders"), ("analytics", "03_total_quantity"),
             ("analytics", "04_daily_revenue"), ("analytics", "05_monthly_revenue"),
             ("analytics", "06_revenue_by_category"), ("analytics", "07_top_10_products"),
             ("analytics", "08_top_10_customers"), ("analytics", "09_average_order_value"),
             ("monitoring", "customer_insights"), ("monitoring", "status_breakdown"))
    if d is None:
        return
    rev, orders = d["01_total_revenue"].iloc[0], d["02_total_orders"].iloc[0]
    c = st.columns(5)
    c[0].metric("Total revenue", fmt_money(rev.total_revenue))
    c[1].metric("Orders (revenue)", fmt_int(orders.revenue_orders), help=f"{fmt_int(orders.all_orders)} orders in any status")
    c[2].metric("Units sold", fmt_int(d["03_total_quantity"].iloc[0].total_quantity))
    c[3].metric("Average order value", fmt_money(d["09_average_order_value"].iloc[0].average_order_value))
    c[4].metric("Pending value", fmt_money(rev.pending_value), help="Pending orders are not yet recognised revenue")

    daily = d["04_daily_revenue"]
    if daily.empty:
        st.info("The warehouse has no sales yet.")
        return
    st.subheader("Daily revenue")
    daily["full_date"] = pd.to_datetime(daily["full_date"])
    daily["revenue"] = daily["revenue"].astype(float)
    base = alt.Chart(daily).encode(x=alt.X("full_date:T", title=None))
    chart = (base.mark_area(opacity=0.18, color=ACCENT).encode(y=alt.Y("revenue:Q", title="Revenue"))
             + base.mark_line(color=ACCENT, strokeWidth=1.6).encode(
                y="revenue:Q", tooltip=[alt.Tooltip("full_date:T"), alt.Tooltip("revenue:Q", format=",.2f"), "orders"]))
    st.altair_chart(themed(chart, 260), width="stretch")

    left, right = st.columns(2)
    with left:
        st.subheader("Revenue by category")
        cat = d["06_revenue_by_category"].assign(revenue=lambda x: x["revenue"].astype(float))
        chart = alt.Chart(cat).mark_bar(color=ACCENT, cornerRadiusEnd=3).encode(
            y=alt.Y("category:N", sort="-x", title=None), x=alt.X("revenue:Q", title="Revenue"),
            tooltip=["category", alt.Tooltip("revenue:Q", format=",.2f"), "revenue_share_pct", "quantity"])
        st.altair_chart(themed(chart), width="stretch")
    with right:
        st.subheader("Monthly revenue")
        mon = d["05_monthly_revenue"].assign(revenue=lambda x: x["revenue"].astype(float),
                                             period=lambda x: x["year"].astype(str) + "-" + x["month"].astype(str).str.zfill(2))
        chart = alt.Chart(mon).mark_bar(color="#A78BFA", cornerRadiusEnd=3).encode(
            x=alt.X("period:N", title=None), y=alt.Y("revenue:Q", title="Revenue"),
            tooltip=["period", alt.Tooltip("revenue:Q", format=",.2f"), "orders"])
        st.altair_chart(themed(chart), width="stretch")

    left, right = st.columns(2)
    with left:
        st.subheader("Top 10 products")
        st.dataframe(d["07_top_10_products"], hide_index=True, width="stretch",
                     column_config={"revenue": st.column_config.NumberColumn(format="%.2f")})
    with right:
        st.subheader("Top 10 customers")
        st.dataframe(d["08_top_10_customers"], hide_index=True, width="stretch",
                     column_config={"purchase_value": st.column_config.NumberColumn(format="%.2f"),
                                    "avg_order_value": st.column_config.NumberColumn(format="%.2f"),
                                    "first_purchase": st.column_config.DateColumn(format="YYYY-MM-DD"),
                                    "last_purchase": st.column_config.DateColumn(format="YYYY-MM-DD")})

    left, right = st.columns(2)
    with left:
        st.subheader("Customer purchase frequency")
        seg = d["customer_insights"].assign(revenue=lambda x: x["revenue"].astype(float))
        chart = alt.Chart(seg).mark_bar(color=GOOD, cornerRadiusEnd=3).encode(
            x=alt.X("segment:N", sort=list(seg["segment"]), title=None), y=alt.Y("customers:Q", title="Customers"),
            tooltip=["segment", "customers", alt.Tooltip("revenue:Q", format=",.2f"), "revenue_per_customer"])
        st.altair_chart(themed(chart, 240), width="stretch")
    with right:
        st.subheader("Order lines by status")
        st.dataframe(d["status_breakdown"], hide_index=True, width="stretch",
                     column_config={"value": st.column_config.NumberColumn(format="%.2f")})


SAMPLE_BATCHES = {
    "Sample batch 1 — orders.csv + products.csv": ("orders.csv", "products.csv"),
    "Sample batch 2 — orders_batch_2.csv + products_update.csv": ("orders_batch_2.csv", "products_update.csv"),
}


def run_controls() -> None:
    st.subheader("Run the pipeline")
    holder = is_running(SETTINGS)
    busy = holder is not None or st.session_state.get("pipeline_busy", False)
    c1, c2, c3 = st.columns([3, 1, 1])
    choice = c1.selectbox("Configured sample input", list(SAMPLE_BATCHES), disabled=busy)
    force = c2.checkbox("Force reprocess", help="Reprocess files already loaded (idempotent upsert)", disabled=busy)
    sample_dir = PROJECT_ROOT / "data" / "sample"
    orders, products = (sample_dir / n for n in SAMPLE_BATCHES[choice])
    if holder:
        st.warning(f"A pipeline run is in progress (run_id `{holder.get('run_id')}`). The button is disabled until it finishes.")
    clicked = c3.button("▶ Run pipeline", type="primary", disabled=busy, width="stretch")
    if clicked:
        if not (orders.exists() and products.exists()):
            st.error("Sample input missing. Generate it with `python scripts/generate_data.py --sample`.")
            return
        st.session_state["pipeline_busy"] = True
        try:
            with st.spinner("Pipeline running…"):
                result = trigger_pipeline(orders, products, force=force, db_path=DB_PATH)
        finally:
            st.session_state["pipeline_busy"] = False
        st.cache_data.clear()
        st.session_state["last_trigger"] = result
    result = st.session_state.get("last_trigger")
    if result:
        st.markdown(f"Last triggered run: `{result.get('run_id', '—')}` {pill(result.get('status'))}",
                    unsafe_allow_html=True)
        if result.get("error_message"):
            st.error(result["error_message"])
        with st.expander("Pipeline output"):
            st.json({k: v for k, v in result.items() if k != "log_tail"})
            st.code("\n".join(result.get("log_tail", [])) or "(no log output)")


def page_runs() -> None:
    st.title("Pipeline Runs")
    st.markdown('<div class="df-sub">Inspect any execution: counts, stage timings, files, warnings and errors.</div>',
                unsafe_allow_html=True)
    run_controls()
    d = data(("monitoring", "runs"), ("monitoring", "run_files"), ("monitoring", "processed_files"),
             ("monitoring", "quarantine_all"))
    if d is None:
        return
    runs = d["runs"]
    if runs.empty:
        st.info("No runs recorded yet.")
        return
    st.subheader("Run history")
    status_filter = st.multiselect("Status", sorted(runs["execution_status"].unique()))
    table = runs[runs["execution_status"].isin(status_filter)] if status_filter else runs
    st.dataframe(table[["run_id", "start_time", "end_time", "duration_seconds", "execution_status", "trigger_source",
                        "input_rows", "valid_rows", "rejected_rows", "output_rows", "error_message"]],
                 hide_index=True, width="stretch", height=280)

    st.subheader("Run details")
    run_id = st.selectbox("Run", runs["run_id"].tolist())
    r = runs[runs["run_id"] == run_id].iloc[0]
    st.markdown(f"`{run_id}` {pill(r.execution_status)} &nbsp; trigger **{r.trigger_source}**"
                f"{' &nbsp; forced' if r.forced else ''}", unsafe_allow_html=True)
    c = st.columns(5)
    c[0].metric("Duration (s)", f"{r.duration_seconds:.3f}" if pd.notna(r.duration_seconds) else "—")
    c[1].metric("Input rows", fmt_int(r.input_rows))
    c[2].metric("Valid rows", fmt_int(r.valid_rows))
    c[3].metric("Rejected rows", fmt_int(r.rejected_rows))
    c[4].metric("Rows written", fmt_int(r.output_rows), help="Fact rows inserted + updated")
    if pd.notna(r.error_message) and r.error_message:
        st.error(r.error_message)
    left, right = st.columns([2, 3])
    with left:
        stages = pd.DataFrame({"stage": ["ingestion", "validation", "transformation", "load"],
                               "seconds": [r.ingestion_seconds, r.validation_seconds, r.transformation_seconds,
                                           r.load_seconds]}).dropna()
        if len(stages):
            chart = alt.Chart(stages).mark_bar(color=ACCENT, cornerRadiusEnd=3).encode(
                y=alt.Y("stage:N", sort=None, title=None), x=alt.X("seconds:Q", title="Seconds"),
                tooltip=["stage", alt.Tooltip("seconds:Q", format=".4f")])
            st.altair_chart(themed(chart, 180), width="stretch")
        detail = pd.DataFrame({"metric": ["facts_inserted", "facts_updated", "facts_unchanged", "facts_stale_skipped",
                                          "product_versions_inserted", "customers_inserted"],
                               "value": [r.facts_inserted, r.facts_updated, r.facts_unchanged, r.facts_stale_skipped,
                                         r.product_versions_inserted, r.customers_inserted]})
        st.dataframe(detail, hide_index=True, width="stretch")
    with right:
        st.markdown("**Files**")
        st.dataframe(d["run_files"][d["run_files"]["run_id"] == run_id].drop(columns="run_id"),
                     hide_index=True, width="stretch")
        warnings = json.loads(r.warnings) if isinstance(r.warnings, str) and r.warnings else []
        st.markdown("**Warnings**")
        if warnings:
            for w in warnings:
                st.markdown(f"- {w}")
        else:
            st.caption("None")
        rejected = d["quarantine_all"][d["quarantine_all"]["run_id"] == run_id]
        st.markdown(f"**Rejected in this run:** {len(rejected):,}")
        if len(rejected):
            st.dataframe(rejected[["source_file", "source_row_number", "record_id", "rejection_reasons"]],
                         hide_index=True, width="stretch", height=200)

    with st.expander("Processed-file ledger (idempotency)"):
        st.dataframe(d["processed_files"], hide_index=True, width="stretch")


# ================================================================================ navigation
with st.sidebar:
    st.markdown("### ◆ DataFlow")
    st.caption("E-commerce data engineering pipeline")
st.sidebar.divider()

pages = st.navigation([
    st.Page(page_overview, title="Pipeline Overview", icon=":material/monitoring:", default=True),
    st.Page(page_quality, title="Data Quality", icon=":material/rule:", url_path="quality"),
    st.Page(page_business, title="Business Analytics", icon=":material/query_stats:", url_path="business"),
    st.Page(page_runs, title="Pipeline Runs", icon=":material/history:", url_path="runs"),
])

with st.sidebar:
    holder = is_running(SETTINGS)
    st.markdown(f"Pipeline: {pill('RUNNING') if holder else pill('IDLE')}", unsafe_allow_html=True)
    st.caption(f"Warehouse: `{DB_PATH.relative_to(PROJECT_ROOT) if DB_PATH.is_relative_to(PROJECT_ROOT) else DB_PATH}`")
    if st.button("↻ Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()

pages.run()
