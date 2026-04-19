"""
dashboard/streamlit_app.py — Olist Analytics Dashboard (SaaS Grade)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Modern, production-ready dashboard for Olist e-commerce data.
Built with Streamlit, Snowflake, and Plotly.
"""
import os
import json
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import streamlit as st

# ---------- Logging ----------
log = logging.getLogger("olist-dashboard")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

# ---------- Page config ----------
st.set_page_config(
    page_title="Olist Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://github.com/noran-salm/olist-aws-snowflake-pipeline",
        "Report a bug": "https://github.com/noran-salm/olist-aws-snowflake-pipeline/issues",
        "About": "AWS + Snowflake + dbt → Olist E-commerce Dashboard",
    },
)

# ---------- Custom CSS (modern, SaaS‑like) ----------
st.markdown("""
<style>
    /* Global */
    body {
        background-color: #f8fafc;
        font-family: 'Inter', system-ui, -apple-system, sans-serif;
    }
    /* Metric cards */
    div[data-testid="metric-container"] {
        background: white;
        border-radius: 1rem;
        padding: 1.2rem 1rem;
        box-shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1), 0 1px 2px -1px rgb(0 0 0 / 0.1);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        border: 1px solid #e2e8f0;
    }
    div[data-testid="metric-container"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 15px -3px rgb(0 0 0 / 0.1);
    }
    div[data-testid="metric-container"] > label {
        font-size: 0.7rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #475569 !important;
    }
    div[data-testid="metric-container"] > div > div > div {
        font-size: 1.8rem !important;
        font-weight: 700 !important;
        color: #0f172a !important;
    }
    /* Section headers */
    .section-header {
        font-size: 1rem;
        font-weight: 600;
        color: #1e293b;
        margin: 1.5rem 0 0.75rem;
        padding-bottom: 0.4rem;
        border-bottom: 2px solid #e2e8f0;
        letter-spacing: -0.01em;
    }
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e2e8f0;
        padding: 1.5rem 0.5rem;
    }
    [data-testid="stSidebar"] .stMarkdown h3 {
        font-size: 1.1rem;
        font-weight: 600;
        color: #0f172a;
    }
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.25rem;
        background-color: #f1f5f9;
        border-radius: 0.75rem;
        padding: 0.25rem;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 0.5rem;
        padding: 0.5rem 1rem;
        font-weight: 500;
        font-size: 0.9rem;
        color: #475569;
        background-color: transparent;
    }
    .stTabs [aria-selected="true"] {
        background-color: white;
        color: #f97316;
        box-shadow: 0 1px 2px 0 rgb(0 0 0 / 0.05);
    }
    /* Buttons */
    .stButton button {
        background-color: #f97316;
        color: white;
        border-radius: 0.5rem;
        font-weight: 500;
        border: none;
        transition: background-color 0.2s;
    }
    .stButton button:hover {
        background-color: #ea580c;
    }
    /* Download buttons */
    .stDownloadButton button {
        background-color: #f1f5f9;
        color: #1e293b;
        border: 1px solid #cbd5e1;
    }
    .stDownloadButton button:hover {
        background-color: #e2e8f0;
    }
    /* Dataframes */
    .stDataFrame {
        border-radius: 0.75rem;
        border: 1px solid #e2e8f0;
        overflow: hidden;
    }
    /* Warning / stale */
    .stale-warning {
        background: #fffbeb;
        border: 1px solid #f59e0b;
        border-radius: 0.5rem;
        padding: 0.5rem 1rem;
        font-size: 0.8rem;
        color: #92400e;
        display: inline-block;
    }
    /* Hide Streamlit footer branding */
    footer {
        visibility: hidden;
    }
    /* Spacing helpers */
    .mt-2 { margin-top: 0.5rem; }
    .mb-2 { margin-bottom: 0.5rem; }
</style>
""", unsafe_allow_html=True)

# ---------- Theme & constants ----------
THEME = {
    "colors": {
        "primary": "#f97316",
        "secondary": "#10b981",
        "tertiary": "#3b82f6",
        "warning": "#f59e0b",
        "danger": "#ef4444",
        "gray": {
            50: "#f8fafc",
            100: "#f1f5f9",
            200: "#e2e8f0",
            500: "#64748b",
            700: "#334155",
            900: "#0f172a",
        }
    },
    "plotly_template": "plotly_white",
    "font_family": "Inter, system-ui, sans-serif",
}

PALETTE = ["#f97316", "#10b981", "#3b82f6", "#8b5cf6", "#f59e0b", "#ec4899", "#14b8a6", "#64748b"]
STATUS_COLOR = {
    "delivered": "#10b981", "shipped": "#3b82f6", "processing": "#f59e0b",
    "canceled": "#ef4444", "unavailable": "#9ca3af", "invoiced": "#8b5cf6",
    "approved": "#14b8a6", "created": "#f97316",
}
TIER_ICON = {"platinum": "🥇 Platinum", "gold": "🥈 Gold", "silver": "🥉 Silver", "bronze": "🔵 Bronze"}

# ---------- Helper functions ----------
def fmt_brl(v: float) -> str:
    if v >= 1_000_000:
        return f"R$ {v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"R$ {v/1_000:.1f}K"
    return f"R$ {v:,.0f}"

def fmt_num(v: float) -> str:
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v/1_000:.1f}K"
    return f"{int(v):,}"

def years_to_sql(years: List[int]) -> str:
    return ",".join(str(y) for y in years)

def apply_chart_theme(fig: go.Figure, height: int = 350) -> go.Figure:
    """Unify Plotly chart styling."""
    fig.update_layout(
        template=THEME["plotly_template"],
        font_family=THEME["font_family"],
        font_color=THEME["colors"]["gray"][700],
        title_font_size=14,
        title_font_color=THEME["colors"]["gray"][900],
        hoverlabel=dict(bgcolor="white", font_size=11, bordercolor="#e2e8f0"),
        margin=dict(l=10, r=10, t=40, b=10),
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, zeroline=False, tickangle=-30),
        yaxis=dict(showgrid=True, gridcolor="#e2e8f0", zeroline=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig

# ---------- Credentials & connection ----------
@st.cache_resource(show_spinner=False)
def _get_credentials() -> dict:
    try:
        return dict(st.secrets["snowflake"])
    except Exception:
        pass
    try:
        import boto3
        client = boto3.client("secretsmanager", region_name="us-east-1")
        return json.loads(client.get_secret_value(SecretId="olist/snowflake/credentials")["SecretString"])
    except Exception:
        pass
    if os.environ.get("SNOWFLAKE_ACCOUNT"):
        return {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "password": os.environ["SNOWFLAKE_PASSWORD"],
            "role": os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
            "database": os.environ.get("SNOWFLAKE_DATABASE", "OLIST_DW"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA", "MARTS"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "OLIST_WH"),
        }
    raise RuntimeError("No Snowflake credentials found. Add secrets or environment variables.")

@st.cache_resource(show_spinner=False)
def get_connection():
    creds = _get_credentials()
    return snowflake.connector.connect(
        account=creds["account"], user=creds["user"], password=creds["password"],
        role=creds.get("role", "SYSADMIN"), database=creds.get("database", "OLIST_DW"),
        schema=creds.get("schema", "MARTS"), warehouse=creds.get("warehouse", "OLIST_WH"),
        session_parameters={"QUERY_TAG": "streamlit-dashboard"},
    )

@st.cache_data(ttl=600, show_spinner=False)
def run_query(sql: str, label: str = "") -> pd.DataFrame:
    try:
        cur = get_connection().cursor()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        df.columns = [c.lower() for c in df.columns]
        log.info("Query OK [%s]: %d rows", label or sql[:40], len(df))
        return df
    except Exception as e:
        log.error("Query failed [%s]: %s", label, e)
        st.error(f"Query error ({label}): {e}")
        return pd.DataFrame()

# ---------- Data loading (cached) ----------
@st.cache_data(ttl=600, show_spinner=False)
def load_freshness() -> Optional[datetime]:
    df = run_query("SELECT MAX(order_purchase_timestamp) AS ts FROM OLIST_DW.MARTS.fct_orders", label="freshness")
    if df.empty or df["ts"].iloc[0] is None:
        return None
    return pd.to_datetime(df["ts"].iloc[0])

@st.cache_data(ttl=600, show_spinner=False)
def load_kpis(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            COUNT(DISTINCT order_id) AS total_orders,
            COUNT(*) AS total_items,
            ROUND(SUM(order_item_revenue), 2) AS total_gmv,
            ROUND(AVG(avg_review_score), 2) AS avg_review,
            ROUND(AVG(CASE WHEN delivery_days >= 0 THEN delivery_days END), 1) AS avg_days,
            ROUND(100.0 * SUM(is_late_delivery::INTEGER) / NULLIF(COUNT(*),0), 1) AS late_pct,
            COUNT(DISTINCT customer_id) AS unique_customers,
            COUNT(DISTINCT seller_id) AS active_sellers
        FROM OLIST_DW.MARTS.fct_orders
        WHERE YEAR(order_purchase_timestamp) IN ({years_csv})
    """, label="kpis")

@st.cache_data(ttl=600, show_spinner=False)
def load_monthly_revenue(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT order_year_month, SUM(revenue_brl) AS revenue, SUM(total_orders) AS orders,
               SUM(total_items) AS items, AVG(avg_review) AS avg_review
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE YEAR(TO_DATE(order_year_month || '-01')) IN ({years_csv})
        GROUP BY 1 ORDER BY 1
    """, label="monthly_revenue")

@st.cache_data(ttl=600, show_spinner=False)
def load_category_revenue(years_csv: str, top_n: int) -> pd.DataFrame:
    return run_query(f"""
        SELECT COALESCE(product_category,'unknown') AS category,
               ROUND(SUM(revenue_brl),0) AS revenue, SUM(total_orders) AS orders,
               ROUND(AVG(avg_review),2) AS avg_review, ROUND(AVG(avg_delivery_days),1) AS avg_days
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE YEAR(TO_DATE(order_year_month || '-01')) IN ({years_csv})
        GROUP BY 1 ORDER BY revenue DESC LIMIT {top_n}
    """, label=f"categories_{top_n}")

@st.cache_data(ttl=600, show_spinner=False)
def load_order_status(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT order_status, COUNT(DISTINCT order_id) AS cnt,
               ROUND(100.0 * COUNT(DISTINCT order_id) / SUM(COUNT(DISTINCT order_id)) OVER (), 1) AS pct
        FROM OLIST_DW.MARTS.fct_orders
        WHERE YEAR(order_purchase_timestamp) IN ({years_csv})
        GROUP BY 1 ORDER BY cnt DESC
    """, label="order_status")

@st.cache_data(ttl=600, show_spinner=False)
def load_top_sellers(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT f.seller_id, s.state, s.seller_tier AS tier,
               COUNT(DISTINCT f.order_id) AS orders,
               ROUND(SUM(f.order_item_revenue),0) AS gmv,
               ROUND(AVG(f.avg_review_score),2) AS score,
               ROUND(AVG(CASE WHEN f.delivery_days>=0 THEN f.delivery_days END),1) AS avg_days,
               ROUND(100.0 * SUM(f.is_late_delivery::INTEGER) / NULLIF(COUNT(*),0),1) AS late_pct
        FROM OLIST_DW.MARTS.fct_orders f
        JOIN OLIST_DW.MARTS.dim_sellers s ON f.seller_id = s.seller_id
        WHERE YEAR(f.order_purchase_timestamp) IN ({years_csv})
        GROUP BY 1,2,3 ORDER BY gmv DESC LIMIT 20
    """, label="sellers")

@st.cache_data(ttl=600, show_spinner=False)
def load_region_data(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT customer_region AS region, customer_state AS state, customer_state_name AS state_name,
               ROUND(SUM(revenue_brl),0) AS revenue, SUM(total_orders) AS orders,
               ROUND(AVG(avg_review),2) AS avg_review, ROUND(AVG(avg_delivery_days),1) AS avg_days
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE customer_region IS NOT NULL AND YEAR(TO_DATE(order_year_month || '-01')) IN ({years_csv})
        GROUP BY 1,2,3 ORDER BY revenue DESC
    """, label="region")

@st.cache_data(ttl=600, show_spinner=False)
def load_delivery(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT customer_state, COUNT(DISTINCT order_id) AS orders,
               ROUND(AVG(CASE WHEN delivery_days>=0 THEN delivery_days END),1) AS avg_days,
               ROUND(100.0 * SUM(is_late_delivery::INTEGER) / NULLIF(COUNT(*),0),1) AS late_pct,
               ROUND(AVG(avg_review_score),2) AS avg_score
        FROM OLIST_DW.MARTS.fct_orders
        WHERE YEAR(order_purchase_timestamp) IN ({years_csv}) AND customer_state IS NOT NULL
        GROUP BY 1 HAVING COUNT(DISTINCT order_id) > 100
        ORDER BY late_pct DESC LIMIT 15
    """, label="delivery")

# ---------- UI Components ----------
def render_header(ts: Optional[datetime]):
    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown("## 🛒 Olist E-commerce Analytics")
        st.caption("AWS Lambda → S3 → Glue ETL → Snowflake → dbt → Streamlit")
    with c2:
        if ts:
            age_h = (datetime.now(timezone.utc) - ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            if age_h > 720:
                st.markdown(f'<div class="stale-warning">📅 Static dataset<br>Latest order: {ts.strftime("%Y-%m-%d")}</div>', unsafe_allow_html=True)
            else:
                st.success(f"✅ Fresh data as of {ts.strftime('%Y-%m-%d')}")
        else:
            st.warning("⚠️ Cannot determine data freshness")

def render_kpis(df: pd.DataFrame):
    if df.empty:
        st.info("No KPI data for selected filters. Try changing the year range.")
        return
    r = df.iloc[0]
    cols = st.columns(8)
    metrics = [
        ("📦 Orders", fmt_num(r["total_orders"])),
        ("🛍️ Items", fmt_num(r["total_items"])),
        ("💰 GMV", fmt_brl(r["total_gmv"])),
        ("👥 Customers", fmt_num(r["unique_customers"])),
        ("🏪 Sellers", fmt_num(r["active_sellers"])),
        ("⭐ Review", f"{r['avg_review']:.2f} / 5"),
        ("🚚 Delivery", f"{r['avg_days']:.1f} days"),
        ("⏰ Late", f"{r['late_pct']:.1f}%"),
    ]
    for col, (label, value) in zip(cols, metrics):
        col.metric(label, value)

def render_revenue_trend(df: pd.DataFrame):
    if df.empty:
        st.info("No revenue data for the selected years.")
        return
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=df["order_year_month"], y=df["revenue"], name="Revenue (BRL)",
                         marker_color=THEME["colors"]["primary"], opacity=0.85,
                         hovertemplate="<b>%{x}</b><br>Revenue: R$ %{y:,.0f}<extra></extra>"),
                  secondary_y=False)
    fig.add_trace(go.Scatter(x=df["order_year_month"], y=df["orders"], name="Orders",
                             mode="lines+markers", line=dict(color=THEME["colors"]["tertiary"], width=2),
                             marker=dict(size=5),
                             hovertemplate="<b>%{x}</b><br>Orders: %{y:,}<extra></extra>"),
                  secondary_y=True)
    fig = apply_chart_theme(fig, height=350)
    fig.update_yaxes(title_text="Revenue (BRL)", tickformat=",.0f", secondary_y=False)
    fig.update_yaxes(title_text="Orders", showgrid=False, secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

def render_categories(df: pd.DataFrame, top_n: int):
    if df.empty:
        st.info("No category data. Try adjusting the top N or year filter.")
        return
    fig = px.bar(df, x="revenue", y="category", orientation="h",
                 color="avg_review", color_continuous_scale="RdYlGn", range_color=[3.0, 5.0],
                 labels={"revenue": "Revenue (BRL)", "category": "", "avg_review": "Avg Review"},
                 hover_data={"orders": True, "avg_review": ":.2f", "avg_days": True},
                 custom_data=["orders", "avg_review", "avg_days"],
                 title=f"Top {top_n} categories by revenue")
    fig.update_traces(hovertemplate="<b>%{y}</b><br>Revenue: R$ %{x:,.0f}<br>Orders: %{customdata[0]:,}<br>Review: %{customdata[1]:.2f}<br>Avg days: %{customdata[2]:.1f}<extra></extra>")
    fig.update_yaxes(autorange="reversed")
    fig = apply_chart_theme(fig, height=400)
    st.plotly_chart(fig, use_container_width=True)

def render_order_status(df: pd.DataFrame):
    if df.empty:
        st.info("No order status data.")
        return
    colors = [STATUS_COLOR.get(s, "#9ca3af") for s in df["order_status"]]
    total = int(df["cnt"].sum())
    fig = go.Figure(go.Pie(labels=df["order_status"].str.title(), values=df["cnt"],
                           marker=dict(colors=colors, line=dict(color="white", width=2)),
                           hole=0.52, textinfo="percent",
                           hovertemplate="<b>%{label}</b><br>%{value:,} orders (%{percent})<extra></extra>"))
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font_family=THEME["font_family"], font_color=THEME["colors"]["gray"][700],
                      margin=dict(l=0, r=0, t=28, b=0), height=320, showlegend=True,
                      legend=dict(orientation="v", x=1.0, y=0.5, font=dict(size=10)))
    fig.add_annotation(text=f"<b>{fmt_num(total)}</b><br><span style='font-size:10px'>orders</span>",
                       x=0.5, y=0.5, showarrow=False, font=dict(size=14), xref="paper", yref="paper")
    st.plotly_chart(fig, use_container_width=True)

def render_top_sellers(df: pd.DataFrame):
    if df.empty:
        st.info("No seller data for the selected filters.")
        return
    d = df.copy()
    d["tier"] = d["tier"].map(lambda t: TIER_ICON.get(t, t))
    d["gmv"] = d["gmv"].apply(fmt_brl)
    d["late_pct"] = d["late_pct"].apply(lambda x: f"{x:.1f}%")
    d["score"] = d["score"].apply(lambda x: f"⭐ {x:.2f}")
    d = d.rename(columns={
        "seller_id": "Seller ID", "state": "State", "tier": "Tier",
        "orders": "Orders", "gmv": "GMV", "score": "Review",
        "avg_days": "Avg Days", "late_pct": "Late %"
    })
    st.dataframe(d[["Seller ID", "State", "Tier", "Orders", "GMV", "Review", "Avg Days", "Late %"]],
                 use_container_width=True, height=400, hide_index=True)

def render_region(df: pd.DataFrame):
    if df.empty:
        st.info("No region data.")
        return
    agg = df.groupby("region").agg(revenue=("revenue", "sum"), orders=("orders", "sum")).reset_index().sort_values("revenue", ascending=False)
    fig = px.bar(agg, x="region", y="revenue", color="region", color_discrete_sequence=PALETTE,
                 labels={"revenue": "Revenue (BRL)", "region": "Region"}, custom_data=["orders"])
    fig.update_traces(hovertemplate="<b>%{x}</b><br>Revenue: R$ %{y:,.0f}<br>Orders: %{customdata[0]:,}<extra></extra>")
    fig = apply_chart_theme(fig, height=280)
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

def render_state_table(df: pd.DataFrame):
    if df.empty:
        return
    d = df.copy()
    d["revenue"] = d["revenue"].apply(fmt_brl)
    d = d.rename(columns={
        "state_name": "State", "state": "Code", "region": "Region",
        "revenue": "Revenue", "orders": "Orders",
        "avg_review": "Avg Review", "avg_days": "Avg Days"
    })
    st.dataframe(d[["State", "Code", "Region", "Revenue", "Orders", "Avg Review", "Avg Days"]],
                 use_container_width=True, height=280, hide_index=True)

def render_delivery(df: pd.DataFrame):
    if df.empty:
        st.info("No delivery data. Try selecting different years or check data availability.")
        return
    fig = make_subplots(rows=1, cols=2, horizontal_spacing=0.14,
                        subplot_titles=("Late Rate (%) by State", "Avg Delivery Days by State"))
    top_late = df.sort_values("late_pct", ascending=True).tail(10)
    fig.add_trace(go.Bar(x=top_late["late_pct"], y=top_late["customer_state"], orientation="h",
                         marker_color=[THEME["colors"]["danger"] if x > 15 else THEME["colors"]["warning"] if x > 8 else THEME["colors"]["secondary"] for x in top_late["late_pct"]],
                         hovertemplate="<b>%{y}</b>: %{x:.1f}%<extra></extra>", name="Late %"), row=1, col=1)
    top_days = df.sort_values("avg_days", ascending=True).tail(10)
    fig.add_trace(go.Bar(x=top_days["avg_days"], y=top_days["customer_state"], orientation="h",
                         marker_color=THEME["colors"]["tertiary"], opacity=0.8,
                         hovertemplate="<b>%{y}</b>: %{x:.1f} days<extra></extra>", name="Avg Days"), row=1, col=2)
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font_family=THEME["font_family"], margin=dict(l=0, r=0, t=40, b=0),
                      height=340, showlegend=False)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    st.plotly_chart(fig, use_container_width=True)

def download_btn(df: pd.DataFrame, fname: str, label: str = "⬇️ Download CSV"):
    if not df.empty:
        st.download_button(label, data=df.to_csv(index=False).encode("utf-8"),
                           file_name=fname, mime="text/csv", use_container_width=True)

# ---------- Sidebar ----------
def render_sidebar() -> Dict[str, Any]:
    with st.sidebar:
        st.markdown("### 🛒 Olist Analytics")
        st.divider()
        st.markdown("**Filters**")
        years = st.multiselect("Order Year", options=[2016, 2017, 2018], default=[2017, 2018],
                               help="Select one or more years to analyse")
        if not years:
            years = [2017, 2018]
        top_n = st.slider("Top N categories", 5, 20, 10, step=1, help="How many top categories to show")
        st.divider()
        show_raw = st.checkbox("Show raw data explorer", value=False,
                               help="Display a table explorer for raw Snowflake tables")
        if st.button("🔄 Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()
        st.divider()
        st.caption("**Pipeline**  \nAWS Lambda → S3 → Glue → Snowflake → dbt")
        st.caption("**Data**  \nOlist Brazilian E-commerce (2016–2018)")
    return {
        "years": years,
        "years_csv": years_to_sql(years),
        "top_n": top_n,
        "show_raw": show_raw,
    }

# ---------- Main app ----------
def main():
    filters = render_sidebar()

    # Freshness
    with st.spinner("Checking data freshness…"):
        last_ts = load_freshness()
    render_header(last_ts)
    st.divider()

    # KPIs
    with st.spinner("Loading KPIs…"):
        kpi_df = load_kpis(filters["years_csv"])
    render_kpis(kpi_df)
    st.divider()

    # Tabs (lazy loading: data fetched only when tab is opened)
    tabs = st.tabs(["📈 Revenue", "🏆 Categories", "🗺️ Geography", "🥇 Sellers", "🚚 Delivery"])
    tab_keys = ["revenue", "categories", "geo", "sellers", "delivery"]

    for tab, key in zip(tabs, tab_keys):
        with tab:
            if f"loaded_{key}" not in st.session_state:
                st.session_state[f"loaded_{key}"] = False
            if not st.session_state[f"loaded_{key}"]:
                st.session_state[f"loaded_{key}"] = True
                with st.spinner(f"Loading {key} data…"):
                    if key == "revenue":
                        rev_df = load_monthly_revenue(filters["years_csv"])
                        status_df = load_order_status(filters["years_csv"])
                        st.session_state["rev_df"] = rev_df
                        st.session_state["status_df"] = status_df
                    elif key == "categories":
                        cat_df = load_category_revenue(filters["years_csv"], filters["top_n"])
                        st.session_state["cat_df"] = cat_df
                    elif key == "geo":
                        reg_df = load_region_data(filters["years_csv"])
                        st.session_state["reg_df"] = reg_df
                    elif key == "sellers":
                        sell_df = load_top_sellers(filters["years_csv"])
                        st.session_state["sell_df"] = sell_df
                    elif key == "delivery":
                        del_df = load_delivery(filters["years_csv"])
                        st.session_state["del_df"] = del_df

            # Render based on stored data
            if key == "revenue":
                col_l, col_r = st.columns([2, 1])
                with col_l:
                    st.markdown('<p class="section-header">Monthly Revenue & Order Volume</p>', unsafe_allow_html=True)
                    if "rev_df" in st.session_state:
                        render_revenue_trend(st.session_state["rev_df"])
                        download_btn(st.session_state["rev_df"], "monthly_revenue.csv")
                with col_r:
                    st.markdown('<p class="section-header">Order Status</p>', unsafe_allow_html=True)
                    if "status_df" in st.session_state:
                        render_order_status(st.session_state["status_df"])
            elif key == "categories":
                st.markdown(f'<p class="section-header">Top {filters["top_n"]} Categories by Revenue</p>', unsafe_allow_html=True)
                st.caption("Color = average review score (green = higher, red = lower)")
                if "cat_df" in st.session_state:
                    render_categories(st.session_state["cat_df"], filters["top_n"])
                    download_btn(st.session_state["cat_df"], "top_categories.csv")
            elif key == "geo":
                col_l, col_r = st.columns([1, 2])
                with col_l:
                    st.markdown('<p class="section-header">Revenue by Region</p>', unsafe_allow_html=True)
                    if "reg_df" in st.session_state:
                        render_region(st.session_state["reg_df"])
                with col_r:
                    st.markdown('<p class="section-header">State Breakdown</p>', unsafe_allow_html=True)
                    if "reg_df" in st.session_state:
                        render_state_table(st.session_state["reg_df"])
                if "reg_df" in st.session_state:
                    download_btn(st.session_state["reg_df"], "geography.csv")
            elif key == "sellers":
                st.markdown('<p class="section-header">Top 20 Sellers by GMV</p>', unsafe_allow_html=True)
                st.caption("Tier: 🥇 Platinum ≥ R$50K · 🥈 Gold ≥ R$10K · 🥉 Silver ≥ R$1K · 🔵 Bronze")
                if "sell_df" in st.session_state:
                    sell_df = st.session_state["sell_df"]
                    tiers = ["All"] + sorted(sell_df["tier"].dropna().unique().tolist()) if not sell_df.empty else ["All"]
                    sel_tier = st.selectbox("Filter by tier", tiers)
                    if sel_tier != "All" and not sell_df.empty:
                        sell_df = sell_df[sell_df["tier"] == sel_tier]
                    render_top_sellers(sell_df)
                    download_btn(sell_df, "top_sellers.csv")
            elif key == "delivery":
                st.markdown('<p class="section-header">Delivery Performance by State</p>', unsafe_allow_html=True)
                st.caption("States with > 100 orders. Red = late rate > 15%, amber = > 8%")
                if "del_df" in st.session_state:
                    render_delivery(st.session_state["del_df"])
                    download_btn(st.session_state["del_df"], "delivery_performance.csv")

    # Raw data explorer (optional)
    if filters["show_raw"]:
        st.divider()
        st.markdown("### 🔍 Raw Data Explorer")
        tbl = st.selectbox("Select table", ["fct_orders", "fct_monthly_revenue", "dim_customers", "dim_products", "dim_sellers"])
        limit = st.slider("Rows to show", 50, 1000, 200, step=50)
        with st.spinner(f"Loading {tbl}…"):
            raw_df = run_query(f"SELECT * FROM OLIST_DW.MARTS.{tbl} LIMIT {limit}", label=f"raw_{tbl}")
        if not raw_df.empty:
            st.dataframe(raw_df, use_container_width=True)
            download_btn(raw_df, f"{tbl}.csv", f"⬇️ Download {tbl}.csv")

    # Footer
    st.divider()
    st.caption(
        f"© Olist Analytics · AWS Lambda → S3 → Glue ETL → Snowflake → dbt Core → Streamlit · "
        f"Dashboard refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

if __name__ == "__main__":
    main()