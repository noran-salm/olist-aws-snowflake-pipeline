"""
dashboard/streamlit_app.py
Olist E-commerce Analytics — Production Dashboard
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture:
  ├── Config layer   — page setup, CSS, theme
  ├── Data layer     — cached Snowflake queries
  ├── Logic layer    — KPI computation, transforms
  └── UI layer       — rendering functions per section

Credential priority:
  1. st.secrets["snowflake"]  → Streamlit Cloud
  2. AWS Secrets Manager       → AWS / ECS
  3. Environment variables     → Local dev
"""

# ── Standard library ──────────────────────────────────────────
import os
import json
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

# ── Third-party ───────────────────────────────────────────────
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import streamlit as st

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("olist-dashboard")

# ══════════════════════════════════════════════════════════════
# 1. CONFIG LAYER
# ══════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Olist Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help":     "https://github.com/noran-salm/olist-aws-snowflake-pipeline",
        "Report a bug": "https://github.com/noran-salm/olist-aws-snowflake-pipeline/issues",
        "About":        "Olist E-commerce Analytics — AWS + Snowflake + dbt pipeline",
    },
)

# ── Inline CSS ────────────────────────────────────────────────
st.markdown("""
<style>
div[data-testid="metric-container"] {
    background: var(--background-color, #f8f9fa);
    border: 1px solid rgba(0,0,0,0.08);
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
div[data-testid="metric-container"] > label {
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #6b7280 !important;
}
div[data-testid="metric-container"] > div > div > div {
    font-size: 1.8rem !important;
    font-weight: 700 !important;
}
.section-header {
    font-size: 1rem;
    font-weight: 600;
    color: #374151;
    margin: 1.5rem 0 0.75rem;
    padding-bottom: 0.4rem;
    border-bottom: 2px solid #f3f4f6;
}
.stale-warning {
    background: #fffbeb;
    border: 1px solid #f59e0b;
    border-radius: 8px;
    padding: 8px 14px;
    font-size: 0.82rem;
    color: #92400e;
}
button[data-baseweb="tab"] {
    font-size: 0.88rem !important;
    font-weight: 500 !important;
}
#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Color palette ─────────────────────────────────────────────
COLORS = {
    "primary":   "#FF6B35",
    "secondary": "#1D9E75",
    "tertiary":  "#3B8BD4",
    "neutral":   "#6b7280",
    "success":   "#10b981",
    "warning":   "#f59e0b",
    "danger":    "#ef4444",
    "bg":        "#f9fafb",
}

PALETTE = [
    "#FF6B35","#1D9E75","#3B8BD4","#8b5cf6",
    "#f59e0b","#ec4899","#14b8a6","#64748b",
]

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", size=12, color="#374151"),
    margin=dict(l=0, r=0, t=28, b=0),
    legend=dict(
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor="rgba(0,0,0,0.1)",
        borderwidth=1,
        font=dict(size=11),
    ),
    xaxis=dict(showgrid=False, zeroline=False),
    yaxis=dict(
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False,
        tickformat=",",
    ),
    hoverlabel=dict(
        bgcolor="white",
        bordercolor="rgba(0,0,0,0.15)",
        font_size=12,
    ),
)


# ══════════════════════════════════════════════════════════════
# 2. DATA LAYER — cached Snowflake queries
# ══════════════════════════════════════════════════════════════

@st.cache_resource(show_spinner=False)
def _get_credentials() -> dict:
    """Fetch Snowflake credentials — try 3 sources in priority order."""
    try:
        creds = dict(st.secrets["snowflake"])
        log.info("Credentials: st.secrets")
        return creds
    except (KeyError, FileNotFoundError, AttributeError):
        pass

    try:
        import boto3
        client = boto3.client("secretsmanager", region_name="us-east-1")
        resp   = client.get_secret_value(SecretId="olist/snowflake/credentials")
        creds  = json.loads(resp["SecretString"])
        log.info("Credentials: AWS Secrets Manager")
        return creds
    except Exception:
        pass

    if os.environ.get("SNOWFLAKE_ACCOUNT"):
        log.info("Credentials: environment variables")
        return {
            "account":   os.environ["SNOWFLAKE_ACCOUNT"],
            "user":      os.environ["SNOWFLAKE_USER"],
            "password":  os.environ["SNOWFLAKE_PASSWORD"],
            "role":      os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN"),
            "database":  os.environ.get("SNOWFLAKE_DATABASE",  "OLIST_DW"),
            "schema":    os.environ.get("SNOWFLAKE_SCHEMA",    "MARTS"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "OLIST_WH"),
        }

    raise RuntimeError(
        "No Snowflake credentials found. "
        "Add [snowflake] to .streamlit/secrets.toml or set env vars."
    )


@st.cache_resource(show_spinner=False)
def get_connection():
    """Create and cache a single Snowflake connection for the session."""
    creds = _get_credentials()
    conn  = snowflake.connector.connect(
        account         = creds["account"],
        user            = creds["user"],
        password        = creds["password"],
        role            = creds.get("role",      "SYSADMIN"),
        database        = creds.get("database",  "OLIST_DW"),
        schema          = creds.get("schema",    "MARTS"),
        warehouse       = creds.get("warehouse", "OLIST_WH"),
        session_parameters={"QUERY_TAG": "streamlit-dashboard"},
    )
    return conn


@st.cache_data(ttl=600, show_spinner=False)
def run_query(sql: str, label: str = "") -> pd.DataFrame:
    """Execute SQL and return a DataFrame. Cached for 10 minutes."""
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        df = cursor.fetch_pandas_all()
        df.columns = [c.lower() for c in df.columns]
        log.info("Query OK [%s]: %d rows", label or sql[:40], len(df))
        return df
    except Exception as exc:
        log.error("Query failed [%s]: %s", label, exc)
        st.error(f"Query error ({label}): {exc}")
        return pd.DataFrame()


def get_data_freshness() -> Optional[datetime]:
    """Return the timestamp of the most recent order in MARTS."""
    df = run_query(
        "SELECT MAX(order_purchase_timestamp) AS ts FROM OLIST_DW.MARTS.fct_orders",
        label="freshness"
    )
    if df.empty or df["ts"][0] is None:
        return None
    return pd.to_datetime(df["ts"][0])


def _extract_years(year_filter: str) -> str:
    """Extract year list from filter like 'YEAR(col) IN (2017,2018)'"""
    match = re.search(r'IN\s*\(([^)]+)\)', year_filter)
    return match.group(1) if match else ""


@st.cache_data(ttl=600, show_spinner=False)
def load_kpis(year_filter: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            COUNT(DISTINCT order_id)                                       AS total_orders,
            COUNT(*)                                                       AS total_items,
            ROUND(SUM(order_item_revenue), 2)                             AS total_gmv,
            ROUND(AVG(avg_review_score), 2)                               AS avg_review,
            ROUND(AVG(CASE WHEN delivery_days >= 0 THEN delivery_days END), 1) AS avg_days,
            ROUND(100.0 * COUNT_IF(is_late_delivery) / NULLIF(COUNT(*), 0), 1) AS late_pct,
            COUNT(DISTINCT customer_id)                                    AS unique_customers,
            COUNT(DISTINCT seller_id)                                      AS active_sellers
        FROM OLIST_DW.MARTS.fct_orders
        WHERE {year_filter}
    """, label="kpis")


@st.cache_data(ttl=600, show_spinner=False)
def load_monthly_revenue(year_filter: str) -> pd.DataFrame:
    years = _extract_years(year_filter)
    if not years:
        return pd.DataFrame()
    return run_query(f"""
        SELECT
            order_year_month,
            SUM(revenue_brl)   AS revenue,
            SUM(total_orders)  AS orders,
            SUM(total_items)   AS items,
            AVG(avg_review)    AS avg_review
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE YEAR(TO_DATE(order_year_month || '-01')) IN ({years})
        GROUP BY 1
        ORDER BY 1
    """, label="monthly_revenue")


@st.cache_data(ttl=600, show_spinner=False)
def load_category_revenue(year_filter: str, top_n: int) -> pd.DataFrame:
    years = _extract_years(year_filter)
    if not years:
        return pd.DataFrame()
    return run_query(f"""
        SELECT
            COALESCE(product_category, 'unknown')  AS category,
            ROUND(SUM(revenue_brl), 0)             AS revenue,
            SUM(total_orders)                      AS orders,
            ROUND(AVG(avg_review), 2)              AS avg_review,
            ROUND(AVG(avg_delivery_days), 1)       AS avg_days
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE YEAR(TO_DATE(order_year_month || '-01')) IN ({years})
        GROUP BY 1
        ORDER BY revenue DESC
        LIMIT {top_n}
    """, label=f"categories_top{top_n}")


@st.cache_data(ttl=600, show_spinner=False)
def load_order_status(year_filter: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            order_status,
            COUNT(DISTINCT order_id) AS cnt,
            ROUND(100.0 * COUNT(DISTINCT order_id) /
                SUM(COUNT(DISTINCT order_id)) OVER (), 1) AS pct
        FROM OLIST_DW.MARTS.fct_orders
        WHERE {year_filter}
        GROUP BY 1
        ORDER BY cnt DESC
    """, label="order_status")


@st.cache_data(ttl=600, show_spinner=False)
def load_top_sellers(year_filter: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            f.seller_id,
            s.state,
            s.seller_tier                                      AS tier,
            COUNT(DISTINCT f.order_id)                         AS orders,
            ROUND(SUM(f.order_item_revenue), 0)               AS gmv,
            ROUND(AVG(f.avg_review_score), 2)                 AS score,
            ROUND(AVG(CASE WHEN f.delivery_days >= 0
                           THEN f.delivery_days END), 1)      AS avg_days,
            ROUND(100.0 * COUNT_IF(f.is_late_delivery) /
                  NULLIF(COUNT(*), 0), 1)                     AS late_pct
        FROM OLIST_DW.MARTS.fct_orders f
        JOIN OLIST_DW.MARTS.dim_sellers s ON f.seller_id = s.seller_id
        WHERE {year_filter}
        GROUP BY 1, 2, 3
        ORDER BY gmv DESC
        LIMIT 20
    """, label="top_sellers")


@st.cache_data(ttl=600, show_spinner=False)
def load_region_data(year_filter: str) -> pd.DataFrame:
    years = _extract_years(year_filter)
    if not years:
        return pd.DataFrame()
    return run_query(f"""
        SELECT
            customer_region                        AS region,
            customer_state                         AS state,
            customer_state_name                    AS state_name,
            ROUND(SUM(revenue_brl), 0)            AS revenue,
            SUM(total_orders)                     AS orders,
            ROUND(AVG(avg_review), 2)             AS avg_review,
            ROUND(AVG(avg_delivery_days), 1)      AS avg_days
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE customer_region IS NOT NULL
          AND YEAR(TO_DATE(order_year_month || '-01')) IN ({years})
        GROUP BY 1, 2, 3
        ORDER BY revenue DESC
    """, label="region")


@st.cache_data(ttl=600, show_spinner=False)
def load_delivery_performance(year_filter: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            customer_state,
            COUNT(DISTINCT order_id)                              AS orders,
            ROUND(AVG(CASE WHEN delivery_days >= 0
                           THEN delivery_days END), 1)           AS avg_days,
            ROUND(100.0 * COUNT_IF(is_late_delivery) /
                  NULLIF(COUNT(*), 0), 1)                        AS late_pct,
            ROUND(AVG(avg_review_score), 2)                      AS avg_score
        FROM OLIST_DW.MARTS.fct_orders
        WHERE {year_filter}
          AND customer_state IS NOT NULL
        GROUP BY 1
        HAVING COUNT(DISTINCT order_id) > 100
        ORDER BY late_pct DESC
        LIMIT 15
    """, label="delivery")


# ══════════════════════════════════════════════════════════════
# 3. LOGIC LAYER — transforms and computations
# ══════════════════════════════════════════════════════════════

def fmt_brl(value: float) -> str:
    if value >= 1_000_000:
        return f"R$ {value/1_000_000:.1f}M"
    if value >= 1_000:
        return f"R$ {value/1_000:.1f}K"
    return f"R$ {value:,.0f}"

def fmt_num(value: float) -> str:
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value/1_000:.1f}K"
    return f"{int(value):,}"

TIER_ICON = {
    "platinum": "🥇 Platinum",
    "gold":     "🥈 Gold",
    "silver":   "🥉 Silver",
    "bronze":   "🔵 Bronze",
}

STATUS_COLOR = {
    "delivered":    "#10b981",
    "shipped":      "#3b82f6",
    "processing":   "#f59e0b",
    "canceled":     "#ef4444",
    "unavailable":  "#9ca3af",
    "invoiced":     "#8b5cf6",
    "approved":     "#14b8a6",
    "created":      "#f97316",
}

def is_data_stale(ts: Optional[datetime], threshold_hours: int = 48) -> bool:
    if ts is None:
        return True
    age = datetime.now(timezone.utc) - ts.replace(tzinfo=timezone.utc)
    return age > timedelta(hours=threshold_hours)


# ══════════════════════════════════════════════════════════════
# 4. UI LAYER — rendering functions
# ══════════════════════════════════════════════════════════════

def render_header(last_refresh: Optional[datetime]):
    col_title, col_meta = st.columns([3, 1])
    with col_title:
        st.markdown("## 🛒 Olist E-commerce Analytics")
        st.caption("AWS Lambda → S3 → Glue ETL → Snowflake → dbt → Streamlit")
    with col_meta:
        if last_refresh:
            age_h = (datetime.now(timezone.utc) - last_refresh.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            if is_data_stale(last_refresh):
                st.markdown(
                    f'<div class="stale-warning">⚠️ Data may be stale<br>'
                    f'Last order: {last_refresh.strftime("%Y-%m-%d")} ({age_h:.0f}h ago)</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.success(f"✅ Fresh data\n{last_refresh.strftime('%Y-%m-%d')}", icon=None)
        else:
            st.warning("⚠️ Cannot determine data freshness")

def render_kpis(df: pd.DataFrame):
    if df.empty:
        st.info("No KPI data for selected filters.")
        return
    r = df.iloc[0]
    c1,c2,c3,c4,c5,c6,c7,c8 = st.columns(8)
    c1.metric("📦 Orders", fmt_num(r["total_orders"]))
    c2.metric("🛍️ Items Sold", fmt_num(r["total_items"]))
    c3.metric("💰 Total GMV", fmt_brl(r["total_gmv"]))
    c4.metric("👥 Customers", fmt_num(r["unique_customers"]))
    c5.metric("🏪 Active Sellers", fmt_num(r["active_sellers"]))
    c6.metric("⭐ Avg Review", f"{r['avg_review']:.2f}", delta="/ 5.0", delta_color="off")
    c7.metric("🚚 Avg Delivery", f"{r['avg_days']:.1f} days")
    c8.metric("⏰ Late Rate", f"{r['late_pct']:.1f}%",
              delta="High" if r['late_pct'] > 10 else "OK",
              delta_color="inverse" if r["late_pct"] > 10 else "off")

def render_revenue_trend(df: pd.DataFrame):
    if df.empty or "order_year_month" not in df.columns or "revenue" not in df.columns:
        st.info("No revenue data for selected filters.")
        return
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=df["order_year_month"], y=df["revenue"], name="Revenue (BRL)",
                         marker_color=COLORS["primary"], opacity=0.85,
                         hovertemplate="<b>%{x}</b><br>Revenue: R$ %{y:,.0f}<extra></extra>"), secondary_y=False)
    fig.add_trace(go.Scatter(x=df["order_year_month"], y=df["orders"], name="Orders",
                             mode="lines+markers", line=dict(color=COLORS["tertiary"], width=2),
                             marker=dict(size=5),
                             hovertemplate="<b>%{x}</b><br>Orders: %{y:,}<extra></extra>"), secondary_y=True)
    fig.update_layout(**CHART_LAYOUT, height=300,
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                      hovermode="x unified")
    fig.update_yaxes(title_text="Revenue (BRL)", tickformat=",.0f", secondary_y=False, gridcolor="rgba(0,0,0,0.05)")
    fig.update_yaxes(title_text="Orders", tickformat=",", secondary_y=True, showgrid=False)
    st.plotly_chart(fig, use_container_width=True)

def render_categories(df: pd.DataFrame):
    if df.empty:
        st.info("No category data.")
        return
    fig = px.bar(df, x="revenue", y="category", orientation="h", color="avg_review",
                 color_continuous_scale="RdYlGn", range_color=[3.0,5.0],
                 labels={"revenue":"Revenue (BRL)","category":"","avg_review":"Avg Review"},
                 hover_data={"orders":True,"avg_review":":.2f","avg_days":True},
                 custom_data=["orders","avg_review","avg_days"])
    fig.update_traces(hovertemplate="<b>%{y}</b><br>Revenue: R$ %{x:,.0f}<br>Orders: %{customdata[0]:,}<br>Avg Review: %{customdata[1]:.2f}<br>Avg Delivery: %{customdata[2]:.1f} days<extra></extra>")
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(**CHART_LAYOUT, height=360, coloraxis_colorbar=dict(title="Review", thickness=10, len=0.6))
    st.plotly_chart(fig, use_container_width=True)

def render_order_status(df: pd.DataFrame):
    if df.empty:
        st.info("No order status data.")
        return
    colors = [STATUS_COLOR.get(s, "#9ca3af") for s in df["order_status"]]
    fig = go.Figure(go.Pie(labels=df["order_status"].str.title(), values=df["cnt"],
                           marker=dict(colors=colors, line=dict(color="white", width=2)),
                           hole=0.5, textinfo="percent",
                           hovertemplate="<b>%{label}</b><br>Orders: %{value:,}<br>Share: %{percent}<extra></extra>"))
    fig.update_layout(**CHART_LAYOUT, height=300, showlegend=True,
                      legend=dict(orientation="v", x=1.0, y=0.5, font=dict(size=10)))
    total = df["cnt"].sum()
    fig.add_annotation(text=f"<b>{fmt_num(total)}</b><br><span style='font-size:10px'>orders</span>",
                       x=0.5, y=0.5, showarrow=False, font=dict(size=14), xref="paper", yref="paper")
    st.plotly_chart(fig, use_container_width=True)

def render_top_sellers(df: pd.DataFrame):
    if df.empty:
        st.info("No seller data.")
        return
    display = df.copy()
    display["tier"] = display["tier"].map(lambda t: TIER_ICON.get(t, t))
    display["gmv"] = display["gmv"].apply(fmt_brl)
    display["late_pct"] = display["late_pct"].apply(lambda x: f"{x:.1f}%")
    display["score"] = display["score"].apply(lambda x: f"⭐ {x:.2f}")
    display = display.rename(columns={"seller_id":"Seller ID","state":"State","tier":"Tier",
                                      "orders":"Orders","gmv":"GMV","score":"Review",
                                      "avg_days":"Avg Days","late_pct":"Late %"})
    cols = ["Seller ID","State","Tier","Orders","GMV","Review","Avg Days","Late %"]
    st.dataframe(display[cols], use_container_width=True, height=400, hide_index=True)

def render_region_bars(df: pd.DataFrame):
    if df.empty:
        st.info("No region data.")
        return
    region_agg = df.groupby("region").agg(revenue=("revenue","sum"), orders=("orders","sum")).reset_index().sort_values("revenue", ascending=False)
    fig = px.bar(region_agg, x="region", y="revenue", color="region", color_discrete_sequence=PALETTE,
                 labels={"revenue":"Revenue (BRL)","region":"Region"}, hover_data={"orders":True},
                 custom_data=["orders"])
    fig.update_traces(hovertemplate="<b>%{x}</b><br>Revenue: R$ %{y:,.0f}<br>Orders: %{customdata[0]:,}<extra></extra>")
    fig.update_layout(**CHART_LAYOUT, height=260, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

def render_state_table(df: pd.DataFrame):
    if df.empty:
        return
    display = df[["state_name","state","region","revenue","orders","avg_review","avg_days"]].copy()
    display["revenue"] = display["revenue"].apply(fmt_brl)
    display = display.rename(columns={"state_name":"State","state":"Code","region":"Region",
                                      "revenue":"Revenue","orders":"Orders","avg_review":"Avg Review","avg_days":"Avg Days"})
    st.dataframe(display, use_container_width=True, height=280, hide_index=True)

def render_delivery(df: pd.DataFrame):
    if df.empty:
        st.info("No delivery data.")
        return
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Late Delivery Rate by State (%)", "Avg Delivery Days by State"),
                        horizontal_spacing=0.12)
    top_late = df.sort_values("late_pct", ascending=True).tail(10)
    fig.add_trace(go.Bar(x=top_late["late_pct"], y=top_late["customer_state"], orientation="h",
                         marker_color=[COLORS["danger"] if x>15 else COLORS["warning"] if x>8 else COLORS["success"] for x in top_late["late_pct"]],
                         hovertemplate="<b>%{y}</b>: %{x:.1f}%<extra></extra>", name="Late %"), row=1, col=1)
    top_days = df.sort_values("avg_days", ascending=True).tail(10)
    fig.add_trace(go.Bar(x=top_days["avg_days"], y=top_days["customer_state"], orientation="h",
                         marker_color=COLORS["tertiary"], opacity=0.8,
                         hovertemplate="<b>%{y}</b>: %{x:.1f} days<extra></extra>", name="Avg Days"), row=1, col=2)
    fig.update_layout(**CHART_LAYOUT, height=300, showlegend=False)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    st.plotly_chart(fig, use_container_width=True)

def download_button(df: pd.DataFrame, filename: str, label: str = "⬇️ Download CSV"):
    if df.empty:
        return
    st.download_button(label=label, data=df.to_csv(index=False).encode("utf-8"), file_name=filename, mime="text/csv", use_container_width=True)


# ══════════════════════════════════════════════════════════════
# 5. SIDEBAR
# ══════════════════════════════════════════════════════════════

def render_sidebar() -> dict:
    with st.sidebar:
        st.image("https://olist.com/wp-content/uploads/2021/08/logo-olist.png", width=120)
        st.markdown("### Filters")
        st.divider()
        selected_years = st.multiselect("Order Year", options=[2016,2017,2018], default=[2017,2018],
                                        help="Filter orders by purchase year")
        if not selected_years:
            selected_years = [2017,2018]
        top_n = st.slider("Top N categories", 5, 20, 10, step=1)
        st.divider()
        st.markdown("### Display")
        show_raw = st.checkbox("Show raw data explorer", value=False)
        st.divider()
        if st.button("🔄 Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()
        st.divider()
        st.caption("Pipeline: AWS Glue → Snowflake → dbt")
        st.caption("Data: Olist Brazilian E-commerce")

    years_str = ",".join(str(y) for y in selected_years)
    year_filter = f"YEAR(order_purchase_timestamp) IN ({years_str})"
    return {"selected_years": selected_years, "year_filter": year_filter,
            "years_str": years_str, "top_n": top_n, "show_raw": show_raw}


# ══════════════════════════════════════════════════════════════
# 6. MAIN APP
# ══════════════════════════════════════════════════════════════

def main():
    filters = render_sidebar()
    with st.spinner("Checking data freshness…"):
        last_refresh = get_data_freshness()
    render_header(last_refresh)
    st.divider()

    with st.spinner("Loading KPIs…"):
        kpi_df = load_kpis(filters["year_filter"])
    render_kpis(kpi_df)
    st.divider()

    tab_revenue, tab_categories, tab_geo, tab_sellers, tab_delivery = st.tabs([
        "📈 Revenue", "🏆 Categories", "🗺️ Geography", "🥇 Sellers", "🚚 Delivery"
    ])

    with tab_revenue:
        col_l, col_r = st.columns([2,1])
        with col_l:
            st.markdown('<p class="section-header">Monthly Revenue & Order Volume</p>', unsafe_allow_html=True)
            rev_df = load_monthly_revenue(filters["year_filter"])
            render_revenue_trend(rev_df)
            download_button(rev_df, "monthly_revenue.csv", "⬇️ Monthly revenue CSV")
        with col_r:
            st.markdown('<p class="section-header">Order Status Breakdown</p>', unsafe_allow_html=True)
            status_df = load_order_status(filters["year_filter"])
            render_order_status(status_df)
        if not rev_df.empty:
            st.markdown('<p class="section-header">Monthly Summary</p>', unsafe_allow_html=True)
            summary = rev_df.copy()
            summary["revenue"] = summary["revenue"].apply(fmt_brl)
            summary["orders"] = summary["orders"].apply(fmt_num)
            summary = summary.rename(columns={"order_year_month":"Month","revenue":"Revenue","orders":"Orders","items":"Items","avg_review":"Avg Review"})
            st.dataframe(summary, use_container_width=True, hide_index=True, height=220)

    with tab_categories:
        st.markdown(f'<p class="section-header">Top {filters["top_n"]} Categories by Revenue</p>', unsafe_allow_html=True)
        st.caption("Color = average review score (green = higher, red = lower)")
        cat_df = load_category_revenue(filters["year_filter"], filters["top_n"])
        render_categories(cat_df)
        download_button(cat_df, "top_categories.csv")

    with tab_geo:
        region_df = load_region_data(filters["year_filter"])
        col_geo_l, col_geo_r = st.columns([1,2])
        with col_geo_l:
            st.markdown('<p class="section-header">Revenue by Region</p>', unsafe_allow_html=True)
            render_region_bars(region_df)
        with col_geo_r:
            st.markdown('<p class="section-header">State Breakdown</p>', unsafe_allow_html=True)
            render_state_table(region_df)
        download_button(region_df, "geography.csv")

    with tab_sellers:
        st.markdown('<p class="section-header">Top 20 Sellers by GMV</p>', unsafe_allow_html=True)
        st.caption("Tier: 🥇 Platinum ≥ R$50K · 🥈 Gold ≥ R$10K · 🥉 Silver ≥ R$1K · 🔵 Bronze")
        sellers_df = load_top_sellers(filters["year_filter"])
        tiers = ["All"] + sorted(sellers_df["tier"].dropna().unique().tolist())
        sel_tier = st.selectbox("Filter by tier", options=tiers, index=0)
        if sel_tier != "All":
            sellers_df = sellers_df[sellers_df["tier"] == sel_tier]
        render_top_sellers(sellers_df)
        download_button(sellers_df, "top_sellers.csv")

    with tab_delivery:
        st.markdown('<p class="section-header">Delivery Performance by State</p>', unsafe_allow_html=True)
        st.caption("States with > 100 orders. Red = late rate > 15%, amber = > 8%.")
        delivery_df = load_delivery_performance(filters["year_filter"])
        render_delivery(delivery_df)
        download_button(delivery_df, "delivery_performance.csv")

    if filters["show_raw"]:
        st.divider()
        st.markdown("### 🔍 Raw Data Explorer")
        tbl = st.selectbox("Select table", ["fct_orders","fct_monthly_revenue","dim_customers","dim_products","dim_sellers"])
        limit = st.slider("Rows to show", 50, 1000, 200, step=50)
        raw_df = run_query(f"SELECT * FROM OLIST_DW.MARTS.{tbl} LIMIT {limit}", label=f"raw_{tbl}")
        if not raw_df.empty:
            st.dataframe(raw_df, use_container_width=True)
            download_button(raw_df, f"{tbl}.csv", f"⬇️ Download {tbl}.csv")

    st.divider()
    st.caption(f"© Olist Analytics · AWS Lambda → S3 → Glue ETL → Snowflake → dbt Core → Streamlit · Dashboard refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

if __name__ == "__main__":
    main()