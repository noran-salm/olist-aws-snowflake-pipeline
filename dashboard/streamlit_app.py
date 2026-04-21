"""
dashboard/streamlit_app.py — Olist Analytics (Dark Mode, SaaS Grade)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

"""
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import streamlit as st

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

# ---------- Custom CSS (Dark Mode, Modern SaaS) ----------
st.markdown("""
<style>
    /* Global dark theme */
    html, body, [data-testid="stAppViewContainer"] {
        background-color: #0f172a;
        color: #e2e8f0;
        font-family: 'Inter', system-ui, -apple-system, sans-serif;
    }
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #1e293b;
        border-right: 1px solid #334155;
    }
    [data-testid="stSidebar"] .stMarkdown, 
    [data-testid="stSidebar"] .stRadio label {
        color: #cbd5e1;
    }
    /* Metric cards */
    .kpi-card {
        background: #1e293b;
        border-radius: 1rem;
        padding: 1.2rem 1rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
        transition: transform 0.2s, box-shadow 0.2s;
        border: 1px solid #334155;
        text-align: center;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.4);
        border-color: #f97316;
    }
    .kpi-label {
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #94a3b8;
    }
    .kpi-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #f1f5f9;
        margin: 0.25rem 0;
    }
    /* Section headers */
    .section-header {
        font-size: 1rem;
        font-weight: 600;
        color: #f1f5f9;
        margin: 1.5rem 0 0.75rem;
        padding-bottom: 0.4rem;
        border-bottom: 2px solid #334155;
    }
    /* Insight banner */
    .insight-banner {
        background: #2d3a4e;
        border-left: 4px solid #f97316;
        padding: 0.75rem 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        font-size: 0.9rem;
        color: #e2e8f0;
    }
    /* Empty state */
    .empty-state {
        background: #1e293b;
        border-radius: 0.75rem;
        padding: 2rem;
        text-align: center;
        color: #94a3b8;
        border: 1px dashed #475569;
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
        background-color: #334155;
        color: #e2e8f0;
        border: 1px solid #475569;
    }
    .stDownloadButton button:hover {
        background-color: #475569;
    }
    /* Dataframe tables */
    .stDataFrame {
        background-color: #1e293b;
        border-radius: 0.75rem;
        overflow: hidden;
        border: 1px solid #334155;
    }
    /* Select boxes, multiselect */
    .stSelectbox div[data-baseweb="select"], 
    .stMultiSelect div[data-baseweb="select"] {
        background-color: #1e293b;
        border-color: #475569;
    }
    /* Radio buttons */
    .stRadio label {
        color: #cbd5e1;
    }
    /* Footer */
    footer {
        visibility: hidden;
    }
    /* Captions */
    .stCaption {
        color: #94a3b8;
    }
    /* Success/Warning/Info */
    .stAlert {
        background-color: #2d3a4e;
        border: 1px solid #475569;
    }
    .stSuccess {
        color: #10b981;
    }
    .stWarning {
        color: #f59e0b;
    }
</style>
""", unsafe_allow_html=True)

# ---------- Constants & Helpers ----------
# Dark theme palette
COLORS = {
    "primary": "#f97316",    # orange
    "secondary": "#10b981",  # green
    "tertiary": "#3b82f6",   # blue
    "warning": "#f59e0b",    # amber
    "danger": "#ef4444",      # red
    "gray": {
        300: "#cbd5e1",
        500: "#64748b",
        700: "#334155",
        800: "#1e293b",
        900: "#0f172a",
    }
}
PALETTE = ["#f97316", "#10b981", "#3b82f6", "#8b5cf6", "#f59e0b", "#ec4899", "#14b8a6", "#64748b"]
STATUS_COLOR = {
    "delivered": "#10b981", "shipped": "#3b82f6", "processing": "#f59e0b",
    "canceled": "#ef4444", "unavailable": "#64748b", "invoiced": "#8b5cf6",
    "approved": "#14b8a6", "created": "#f97316",
}
TIER_ICON = {"platinum": "🥇 Platinum", "gold": "🥈 Gold", "silver": "🥉 Silver", "bronze": "🔵 Bronze"}

# State coordinates for map (centroids)
STATE_COORDS = {
    "AC": (-9.0238, -70.8110), "AL": (-9.5713, -36.7819), "AP": (1.9989, -50.9476),
    "AM": (-3.4653, -62.2159), "BA": (-12.5797, -41.7007), "CE": (-5.4984, -39.3206),
    "DF": (-15.7998, -47.8645), "ES": (-19.1834, -40.3089), "GO": (-15.8270, -49.8362),
    "MA": (-5.4230, -45.8885), "MT": (-12.6819, -56.9211), "MS": (-20.7722, -54.7852),
    "MG": (-18.5122, -44.5550), "PA": (-5.2725, -52.4359), "PB": (-7.2765, -36.9551),
    "PR": (-24.9530, -51.5350), "PE": (-8.2853, -35.9697), "PI": (-6.2887, -43.1895),
    "RJ": (-22.9068, -43.1729), "RN": (-5.4026, -36.9541), "RS": (-30.0346, -51.2177),
    "RO": (-10.8271, -63.0326), "RR": (1.8892, -61.3620), "SC": (-27.2423, -50.2189),
    "SP": (-23.5505, -46.6333), "SE": (-10.5741, -37.3857), "TO": (-10.1840, -48.3338),
}

def fmt_brl(v: float) -> str:
    if pd.isna(v): return "R$ 0"
    if v >= 1_000_000: return f"R$ {v/1_000_000:.1f}M"
    if v >= 1_000: return f"R$ {v/1_000:.1f}K"
    return f"R$ {v:,.0f}"

def fmt_num(v: float) -> str:
    if pd.isna(v): return "0"
    if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if v >= 1_000: return f"{v/1_000:.1f}K"
    return f"{int(v):,}"

def years_to_sql(years: List[int]) -> str:
    return ",".join(str(y) for y in years)

def compute_delta(current: float, previous: float) -> Optional[float]:
    if previous == 0 or pd.isna(previous): return None
    return ((current - previous) / previous) * 100

# ---------- Plotly dark theme helper ----------
def apply_dark_theme(fig):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=12, color="#e2e8f0"),
        hoverlabel=dict(bgcolor="#1e293b", font_size=11, font_color="#f1f5f9"),
        xaxis=dict(showgrid=False, zeroline=False, tickangle=-30),
        yaxis=dict(showgrid=True, gridcolor="#334155", zeroline=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=10, r=10, t=40, b=10),
    )
    return fig

# ---------- Credentials & Connection ----------
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
    raise RuntimeError("No Snowflake credentials found.")

# def get_connection():
#     creds = _get_credentials()
#     return snowflake.connector.connect(
#         account=creds["account"], user=creds["user"], password=creds["password"],
#         role=creds.get("role", "SYSADMIN"), database=creds.get("database", "OLIST_DW"),
#         schema=creds.get("schema", "MARTS"), warehouse=creds.get("warehouse", "OLIST_WH"),
#         session_parameters={"QUERY_TAG": "streamlit-dashboard"},
#     )

def get_connection():
    creds = _get_credentials()
    return snowflake.connector.connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        role=creds.get("role", "SYSADMIN"),
        database=creds.get("database"),
        schema=creds.get("schema"),
        warehouse=creds.get("warehouse"),
        client_session_keep_alive=True
    )

def run_query(sql: str, label: str = "") -> pd.DataFrame:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        df.columns = [c.lower() for c in df.columns]
        return df

    except Exception as e:
        if "390114" in str(e):
            log.warning("Session expired. Reconnecting...")
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(sql)
            df = cur.fetch_pandas_all()
            df.columns = [c.lower() for c in df.columns]
            return df

        log.error("Query failed [%s]: %s", label, e)
        st.error(f"Query error ({label}): {e}")
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()

# ---------- Data Loading (cached, per page) ----------
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
        SELECT order_year_month,
               SUM(revenue_brl) AS revenue,
               SUM(total_orders) AS orders,
               AVG(avg_review) AS avg_review
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE YEAR(TO_DATE(order_year_month || '-01')) IN ({years_csv})
        GROUP BY 1 ORDER BY 1
    """, label="monthly_revenue")

@st.cache_data(ttl=600, show_spinner=False)
def load_category_revenue(years_csv: str, top_n: int) -> pd.DataFrame:
    return run_query(f"""
        SELECT COALESCE(product_category,'unknown') AS category,
               ROUND(SUM(revenue_brl),0) AS revenue,
               SUM(total_orders) AS orders,
               ROUND(AVG(avg_review),2) AS avg_review
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE YEAR(TO_DATE(order_year_month || '-01')) IN ({years_csv})
        GROUP BY 1 ORDER BY revenue DESC LIMIT {top_n}
    """, label="categories")

@st.cache_data(ttl=600, show_spinner=False)
def load_order_status(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT order_status, COUNT(DISTINCT order_id) AS cnt
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
        SELECT customer_state AS state_code,
               customer_state_name AS state_name,
               customer_region AS region,
               ROUND(SUM(revenue_brl),0) AS revenue,
               SUM(total_orders) AS orders
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE customer_region IS NOT NULL
          AND YEAR(TO_DATE(order_year_month || '-01')) IN ({years_csv})
        GROUP BY 1,2,3
        ORDER BY revenue DESC
    """, label="region")

@st.cache_data(ttl=600, show_spinner=False)
def load_delivery(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT customer_state,
               COUNT(DISTINCT order_id) AS orders,
               ROUND(AVG(CASE WHEN delivery_days>=0 THEN delivery_days END),1) AS avg_days,
               ROUND(100.0 * SUM(is_late_delivery::INTEGER) / NULLIF(COUNT(*),0),1) AS late_pct
        FROM OLIST_DW.MARTS.fct_orders
        WHERE YEAR(order_purchase_timestamp) IN ({years_csv})
          AND customer_state IS NOT NULL
        GROUP BY 1 HAVING COUNT(DISTINCT order_id) > 100
        ORDER BY late_pct DESC
    """, label="delivery")

# ---------- Insight Engine ----------
def revenue_insight(df: pd.DataFrame) -> str:
    if len(df) < 2:
        return "Not enough data for trend."
    prev = df["revenue"].iloc[-2]
    curr = df["revenue"].iloc[-1]
    if prev == 0:
        return "Revenue trend cannot be calculated."
    change = ((curr - prev) / prev) * 100
    if change > 5:
        return f"📈 Revenue grew by {change:.1f}% compared to the previous month."
    elif change < -5:
        return f"📉 Revenue declined by {abs(change):.1f}% compared to the previous month."
    else:
        return f"➡️ Revenue is stable (Δ {change:+.1f}%)."

def top_category_insight(df: pd.DataFrame) -> str:
    if df.empty:
        return "No category data available."
    top = df.iloc[0]
    return f"🏆 Top category: **{top['category']}** with {fmt_brl(top['revenue'])}."

def region_insight(df: pd.DataFrame) -> str:
    if df.empty:
        return "No region data."
    top_region = df.groupby("region")["revenue"].sum().idxmax()
    return f"📍 Best performing region: **{top_region}**."

def delivery_insight(df: pd.DataFrame) -> str:
    if df.empty:
        return "No delivery data."
    worst = df.iloc[0]
    return f"⚠️ Highest late rate: **{worst['customer_state']}** ({worst['late_pct']:.1f}% late)."

# ---------- UI Components ----------
def render_kpi_row(df: pd.DataFrame):
    if df.empty:
        st.markdown('<div class="empty-state">No KPI data for selected filters.</div>', unsafe_allow_html=True)
        return
    r = df.iloc[0]
    cols = st.columns(4)
    metrics = [
        ("Total Orders", fmt_num(r["total_orders"])),
        ("GMV", fmt_brl(r["total_gmv"])),
        ("Avg Review", f"{r['avg_review']:.2f} / 5"),
        ("Late Rate", f"{r['late_pct']:.1f}%"),
    ]
    for col, (label, value) in zip(cols, metrics):
        with col:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value">{value}</div>
            </div>
            """, unsafe_allow_html=True)

def render_header(ts: Optional[datetime]):
    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown("## 🛒 Olist E-commerce Analytics")
        st.caption("AWS → Snowflake → dbt → Streamlit")
    with c2:
        if ts:
            st.success(f"✅ Data as of {ts.strftime('%b %d, %Y')}")
        else:
            st.warning("⚠️ Freshness unknown")

def render_revenue_page(years_csv: str):
    with st.spinner("Loading revenue data..."):
        df = load_monthly_revenue(years_csv)
    if df.empty:
        st.markdown('<div class="empty-state">No revenue data for selected years.</div>', unsafe_allow_html=True)
        return
    st.markdown(f'<div class="insight-banner">{revenue_insight(df)}</div>', unsafe_allow_html=True)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=df["order_year_month"], y=df["revenue"], name="Revenue (BRL)",
                         marker_color=COLORS["primary"], opacity=0.8), secondary_y=False)
    fig.add_trace(go.Scatter(x=df["order_year_month"], y=df["orders"], name="Orders",
                             mode="lines+markers", line=dict(color=COLORS["tertiary"], width=2)), secondary_y=True)
    fig = apply_dark_theme(fig)
    fig.update_layout(height=400)
    fig.update_yaxes(title_text="Revenue (BRL)", tickformat=",.0f", secondary_y=False)
    fig.update_yaxes(title_text="Orders", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)
    st.download_button("⬇️ Download CSV", data=df.to_csv(index=False), file_name="monthly_revenue.csv", mime="text/csv")

def render_categories_page(years_csv: str, top_n: int):
    with st.spinner("Loading categories..."):
        df = load_category_revenue(years_csv, top_n)
    if df.empty:
        st.markdown('<div class="empty-state">No category data.</div>', unsafe_allow_html=True)
        return
    st.markdown(f'<div class="insight-banner">{top_category_insight(df)}</div>', unsafe_allow_html=True)
    fig = px.bar(df, x="revenue", y="category", orientation="h", color="avg_review",
                 color_continuous_scale="RdYlGn", range_color=[3,5],
                 labels={"revenue":"Revenue (BRL)","category":"","avg_review":"Avg Review"})
    fig = apply_dark_theme(fig)
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    st.download_button("⬇️ Download CSV", data=df.to_csv(index=False), file_name="top_categories.csv", mime="text/csv")

def render_geography_page(years_csv: str):
    with st.spinner("Loading geography data..."):
        df = load_region_data(years_csv)
    if df.empty:
        st.markdown('<div class="empty-state">No geography data.</div>', unsafe_allow_html=True)
        return
    st.markdown(f'<div class="insight-banner">{region_insight(df)}</div>', unsafe_allow_html=True)
    # Map
    df_map = df.copy()
    df_map["lat"] = df_map["state_code"].map(lambda x: STATE_COORDS.get(x, (None, None))[0])
    df_map["lon"] = df_map["state_code"].map(lambda x: STATE_COORDS.get(x, (None, None))[1])
    df_map = df_map.dropna(subset=["lat", "lon"])
    if not df_map.empty:
        fig_map = px.scatter_mapbox(df_map, lat="lat", lon="lon", size="revenue", color="revenue",
                                    hover_name="state_name", zoom=3, height=500,
                                    color_continuous_scale="Oranges", size_max=50)
        fig_map.update_layout(mapbox_style="carto-darkmatter", margin=dict(l=0, r=0, t=30, b=0))
        fig_map = apply_dark_theme(fig_map)
        st.plotly_chart(fig_map, use_container_width=True)
    # Bar chart
    st.markdown('<p class="section-header">Revenue by State</p>', unsafe_allow_html=True)
    fig_bar = px.bar(df.sort_values("revenue", ascending=False).head(15),
                     x="state_name", y="revenue", color="region", title="Top 15 States")
    fig_bar = apply_dark_theme(fig_bar)
    fig_bar.update_layout(xaxis_tickangle=-45, height=400)
    st.plotly_chart(fig_bar, use_container_width=True)
    st.download_button("⬇️ Download CSV", data=df.to_csv(index=False), file_name="geography.csv", mime="text/csv")

def render_sellers_page(years_csv: str):
    with st.spinner("Loading sellers..."):
        df = load_top_sellers(years_csv)
    if df.empty:
        st.markdown('<div class="empty-state">No seller data.</div>', unsafe_allow_html=True)
        return
    st.markdown(f'<div class="insight-banner">🥇 Top seller: **{df.iloc[0]["seller_id"]}** with {fmt_brl(df.iloc[0]["gmv"])}</div>', unsafe_allow_html=True)
    tiers = ["All"] + sorted(df["tier"].dropna().unique().tolist())
    sel_tier = st.selectbox("Filter by tier", tiers)
    if sel_tier != "All":
        df = df[df["tier"] == sel_tier]
    display = df.copy()
    display["tier"] = display["tier"].map(lambda t: TIER_ICON.get(t, t))
    display["gmv"] = display["gmv"].apply(fmt_brl)
    display["late_pct"] = display["late_pct"].apply(lambda x: f"{x:.1f}%")
    display["score"] = display["score"].apply(lambda x: f"⭐ {x:.2f}")
    display = display.rename(columns={
        "seller_id": "Seller ID", "state": "State", "tier": "Tier", "orders": "Orders",
        "gmv": "GMV", "score": "Review", "avg_days": "Avg Days", "late_pct": "Late %"
    })
    st.dataframe(display, use_container_width=True, height=500, hide_index=True)
    st.download_button("⬇️ Download CSV", data=df.to_csv(index=False), file_name="top_sellers.csv", mime="text/csv")

def render_delivery_page(years_csv: str):
    with st.spinner("Loading delivery data..."):
        df = load_delivery(years_csv)
    if df.empty:
        st.markdown('<div class="empty-state">No delivery data.</div>', unsafe_allow_html=True)
        return

    st.markdown(f'<div class="insight-banner">{delivery_insight(df)}</div>', unsafe_allow_html=True)

    # Create two subplots side by side
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Late Rate by State", "Avg Delivery Days by State"),
        horizontal_spacing=0.12,
        shared_yaxes=True,
    )

    # Late rate chart (top 10 highest late rates)
    late_df = df.sort_values("late_pct", ascending=True).tail(10)
    fig.add_trace(
        go.Bar(
            x=late_df["late_pct"],
            y=late_df["customer_state"],
            orientation="h",
            marker=dict(color=late_df["late_pct"], colorscale="Reds", showscale=True, colorbar=dict(title="Late %", x=0.45)),
            hovertemplate="<b>%{y}</b>: %{x:.1f}%<extra></extra>",
            name="Late Rate",
        ),
        row=1, col=1
    )

    # Avg days chart (top 10 highest avg days)
    days_df = df.sort_values("avg_days", ascending=True).tail(10)
    fig.add_trace(
        go.Bar(
            x=days_df["avg_days"],
            y=days_df["customer_state"],
            orientation="h",
            marker=dict(color=days_df["avg_days"], colorscale="Blues", showscale=True, colorbar=dict(title="Days", x=1.0)),
            hovertemplate="<b>%{y}</b>: %{x:.1f} days<extra></extra>",
            name="Avg Days",
        ),
        row=1, col=2
    )

    # Update layout for dark theme
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=12, color="#e2e8f0"),
        hoverlabel=dict(bgcolor="#1e293b", font_size=11, font_color="#f1f5f9"),
        margin=dict(l=10, r=10, t=60, b=10),
        height=500,
        showlegend=False,
        title=None,
    )
    # Ensure y-axes are visible and share labels
    fig.update_yaxes(title_text="", tickfont=dict(size=10))
    fig.update_xaxes(title_text="Late Rate (%)", row=1, col=1, gridcolor="#334155")
    fig.update_xaxes(title_text="Avg Delivery Days", row=1, col=2, gridcolor="#334155")

    st.plotly_chart(fig, use_container_width=True)
    st.download_button("⬇️ Download CSV", data=df.to_csv(index=False), file_name="delivery.csv", mime="text/csv")
# ---------- Sidebar ----------
def render_sidebar() -> Dict[str, Any]:
    with st.sidebar:
        st.markdown("### 🛒 Olist Analytics")
        st.divider()
        page = st.radio("Navigation", [
            "📈 Revenue", "🏆 Categories", "🗺️ Geography", "🥇 Sellers", "🚚 Delivery"
        ], index=0)
        st.divider()
        st.markdown("**Filters**")
        years = st.multiselect("Order Year", [2016,2017,2018], default=[2017,2018])
        if not years:
            years = [2017,2018]
        top_n = st.slider("Top N categories", 5, 20, 10) if "Categories" in page else 10
        if st.button("🔄 Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()
        st.divider()
        st.caption("**Pipeline**\nAWS Lambda → S3 → Glue → Snowflake → dbt")
        st.caption("**Data**\nOlist Brazilian E-commerce (2016–2018)")
    return {
        "page": page,
        "years": years,
        "years_csv": years_to_sql(years),
        "top_n": top_n,
    }

# ---------- Main ----------
def main():
    filters = render_sidebar()
    with st.spinner("Checking freshness..."):
        ts = load_freshness()
    render_header(ts)
    st.divider()
    with st.spinner("Loading KPIs..."):
        kpi_df = load_kpis(filters["years_csv"])
    render_kpi_row(kpi_df)
    st.divider()
    if filters["page"] == "📈 Revenue":
        render_revenue_page(filters["years_csv"])
    elif filters["page"] == "🏆 Categories":
        render_categories_page(filters["years_csv"], filters["top_n"])
    elif filters["page"] == "🗺️ Geography":
        render_geography_page(filters["years_csv"])
    elif filters["page"] == "🥇 Sellers":
        render_sellers_page(filters["years_csv"])
    elif filters["page"] == "🚚 Delivery":
        render_delivery_page(filters["years_csv"])
    st.divider()
    st.caption(f"© Olist Analytics · Refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

if __name__ == "__main__":
    main()