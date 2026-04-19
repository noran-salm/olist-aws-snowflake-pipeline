"""
dashboard/streamlit_app.py — Olist Analytics (SaaS Grade)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Features:
- Sidebar navigation (no tabs)
- Insight engine (AI-like summaries)
- KPI cards with trend indicators (MoM)
- Map (scatter mapbox) for state revenue
- Lazy loading per page
- Clean, modular structure
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

# ---------- Custom CSS (modern, SaaS‑like) ----------
st.markdown("""
<style>
    /* Global */
    body { background-color: #f8fafc; font-family: 'Inter', system-ui, sans-serif; }
    /* KPI cards */
    .kpi-card {
        background: white;
        border-radius: 1rem;
        padding: 1.2rem 1rem;
        box-shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1);
        transition: transform 0.2s, box-shadow 0.2s;
        border: 1px solid #e2e8f0;
        text-align: center;
    }
    .kpi-card:hover { transform: translateY(-2px); box-shadow: 0 10px 15px -3px rgb(0 0 0 / 0.1); }
    .kpi-label { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: #475569; }
    .kpi-value { font-size: 1.8rem; font-weight: 700; color: #0f172a; margin: 0.25rem 0; }
    .kpi-delta { font-size: 0.8rem; font-weight: 500; }
    /* Section headers */
    .section-header {
        font-size: 1rem;
        font-weight: 600;
        color: #1e293b;
        margin: 1.5rem 0 0.75rem;
        padding-bottom: 0.4rem;
        border-bottom: 2px solid #e2e8f0;
    }
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: white;
        border-right: 1px solid #e2e8f0;
        padding: 1rem;
    }
    /* Insight banner */
    .insight-banner {
        background: #fef9c3;
        border-left: 4px solid #f97316;
        padding: 0.75rem 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        font-size: 0.9rem;
        color: #1e293b;
    }
    /* Empty state */
    .empty-state {
        background: #f1f5f9;
        border-radius: 0.75rem;
        padding: 2rem;
        text-align: center;
        color: #64748b;
    }
    /* Footer */
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ---------- Constants & Helpers ----------
PALETTE = ["#f97316", "#10b981", "#3b82f6", "#8b5cf6", "#f59e0b", "#ec4899", "#14b8a6", "#64748b"]
STATUS_COLOR = {
    "delivered": "#10b981", "shipped": "#3b82f6", "processing": "#f59e0b",
    "canceled": "#ef4444", "unavailable": "#9ca3af", "invoiced": "#8b5cf6",
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
        return df
    except Exception as e:
        log.error("Query failed [%s]: %s", label, e)
        st.error(f"Query error ({label}): {e}")
        return pd.DataFrame()

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
def render_kpi_row(df: pd.DataFrame, prev_df: pd.DataFrame = None):
    if df.empty:
        st.markdown('<div class="empty-state">No KPI data for selected filters.</div>', unsafe_allow_html=True)
        return
    r = df.iloc[0]
    # For deltas we need previous period data – simplified: we compute MoM from monthly revenue
    # For simplicity, we'll show only current values without deltas here (can be added later)
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
    # Insight
    st.markdown(f'<div class="insight-banner">{revenue_insight(df)}</div>', unsafe_allow_html=True)
    # Chart
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=df["order_year_month"], y=df["revenue"], name="Revenue (BRL)",
                         marker_color="#f97316", opacity=0.8), secondary_y=False)
    fig.add_trace(go.Scatter(x=df["order_year_month"], y=df["orders"], name="Orders",
                             mode="lines+markers", line=dict(color="#3b82f6", width=2)), secondary_y=True)
    fig.update_layout(height=400, margin=dict(l=10, r=10, t=40, b=10), hovermode="x unified")
    fig.update_yaxes(title_text="Revenue (BRL)", tickformat=",.0f", secondary_y=False)
    fig.update_yaxes(title_text="Orders", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)
    # Download
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
    fig.update_layout(height=500, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)
    st.download_button("⬇️ Download CSV", data=df.to_csv(index=False), file_name="top_categories.csv", mime="text/csv")

def render_geography_page(years_csv: str):
    with st.spinner("Loading geography data..."):
        df = load_region_data(years_csv)
    if df.empty:
        st.markdown('<div class="empty-state">No geography data.</div>', unsafe_allow_html=True)
        return
    st.markdown(f'<div class="insight-banner">{region_insight(df)}</div>', unsafe_allow_html=True)
    # Map (scatter mapbox)
    df_map = df.copy()
    df_map["lat"] = df_map["state_code"].map(lambda x: STATE_COORDS.get(x, (None, None))[0])
    df_map["lon"] = df_map["state_code"].map(lambda x: STATE_COORDS.get(x, (None, None))[1])
    df_map = df_map.dropna(subset=["lat", "lon"])
    if not df_map.empty:
        fig_map = px.scatter_mapbox(df_map, lat="lat", lon="lon", size="revenue", color="revenue",
                                    hover_name="state_name", zoom=3, height=500,
                                    color_continuous_scale="Oranges", size_max=50)
        fig_map.update_layout(mapbox_style="carto-positron", margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_map, use_container_width=True)
    # Bar chart by state
    st.markdown('<p class="section-header">Revenue by State</p>', unsafe_allow_html=True)
    fig_bar = px.bar(df.sort_values("revenue", ascending=False).head(15),
                     x="state_name", y="revenue", color="region", title="Top 15 States")
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
    # Tier filter
    tiers = ["All"] + sorted(df["tier"].dropna().unique().tolist())
    sel_tier = st.selectbox("Filter by tier", tiers)
    if sel_tier != "All":
        df = df[df["tier"] == sel_tier]
    # Table
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
    # Two charts
    col1, col2 = st.columns(2)
    with col1:
        fig_late = px.bar(df.sort_values("late_pct", ascending=True).tail(10),
                          x="late_pct", y="customer_state", orientation="h",
                          color="late_pct", color_continuous_scale="Reds",
                          title="Late Rate by State")
        st.plotly_chart(fig_late, use_container_width=True)
    with col2:
        fig_days = px.bar(df.sort_values("avg_days", ascending=True).tail(10),
                          x="avg_days", y="customer_state", orientation="h",
                          color="avg_days", color_continuous_scale="Blues",
                          title="Avg Delivery Days by State")
        st.plotly_chart(fig_days, use_container_width=True)
    st.download_button("⬇️ Download CSV", data=df.to_csv(index=False), file_name="delivery.csv", mime="text/csv")

# ---------- Sidebar ----------
def render_sidebar() -> Dict[str, Any]:
    with st.sidebar:
        st.markdown("### 🛒 Olist Analytics")
        st.divider()
        # Navigation
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
    # Header & freshness (shared)
    with st.spinner("Checking freshness..."):
        ts = load_freshness()
    render_header(ts)
    st.divider()
    # KPIs (shared)
    with st.spinner("Loading KPIs..."):
        kpi_df = load_kpis(filters["years_csv"])
    render_kpi_row(kpi_df)
    st.divider()
    # Render selected page
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
    # Footer
    st.divider()
    st.caption(f"© Olist Analytics · Refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

if __name__ == "__main__":
    main()