"""
dashboard/streamlit_app.py — Production Dashboard v2.1
Fixed: is_late_delivery cast, year filter IN clause, Plotly layout dict merge
"""
import os, json, logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import streamlit as st

log = logging.getLogger("olist-dashboard")
logging.basicConfig(level=logging.INFO)

st.set_page_config(
    page_title="Olist Analytics", page_icon="🛒", layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"About": "Olist E-commerce Analytics — AWS + Snowflake + dbt pipeline"},
)

st.markdown("""
<style>
div[data-testid="metric-container"] {
    background: rgba(0,0,0,0.03);
    border: 1px solid rgba(0,0,0,0.08);
    border-radius: 12px;
    padding: 16px 20px;
}
div[data-testid="metric-container"] > label {
    font-size: 0.75rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #6b7280 !important;
}
.section-header {
    font-size: 0.95rem;
    font-weight: 600;
    color: #374151;
    margin: 1.2rem 0 0.5rem;
    padding-bottom: 0.3rem;
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
</style>
""", unsafe_allow_html=True)

COLORS = {
    "primary": "#FF6B35", "secondary": "#1D9E75",
    "tertiary": "#3B8BD4", "success": "#10b981",
    "warning": "#f59e0b", "danger": "#ef4444",
}
PALETTE = ["#FF6B35","#1D9E75","#3B8BD4","#8b5cf6",
           "#f59e0b","#ec4899","#14b8a6","#64748b"]
STATUS_COLOR = {
    "delivered":"#10b981","shipped":"#3b82f6","processing":"#f59e0b",
    "canceled":"#ef4444","unavailable":"#9ca3af","invoiced":"#8b5cf6",
    "approved":"#14b8a6","created":"#f97316",
}
TIER_ICON = {"platinum":"🥇 Platinum","gold":"🥈 Gold",
             "silver":"🥉 Silver","bronze":"🔵 Bronze"}

BASE_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", size=12, color="#374151"),
    margin=dict(l=0, r=0, t=32, b=0),
    hoverlabel=dict(bgcolor="white", bordercolor="rgba(0,0,0,0.15)", font_size=12),
    xaxis=dict(showgrid=False, zeroline=False),
    yaxis=dict(gridcolor="rgba(0,0,0,0.05)", zeroline=False),
)

# ── Credentials ───────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def _get_credentials() -> dict:
    try:
        return dict(st.secrets["snowflake"])
    except Exception:
        pass
    try:
        import boto3
        c = boto3.client("secretsmanager", region_name="us-east-1")
        return json.loads(c.get_secret_value(SecretId="olist/snowflake/credentials")["SecretString"])
    except Exception:
        pass
    if os.environ.get("SNOWFLAKE_ACCOUNT"):
        return {k: os.environ[v] for k, v in {
            "account":"SNOWFLAKE_ACCOUNT","user":"SNOWFLAKE_USER",
            "password":"SNOWFLAKE_PASSWORD"}.items()} | {
            "role": os.environ.get("SNOWFLAKE_ROLE","SYSADMIN"),
            "database": os.environ.get("SNOWFLAKE_DATABASE","OLIST_DW"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA","MARTS"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE","OLIST_WH"),
        }
    raise RuntimeError("No Snowflake credentials found.")

@st.cache_resource(show_spinner=False)
def get_connection():
    c = _get_credentials()
    return snowflake.connector.connect(
        account=c["account"], user=c["user"], password=c["password"],
        role=c.get("role","SYSADMIN"), database=c.get("database","OLIST_DW"),
        schema=c.get("schema","MARTS"), warehouse=c.get("warehouse","OLIST_WH"),
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

# ── Helpers ───────────────────────────────────────────────────

def fmt_brl(v):
    if v >= 1_000_000: return f"R$ {v/1_000_000:.1f}M"
    if v >= 1_000:     return f"R$ {v/1_000:.1f}K"
    return f"R$ {v:,.0f}"

def fmt_num(v):
    if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if v >= 1_000:     return f"{v/1_000:.1f}K"
    return f"{int(v):,}"

def years_to_sql(years: list[int]) -> str:
    """Build safe SQL IN clause for year lists."""
    return ",".join(str(y) for y in years)

# ── Data layer ────────────────────────────────────────────────

@st.cache_data(ttl=600, show_spinner=False)
def load_freshness() -> Optional[datetime]:
    df = run_query(
        "SELECT MAX(order_purchase_timestamp) AS ts FROM OLIST_DW.MARTS.fct_orders",
        label="freshness"
    )
    if df.empty or df["ts"][0] is None: return None
    return pd.to_datetime(df["ts"][0])

@st.cache_data(ttl=600, show_spinner=False)
def load_kpis(years_csv: str) -> pd.DataFrame:
    # FIX: cast is_late_delivery to INTEGER before SUM
    return run_query(f"""
        SELECT
            COUNT(DISTINCT order_id)                                               AS total_orders,
            COUNT(*)                                                               AS total_items,
            ROUND(SUM(order_item_revenue), 2)                                     AS total_gmv,
            ROUND(AVG(avg_review_score), 2)                                       AS avg_review,
            ROUND(AVG(CASE WHEN delivery_days >= 0 THEN delivery_days END), 1)    AS avg_days,
            ROUND(100.0 * SUM(is_late_delivery::INTEGER) / NULLIF(COUNT(*),0), 1) AS late_pct,
            COUNT(DISTINCT customer_id)                                            AS unique_customers,
            COUNT(DISTINCT seller_id)                                              AS active_sellers
        FROM OLIST_DW.MARTS.fct_orders
        WHERE YEAR(order_purchase_timestamp) IN ({years_csv})
    """, label="kpis")

@st.cache_data(ttl=600, show_spinner=False)
def load_monthly_revenue(years_csv: str) -> pd.DataFrame:
    # FIX: correct IN clause — no sub-expression, direct integer list
    return run_query(f"""
        SELECT
            order_year_month,
            SUM(revenue_brl)  AS revenue,
            SUM(total_orders) AS orders,
            SUM(total_items)  AS items,
            AVG(avg_review)   AS avg_review
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE YEAR(TO_DATE(order_year_month || '-01')) IN ({years_csv})
        GROUP BY 1
        ORDER BY 1
    """, label="monthly_revenue")

@st.cache_data(ttl=600, show_spinner=False)
def load_category_revenue(years_csv: str, top_n: int) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            COALESCE(product_category,'unknown') AS category,
            ROUND(SUM(revenue_brl), 0)           AS revenue,
            SUM(total_orders)                    AS orders,
            ROUND(AVG(avg_review), 2)            AS avg_review,
            ROUND(AVG(avg_delivery_days), 1)     AS avg_days
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE YEAR(TO_DATE(order_year_month || '-01')) IN ({years_csv})
        GROUP BY 1 ORDER BY revenue DESC LIMIT {top_n}
    """, label=f"categories_{top_n}")

@st.cache_data(ttl=600, show_spinner=False)
def load_order_status(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            order_status,
            COUNT(DISTINCT order_id) AS cnt,
            ROUND(100.0 * COUNT(DISTINCT order_id) /
                  SUM(COUNT(DISTINCT order_id)) OVER (), 1) AS pct
        FROM OLIST_DW.MARTS.fct_orders
        WHERE YEAR(order_purchase_timestamp) IN ({years_csv})
        GROUP BY 1 ORDER BY cnt DESC
    """, label="order_status")

@st.cache_data(ttl=600, show_spinner=False)
def load_top_sellers(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            f.seller_id, s.state, s.seller_tier AS tier,
            COUNT(DISTINCT f.order_id)                                       AS orders,
            ROUND(SUM(f.order_item_revenue), 0)                             AS gmv,
            ROUND(AVG(f.avg_review_score), 2)                               AS score,
            ROUND(AVG(CASE WHEN f.delivery_days>=0 THEN f.delivery_days END),1) AS avg_days,
            ROUND(100.0 * SUM(f.is_late_delivery::INTEGER) /
                  NULLIF(COUNT(*),0), 1)                                    AS late_pct
        FROM OLIST_DW.MARTS.fct_orders f
        JOIN OLIST_DW.MARTS.dim_sellers s ON f.seller_id = s.seller_id
        WHERE YEAR(f.order_purchase_timestamp) IN ({years_csv})
        GROUP BY 1,2,3 ORDER BY gmv DESC LIMIT 20
    """, label="sellers")

@st.cache_data(ttl=600, show_spinner=False)
def load_region_data(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            customer_region AS region, customer_state AS state,
            customer_state_name AS state_name,
            ROUND(SUM(revenue_brl),0) AS revenue,
            SUM(total_orders)        AS orders,
            ROUND(AVG(avg_review),2) AS avg_review,
            ROUND(AVG(avg_delivery_days),1) AS avg_days
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE customer_region IS NOT NULL
          AND YEAR(TO_DATE(order_year_month || '-01')) IN ({years_csv})
        GROUP BY 1,2,3 ORDER BY revenue DESC
    """, label="region")

@st.cache_data(ttl=600, show_spinner=False)
def load_delivery(years_csv: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            customer_state,
            COUNT(DISTINCT order_id) AS orders,
            ROUND(AVG(CASE WHEN delivery_days>=0 THEN delivery_days END),1) AS avg_days,
            ROUND(100.0 * SUM(is_late_delivery::INTEGER) /
                  NULLIF(COUNT(*),0), 1) AS late_pct,
            ROUND(AVG(avg_review_score),2) AS avg_score
        FROM OLIST_DW.MARTS.fct_orders
        WHERE YEAR(order_purchase_timestamp) IN ({years_csv})
          AND customer_state IS NOT NULL
        GROUP BY 1 HAVING COUNT(DISTINCT order_id)>100
        ORDER BY late_pct DESC LIMIT 15
    """, label="delivery")

# ── UI components ──────────────────────────────────────────────

def render_header(ts: Optional[datetime]):
    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown("## 🛒 Olist E-commerce Analytics")
        st.caption("AWS Lambda → S3 → Glue ETL → Snowflake → dbt → Streamlit")
    with c2:
        if ts:
            age_h = (datetime.now(timezone.utc) -
                     ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            # Data is static 2016-2018 — stale only if > 30 days
            if age_h > 720:
                st.markdown(
                    f'<div class="stale-warning">📅 Static dataset<br>'
                    f'Latest order: {ts.strftime("%Y-%m-%d")}</div>',
                    unsafe_allow_html=True)
            else:
                st.success(f"✅ {ts.strftime('%Y-%m-%d')}")
        else:
            st.warning("Cannot determine freshness")

def render_kpis(df: pd.DataFrame):
    if df.empty:
        st.info("No KPI data for selected filters.")
        return
    r = df.iloc[0]
    cols = st.columns(8)
    metrics = [
        ("📦 Orders",        fmt_num(r["total_orders"])),
        ("🛍️ Items",         fmt_num(r["total_items"])),
        ("💰 GMV",           fmt_brl(r["total_gmv"])),
        ("👥 Customers",     fmt_num(r["unique_customers"])),
        ("🏪 Sellers",       fmt_num(r["active_sellers"])),
        ("⭐ Avg Review",    f"{r['avg_review']:.2f} / 5"),
        ("🚚 Avg Delivery",  f"{r['avg_days']:.1f} days"),
        ("⏰ Late Rate",     f"{r['late_pct']:.1f}%"),
    ]
    for col, (label, value) in zip(cols, metrics):
        col.metric(label, value)

def render_revenue_trend(df: pd.DataFrame):
    if df.empty:
        st.info("No revenue data for selected filters.")
        return
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=df["order_year_month"], y=df["revenue"], name="Revenue (BRL)",
        marker_color=COLORS["primary"], opacity=0.85,
        hovertemplate="<b>%{x}</b><br>Revenue: R$ %{y:,.0f}<extra></extra>",
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=df["order_year_month"], y=df["orders"], name="Orders",
        mode="lines+markers", line=dict(color=COLORS["tertiary"], width=2),
        marker=dict(size=5),
        hovertemplate="<b>%{x}</b><br>Orders: %{y:,}<extra></extra>",
    ), secondary_y=True)
    layout = dict(**BASE_LAYOUT, height=300,
                  legend=dict(orientation="h", yanchor="bottom",
                              y=1.02, xanchor="right", x=1),
                  hovermode="x unified")
    fig.update_layout(**layout)
    fig.update_yaxes(title_text="Revenue (BRL)", tickformat=",.0f",
                     gridcolor="rgba(0,0,0,0.05)", secondary_y=False)
    fig.update_yaxes(title_text="Orders", showgrid=False, secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

def render_categories(df: pd.DataFrame, top_n: int):
    if df.empty:
        st.info("No category data.")
        return
    fig = px.bar(
        df, x="revenue", y="category", orientation="h",
        color="avg_review", color_continuous_scale="RdYlGn",
        range_color=[3.0, 5.0],
        labels={"revenue":"Revenue (BRL)","category":"","avg_review":"Avg Review"},
        hover_data={"orders":True,"avg_review":":.2f","avg_days":True},
        custom_data=["orders","avg_review","avg_days"],
        title=f"Top {top_n} categories by revenue",
    )
    fig.update_traces(hovertemplate=(
        "<b>%{y}</b><br>Revenue: R$ %{x:,.0f}<br>"
        "Orders: %{customdata[0]:,}<br>Review: %{customdata[1]:.2f}<br>"
        "Avg days: %{customdata[2]:.1f}<extra></extra>"
    ))
    fig.update_yaxes(autorange="reversed")
    layout = dict(**BASE_LAYOUT, height=380)
    fig.update_layout(**layout)
    st.plotly_chart(fig, use_container_width=True)

def render_order_status(df: pd.DataFrame):
    if df.empty:
        st.info("No order status data.")
        return
    colors = [STATUS_COLOR.get(s, "#9ca3af") for s in df["order_status"]]
    total  = int(df["cnt"].sum())
    # FIX: build figure then call update_layout — no **CHART_LAYOUT spread issue
    fig = go.Figure(go.Pie(
        labels=df["order_status"].str.title(),
        values=df["cnt"],
        marker=dict(colors=colors, line=dict(color="white", width=2)),
        hole=0.52,
        textinfo="percent",
        hovertemplate="<b>%{label}</b><br>%{value:,} orders (%{percent})<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=12, color="#374151"),
        margin=dict(l=0, r=0, t=28, b=0),
        height=300,
        showlegend=True,
        legend=dict(orientation="v", x=1.0, y=0.5, font=dict(size=10)),
        hoverlabel=dict(bgcolor="white", font_size=12),
    )
    fig.add_annotation(
        text=f"<b>{fmt_num(total)}</b><br><span style='font-size:10px'>orders</span>",
        x=0.5, y=0.5, showarrow=False, font=dict(size=14),
        xref="paper", yref="paper",
    )
    st.plotly_chart(fig, use_container_width=True)

def render_top_sellers(df: pd.DataFrame):
    if df.empty:
        st.info("No seller data.")
        return
    d = df.copy()
    d["tier"]     = d["tier"].map(lambda t: TIER_ICON.get(t, t))
    d["gmv"]      = d["gmv"].apply(fmt_brl)
    d["late_pct"] = d["late_pct"].apply(lambda x: f"{x:.1f}%")
    d["score"]    = d["score"].apply(lambda x: f"⭐ {x:.2f}")
    d = d.rename(columns={
        "seller_id":"Seller ID","state":"State","tier":"Tier",
        "orders":"Orders","gmv":"GMV","score":"Review",
        "avg_days":"Avg Days","late_pct":"Late %",
    })
    st.dataframe(d[["Seller ID","State","Tier","Orders","GMV",
                     "Review","Avg Days","Late %"]],
                 use_container_width=True, height=400, hide_index=True)

def render_region(df: pd.DataFrame):
    if df.empty:
        st.info("No region data.")
        return
    agg = (df.groupby("region")
             .agg(revenue=("revenue","sum"), orders=("orders","sum"))
             .reset_index().sort_values("revenue", ascending=False))
    fig = px.bar(agg, x="region", y="revenue", color="region",
                 color_discrete_sequence=PALETTE,
                 labels={"revenue":"Revenue (BRL)","region":"Region"},
                 custom_data=["orders"])
    fig.update_traces(hovertemplate=(
        "<b>%{x}</b><br>Revenue: R$ %{y:,.0f}<br>"
        "Orders: %{customdata[0]:,}<extra></extra>"
    ))
    layout = dict(**BASE_LAYOUT, height=260, showlegend=False)
    fig.update_layout(**layout)
    st.plotly_chart(fig, use_container_width=True)

def render_delivery(df: pd.DataFrame):
    if df.empty:
        st.info("No delivery data.")
        return
    fig = make_subplots(rows=1, cols=2, horizontal_spacing=0.14,
                        subplot_titles=("Late Rate (%) by State",
                                        "Avg Delivery Days by State"))
    top = df.sort_values("late_pct", ascending=True).tail(10)
    fig.add_trace(go.Bar(
        x=top["late_pct"], y=top["customer_state"], orientation="h",
        marker_color=[COLORS["danger"] if x>15
                      else COLORS["warning"] if x>8
                      else COLORS["success"] for x in top["late_pct"]],
        hovertemplate="<b>%{y}</b>: %{x:.1f}%<extra></extra>", name="Late %",
    ), row=1, col=1)
    days = df.sort_values("avg_days", ascending=True).tail(10)
    fig.add_trace(go.Bar(
        x=days["avg_days"], y=days["customer_state"], orientation="h",
        marker_color=COLORS["tertiary"], opacity=0.8,
        hovertemplate="<b>%{y}</b>: %{x:.1f} days<extra></extra>", name="Avg Days",
    ), row=1, col=2)
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=12),
        margin=dict(l=0, r=0, t=32, b=0), height=320, showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

def download_btn(df, fname, label="⬇️ Download CSV"):
    if not df.empty:
        st.download_button(label, df.to_csv(index=False).encode(),
                           fname, "text/csv", use_container_width=True)

# ── Sidebar ────────────────────────────────────────────────────

def render_sidebar() -> dict:
    with st.sidebar:
        st.markdown("### 🛒 Olist Analytics")
        st.divider()
        st.markdown("**Filters**")
        years = st.multiselect("Order Year", [2016,2017,2018],
                               default=[2017,2018])
        if not years: years = [2017,2018]
        top_n = st.slider("Top N categories", 5, 20, 10)
        st.divider()
        show_raw = st.checkbox("Show raw data explorer")
        if st.button("🔄 Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()
        st.divider()
        st.caption("AWS Lambda → S3 → Glue → Snowflake → dbt")
    return {
        "years":    years,
        "years_csv": years_to_sql(years),
        "top_n":    top_n,
        "show_raw": show_raw,
    }

# ── Main ──────────────────────────────────────────────────────

def main():
    f = render_sidebar()

    with st.spinner("Checking data freshness…"):
        ts = load_freshness()
    render_header(ts)
    st.divider()

    with st.spinner("Loading KPIs…"):
        kpi_df = load_kpis(f["years_csv"])
    render_kpis(kpi_df)
    st.divider()

    t1, t2, t3, t4, t5 = st.tabs([
        "📈 Revenue", "🏆 Categories",
        "🗺️ Geography", "🥇 Sellers", "🚚 Delivery",
    ])

    with t1:
        col_l, col_r = st.columns([2, 1])
        with col_l:
            st.markdown('<p class="section-header">Monthly Revenue & Order Volume</p>',
                        unsafe_allow_html=True)
            with st.spinner("Loading…"):
                rev_df = load_monthly_revenue(f["years_csv"])
            render_revenue_trend(rev_df)
            download_btn(rev_df, "monthly_revenue.csv")
        with col_r:
            st.markdown('<p class="section-header">Order Status</p>',
                        unsafe_allow_html=True)
            with st.spinner("Loading…"):
                status_df = load_order_status(f["years_csv"])
            render_order_status(status_df)

    with t2:
        with st.spinner("Loading…"):
            cat_df = load_category_revenue(f["years_csv"], f["top_n"])
        render_categories(cat_df, f["top_n"])
        download_btn(cat_df, "categories.csv")

    with t3:
        with st.spinner("Loading…"):
            reg_df = load_region_data(f["years_csv"])
        col_l, col_r = st.columns([1, 2])
        with col_l:
            st.markdown('<p class="section-header">By Region</p>',
                        unsafe_allow_html=True)
            render_region(reg_df)
        with col_r:
            st.markdown('<p class="section-header">By State</p>',
                        unsafe_allow_html=True)
            if not reg_df.empty:
                d = reg_df.copy()
                d["revenue"] = d["revenue"].apply(fmt_brl)
                d = d.rename(columns={
                    "state_name":"State","state":"Code","region":"Region",
                    "revenue":"Revenue","orders":"Orders",
                    "avg_review":"Avg Review","avg_days":"Avg Days"})
                st.dataframe(d[["State","Code","Region","Revenue",
                                "Orders","Avg Review","Avg Days"]],
                             use_container_width=True, height=280, hide_index=True)
        download_btn(reg_df, "geography.csv")

    with t4:
        st.caption("Tier: 🥇 Platinum ≥ R$50K · 🥈 Gold ≥ R$10K · 🥉 Silver ≥ R$1K · 🔵 Bronze")
        with st.spinner("Loading…"):
            sell_df = load_top_sellers(f["years_csv"])
        tiers = ["All"] + sorted(sell_df["tier"].dropna().unique().tolist()) if not sell_df.empty else ["All"]
        sel = st.selectbox("Filter by tier", tiers)
        if sel != "All" and not sell_df.empty:
            sell_df = sell_df[sell_df["tier"] == sel]
        render_top_sellers(sell_df)
        download_btn(sell_df, "top_sellers.csv")

    with t5:
        st.caption("States with > 100 orders. Red = late > 15%, amber = > 8%")
        with st.spinner("Loading…"):
            del_df = load_delivery(f["years_csv"])
        render_delivery(del_df)
        download_btn(del_df, "delivery.csv")

    if f["show_raw"]:
        st.divider()
        st.markdown("### 🔍 Raw Data Explorer")
        tbl = st.selectbox("Table", ["fct_orders","fct_monthly_revenue",
                                      "dim_customers","dim_products","dim_sellers"])
        limit = st.slider("Rows", 50, 1000, 200, 50)
        with st.spinner(f"Loading {tbl}…"):
            raw = run_query(f"SELECT * FROM OLIST_DW.MARTS.{tbl} LIMIT {limit}",
                            label=f"raw_{tbl}")
        if not raw.empty:
            st.dataframe(raw, use_container_width=True)
            download_btn(raw, f"{tbl}.csv", f"⬇️ {tbl}.csv")

    st.divider()
    st.caption(
        f"© Olist Analytics · "
        f"Refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

if __name__ == "__main__":
    main()
