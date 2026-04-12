"""
Olist E-commerce Analytics Dashboard
Connects to Snowflake using externalbrowser (SSO) auth
"""
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import snowflake.connector

# ── Page Config ───────────────────────────────────────────────
st.set_page_config(
    page_title="Olist E-commerce Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Snowflake Connection ───────────────────────────────────────
@st.cache_resource(show_spinner="Connecting to Snowflake…")
def get_snowflake_connection():
    return snowflake.connector.connect(
        account     = os.getenv("SNOWFLAKE_ACCOUNT", "NZFSGYT-PU98877"),   # or "NZFSGYT"
        user        = os.getenv("SNOWFLAKE_USER", "NORANSALM15"),
        password    = os.getenv("SNOWFLAKE_PASSWORD"),                     # ← required now
        role        = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        database    = os.getenv("SNOWFLAKE_DATABASE", "OLIST_DW"),
        schema      = os.getenv("SNOWFLAKE_SCHEMA", "MARTS"),
        warehouse   = os.getenv("SNOWFLAKE_WAREHOUSE", "OLIST_WH"),
    )

@st.cache_data(ttl=600, show_spinner="Fetching data…")
def run_query(sql: str) -> pd.DataFrame:
    conn   = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    df = cursor.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]
    return df

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.title("🛒 Olist Analytics")
    st.divider()
    year_options = [2016, 2017, 2018]
    selected_years = st.multiselect("Order Year", year_options, default=[2017, 2018])
    top_n = st.slider("Top N Categories", 5, 20, 10)
    st.divider()
    if st.button("🔄 Clear Cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

year_clause = (
    f"YEAR(order_purchase_timestamp) IN ({','.join(str(y) for y in selected_years)})"
    if selected_years else "1=1"
)

# ── Header ────────────────────────────────────────────────────
st.title("🛒 Olist E-commerce Analytics")
st.caption("Pipeline: AWS Lambda → S3 → Glue ETL → Snowflake → dbt → Streamlit")
st.divider()

# ── KPI Cards ─────────────────────────────────────────────────
kpi_sql = f"""
SELECT
    COUNT(DISTINCT order_id)                                    AS total_orders,
    COUNT(*)                                                    AS total_items,
    ROUND(SUM(order_item_revenue), 2)                          AS total_gmv,
    ROUND(AVG(avg_review_score), 2)                            AS avg_review,
    ROUND(AVG(delivery_days), 1)                               AS avg_days,
    ROUND(100.0 * COUNT_IF(is_late_delivery) / NULLIF(COUNT(*),0), 2) AS late_pct
FROM OLIST_DW.MARTS.fct_orders
WHERE {year_clause}
"""
kpi = run_query(kpi_sql)

if not kpi.empty:
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("📦 Orders",        f"{int(kpi['total_orders'][0]):,}")
    c2.metric("🛍️ Items Sold",     f"{int(kpi['total_items'][0]):,}")
    c3.metric("💰 GMV (BRL)",      f"R$ {kpi['total_gmv'][0]:,.0f}")
    c4.metric("⭐ Avg Review",     f"{kpi['avg_review'][0]} / 5")
    c5.metric("🚚 Avg Delivery",   f"{kpi['avg_days'][0]} days")
    c6.metric("⏰ Late Rate",      f"{kpi['late_pct'][0]}%")

st.divider()

# ── Row 1: Revenue Trend + Top Categories ─────────────────────
col1, col2 = st.columns([3, 2])

with col1:
    st.subheader("📈 Monthly Revenue (BRL)")
    rev_sql = f"""
    SELECT
        DATE_TRUNC('month', order_purchase_timestamp)::DATE AS month,
        ROUND(SUM(order_item_revenue), 2)                   AS revenue,
        COUNT(DISTINCT order_id)                            AS orders
    FROM OLIST_DW.MARTS.fct_orders
    WHERE {year_clause}
    GROUP BY 1 ORDER BY 1
    """
    rev_df = run_query(rev_sql)
    if not rev_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=rev_df["month"], y=rev_df["revenue"],
            mode="lines+markers",
            line=dict(color="#FF6B35", width=2.5),
            fill="tozeroy",
            fillcolor="rgba(255,107,53,0.12)",
            name="Revenue"
        ))
        fig.update_layout(
            xaxis_title="Month", yaxis_title="Revenue (BRL)",
            hovermode="x unified",
            margin=dict(l=0, r=0, t=10, b=0), height=320
        )
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader(f"🏆 Top {top_n} Categories")
    cat_sql = f"""
    SELECT
        COALESCE(product_category, 'unknown') AS category,
        ROUND(SUM(order_item_revenue), 2)     AS revenue
    FROM OLIST_DW.MARTS.fct_orders
    WHERE {year_clause}
    GROUP BY 1 ORDER BY revenue DESC
    LIMIT {top_n}
    """
    cat_df = run_query(cat_sql)
    if not cat_df.empty:
        fig2 = px.bar(
            cat_df, x="revenue", y="category", orientation="h",
            color="revenue", color_continuous_scale="Oranges",
            labels={"revenue": "Revenue (BRL)", "category": ""}
        )
        fig2.update_layout(
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0), height=320
        )
        fig2.update_yaxes(autorange="reversed")
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Row 2: Order Status Pie + Seller Table ────────────────────
col3, col4 = st.columns([1, 2])

with col3:
    st.subheader("📊 Order Status")
    status_sql = f"""
    SELECT order_status, COUNT(DISTINCT order_id) AS cnt
    FROM OLIST_DW.MARTS.fct_orders
    WHERE {year_clause}
    GROUP BY 1 ORDER BY cnt DESC
    """
    status_df = run_query(status_sql)
    if not status_df.empty:
        color_map = {
            "delivered":"#2ECC71","shipped":"#3498DB",
            "processing":"#F39C12","canceled":"#E74C3C",
            "unavailable":"#95A5A6","invoiced":"#9B59B6",
            "approved":"#1ABC9C","created":"#E67E22"
        }
        fig3 = px.pie(
            status_df, values="cnt", names="order_status",
            color="order_status", color_discrete_map=color_map, hole=0.45
        )
        fig3.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=320)
        st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("🥇 Top Sellers")
    seller_sql = f"""
    SELECT
        f.seller_id,
        s.state                                              AS state,
        s.seller_tier                                        AS tier,
        COUNT(DISTINCT f.order_id)                           AS orders,
        ROUND(SUM(f.order_item_revenue), 2)                 AS gmv,
        ROUND(AVG(f.avg_review_score), 2)                   AS score,
        ROUND(AVG(f.delivery_days), 1)                      AS avg_days
    FROM OLIST_DW.MARTS.fct_orders f
    JOIN OLIST_DW.MARTS.dim_sellers s ON f.seller_id = s.seller_id
    WHERE {year_clause}
    GROUP BY 1, 2, 3
    ORDER BY gmv DESC
    LIMIT 15
    """
    seller_df = run_query(seller_sql)
    if not seller_df.empty:
        tier_icon = {"platinum":"🥇","gold":"🥈","silver":"🥉","bronze":"🔵"}
        seller_df["tier"] = seller_df["tier"].map(
            lambda t: f"{tier_icon.get(t,'')} {t.title()}" if t else "—"
        )
        seller_df.columns = ["Seller ID","State","Tier","Orders","GMV (BRL)","Score","Avg Days"]
        st.dataframe(seller_df, use_container_width=True, height=320)

st.divider()

# ── Raw Data Explorer ──────────────────────────────────────────
with st.expander("🔍 Raw Data Explorer"):
    tbl = st.selectbox("Table", ["fct_orders","dim_customers","dim_products","dim_sellers"])
    raw_df = run_query(f"SELECT * FROM OLIST_DW.MARTS.{tbl} LIMIT 500")
    st.dataframe(raw_df, use_container_width=True)
    st.download_button(
        "⬇️ Download CSV",
        data=raw_df.to_csv(index=False).encode("utf-8"),
        file_name=f"{tbl}.csv", mime="text/csv"
    )

st.caption("© Olist Analytics · AWS + Snowflake + dbt + Streamlit")
