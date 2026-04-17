"""
Olist E-commerce Analytics Dashboard
Credential strategy:
  - Streamlit Cloud: uses st.secrets (secrets.toml configured in dashboard)
  - Local / AWS App Runner: uses AWS Secrets Manager via boto3
"""
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import snowflake.connector

st.set_page_config(
    page_title="Olist E-commerce Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Credential Strategy ────────────────────────────────────────
@st.cache_resource(show_spinner="Loading credentials…")
def get_credentials() -> dict:
    """
    Try Streamlit secrets first (works on Streamlit Cloud).
    Fall back to AWS Secrets Manager (works on AWS/local).
    """
    # Strategy 1: Streamlit secrets (Streamlit Cloud deployment)
    try:
        creds = dict(st.secrets["snowflake"])
        st.sidebar.caption("🔑 Auth: Streamlit Secrets")
        return creds
    except (KeyError, FileNotFoundError):
        pass

    # Strategy 2: AWS Secrets Manager (AWS deployment)
    try:
        import boto3, json
        client = boto3.client("secretsmanager", region_name="us-east-1")
        resp   = client.get_secret_value(SecretId="olist/snowflake/credentials")
        creds  = json.loads(resp["SecretString"])
        st.sidebar.caption("🔐 Auth: AWS Secrets Manager")
        return creds
    except Exception:
        pass

    # Strategy 3: Environment variables (local dev)
    if os.environ.get("SNOWFLAKE_ACCOUNT"):
        st.sidebar.caption("🔧 Auth: Environment Variables")
        return {
            "account":   os.environ["SNOWFLAKE_ACCOUNT"],
            "user":      os.environ["SNOWFLAKE_USER"],
            "password":  os.environ["SNOWFLAKE_PASSWORD"],
            "role":      os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN"),
            "database":  os.environ.get("SNOWFLAKE_DATABASE",  "OLIST_DW"),
            "schema":    os.environ.get("SNOWFLAKE_SCHEMA",    "MARTS"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "OLIST_WH"),
        }

    st.error("❌ No credentials found. Configure Streamlit secrets or AWS Secrets Manager.")
    st.stop()


@st.cache_resource(show_spinner="Connecting to Snowflake…")
def get_snowflake_connection():
    creds = get_credentials()
    return snowflake.connector.connect(
        account   = creds["account"],
        user      = creds["user"],
        password  = creds["password"],
        role      = creds.get("role",      "SYSADMIN"),
        database  = creds.get("database",  "OLIST_DW"),
        schema    = creds.get("schema",    "MARTS"),
        warehouse = creds.get("warehouse", "OLIST_WH"),
    )


@st.cache_data(ttl=600, show_spinner="Fetching data…")
def run_query(sql: str) -> pd.DataFrame:
    conn   = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    df = cursor.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]
    return df


# ── Sidebar ─────────────────────────────────────────────────────
with st.sidebar:
    st.title("🛒 Olist Analytics")
    st.divider()
    selected_years = st.multiselect(
        "Order Year", [2016, 2017, 2018], default=[2017, 2018]
    )
    top_n = st.slider("Top N Categories", 5, 20, 10)
    st.divider()
    if st.button("🔄 Clear Cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

year_in = ','.join(str(y) for y in selected_years) if selected_years else '2017,2018'
year_clause = f"YEAR(order_purchase_timestamp) IN ({year_in})"
month_clause = f"YEAR(TO_DATE(order_year_month || '-01')) IN ({year_in})"

# ── Header ──────────────────────────────────────────────────────
st.title("🛒 Olist E-commerce Analytics")
st.caption("AWS Lambda → S3 → Glue ETL → Snowflake → dbt → Streamlit")
st.divider()

# ── KPIs ────────────────────────────────────────────────────────
kpi = run_query(f"""
SELECT
    COUNT(DISTINCT order_id)                                      AS total_orders,
    COUNT(*)                                                      AS total_items,
    ROUND(SUM(order_item_revenue), 2)                            AS total_gmv,
    ROUND(AVG(avg_review_score), 2)                              AS avg_review,
    ROUND(AVG(delivery_days), 1)                                 AS avg_days,
    ROUND(100.0 * COUNT_IF(is_late_delivery) / NULLIF(COUNT(*),0), 2) AS late_pct
FROM OLIST_DW.MARTS.fct_orders
WHERE {year_clause}
""")

if not kpi.empty:
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("📦 Orders",       f"{int(kpi['total_orders'][0]):,}")
    c2.metric("🛍️ Items",         f"{int(kpi['total_items'][0]):,}")
    c3.metric("💰 GMV (BRL)",     f"R$ {kpi['total_gmv'][0]:,.0f}")
    c4.metric("⭐ Avg Review",    f"{kpi['avg_review'][0]} / 5")
    c5.metric("🚚 Avg Delivery",  f"{kpi['avg_days'][0]} days")
    c6.metric("⏰ Late Rate",     f"{kpi['late_pct'][0]}%")

st.divider()

# ── Row 1: Revenue + Categories ─────────────────────────────────
col1, col2 = st.columns([3, 2])

with col1:
    st.subheader("📈 Monthly Revenue (BRL)")
    rev = run_query(f"""
        SELECT order_year_month, SUM(revenue_brl) AS revenue, SUM(total_orders) AS orders
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE {month_clause}
        GROUP BY 1 ORDER BY 1
    """)
    if not rev.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=rev["order_year_month"], y=rev["revenue"],
            mode="lines+markers",
            line=dict(color="#FF6B35", width=2.5),
            fill="tozeroy", fillcolor="rgba(255,107,53,0.12)",
        ))
        fig.update_layout(
            xaxis_title="Month", yaxis_title="Revenue (BRL)",
            hovermode="x unified",
            margin=dict(l=0,r=0,t=10,b=0), height=320
        )
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader(f"🏆 Top {top_n} Categories")
    cat = run_query(f"""
        SELECT COALESCE(product_category,'unknown') AS category,
               ROUND(SUM(revenue_brl),2) AS revenue
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE {month_clause}
        GROUP BY 1 ORDER BY revenue DESC LIMIT {top_n}
    """)
    if not cat.empty:
        fig2 = px.bar(cat, x="revenue", y="category", orientation="h",
                      color="revenue", color_continuous_scale="Oranges")
        fig2.update_layout(
            coloraxis_showscale=False,
            margin=dict(l=0,r=0,t=10,b=0), height=320
        )
        fig2.update_yaxes(autorange="reversed")
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Row 2: Status Pie + Sellers ─────────────────────────────────
col3, col4 = st.columns([1, 2])

with col3:
    st.subheader("📊 Order Status")
    status = run_query(f"""
        SELECT order_status, COUNT(DISTINCT order_id) AS cnt
        FROM OLIST_DW.MARTS.fct_orders WHERE {year_clause}
        GROUP BY 1 ORDER BY cnt DESC
    """)
    if not status.empty:
        color_map = {
            "delivered":"#2ECC71","shipped":"#3498DB",
            "processing":"#F39C12","canceled":"#E74C3C",
            "unavailable":"#95A5A6","invoiced":"#9B59B6",
            "approved":"#1ABC9C","created":"#E67E22"
        }
        fig3 = px.pie(status, values="cnt", names="order_status",
                      color="order_status",
                      color_discrete_map=color_map, hole=0.45)
        fig3.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=320)
        st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("🥇 Top Sellers")
    sellers = run_query(f"""
        SELECT f.seller_id, s.state, s.seller_tier AS tier,
               COUNT(DISTINCT f.order_id) AS orders,
               ROUND(SUM(f.order_item_revenue),2) AS gmv,
               ROUND(AVG(f.avg_review_score),2) AS score
        FROM OLIST_DW.MARTS.fct_orders f
        JOIN OLIST_DW.MARTS.dim_sellers s ON f.seller_id = s.seller_id
        WHERE {year_clause}
        GROUP BY 1,2,3 ORDER BY gmv DESC LIMIT 15
    """)
    if not sellers.empty:
        icons = {"platinum":"🥇","gold":"🥈","silver":"🥉","bronze":"🔵"}
        sellers["tier"] = sellers["tier"].map(
            lambda t: f"{icons.get(t,'')} {t.title()}" if t else "—"
        )
        sellers.columns = ["Seller ID","State","Tier","Orders","GMV (BRL)","Score"]
        st.dataframe(sellers, use_container_width=True, height=320)

st.divider()

# ── Region Chart ─────────────────────────────────────────────────
st.subheader("🗺️ Revenue by Region")
region = run_query(f"""
    SELECT customer_region AS region,
           ROUND(SUM(revenue_brl),0) AS revenue,
           SUM(total_orders) AS orders
    FROM OLIST_DW.MARTS.fct_monthly_revenue
    WHERE customer_region IS NOT NULL AND {month_clause}
    GROUP BY 1 ORDER BY revenue DESC
""")
if not region.empty:
    col_r1, col_r2 = st.columns([2, 1])
    with col_r1:
        fig4 = px.bar(region, x="region", y="revenue",
                      color="revenue", color_continuous_scale="Oranges")
        fig4.update_layout(
            coloraxis_showscale=False,
            margin=dict(l=0,r=0,t=10,b=0), height=300
        )
        st.plotly_chart(fig4, use_container_width=True)
    with col_r2:
        st.dataframe(
            region.rename(columns={"region":"Region",
                                   "revenue":"Revenue (BRL)",
                                   "orders":"Orders"}),
            use_container_width=True, height=300
        )

# ── Raw Explorer ─────────────────────────────────────────────────
with st.expander("🔍 Raw Data Explorer"):
    tbl = st.selectbox("Table", [
        "fct_orders","fct_monthly_revenue",
        "dim_customers","dim_products","dim_sellers"
    ])
    raw = run_query(f"SELECT * FROM OLIST_DW.MARTS.{tbl} LIMIT 500")
    st.dataframe(raw, use_container_width=True)
    st.download_button("⬇️ Download CSV",
        data=raw.to_csv(index=False).encode(),
        file_name=f"{tbl}.csv", mime="text/csv")

st.caption("© Olist Analytics · AWS + Snowflake + dbt + Streamlit")
