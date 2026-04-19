"""
dashboard/streamlit_app.py — Olist Analytics v3.0
SaaS-grade dashboard — Stripe/Notion aesthetic
"""
import json, logging, os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import streamlit as st

log = logging.getLogger("olist")
logging.basicConfig(level=logging.INFO)

st.set_page_config(
    page_title="Olist Analytics", page_icon="🛒", layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""<style>
div[data-testid="metric-container"] {
    background:white; border:1px solid #E5E7EB; border-radius:10px;
    padding:20px 22px 16px; box-shadow:0 1px 3px rgba(0,0,0,0.08);
    transition:box-shadow 0.15s;
}
div[data-testid="metric-container"]:hover { box-shadow:0 4px 12px rgba(0,0,0,0.1); }
div[data-testid="metric-container"] > label {
    font-size:0.72rem!important; font-weight:600!important;
    letter-spacing:0.06em!important; text-transform:uppercase!important;
    color:#9CA3AF!important;
}
div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size:1.75rem!important; font-weight:700!important;
    color:#111827!important; letter-spacing:-0.02em;
}
.sec { font-size:0.78rem; font-weight:700; letter-spacing:0.08em;
       text-transform:uppercase; color:#9CA3AF; margin:1.5rem 0 0.6rem; }
.badge-g { display:inline-block; padding:3px 10px; border-radius:20px;
           font-size:0.72rem; font-weight:600; background:#D1FAE5; color:#065F46; }
.badge-o { display:inline-block; padding:3px 10px; border-radius:20px;
           font-size:0.72rem; font-weight:600; background:#FEF3C7; color:#92400E; }
#MainMenu, footer, header { visibility:hidden; }
div[data-testid="stToolbar"] { display:none; }
button[data-baseweb="tab"] { font-size:0.84rem!important; font-weight:500!important; }
hr { border-color:#F3F4F6!important; margin:1rem 0; }
</style>""", unsafe_allow_html=True)

C = {"orange":"#FF6B35","green":"#1D9E75","blue":"#3B8BD4","purple":"#7C3AED",
     "red":"#EF4444","amber":"#F59E0B","teal":"#14B8A6","slate":"#64748B"}
PALETTE = [C["orange"],C["green"],C["blue"],C["purple"],
           C["amber"],C["teal"],C["red"],C["slate"]]
STATUS_COLOR = {
    "delivered":C["green"],"shipped":C["blue"],"processing":C["amber"],
    "canceled":C["red"],"unavailable":"#9CA3AF","invoiced":C["purple"],
    "approved":C["teal"],"created":C["orange"],
}
TIER_ICON = {"platinum":"🥇","gold":"🥈","silver":"🥉","bronze":"🔵"}

def _L(h=300, legend=True, **kw):
    d = dict(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
             font=dict(family="Inter,system-ui,sans-serif",size=12,color="#374151"),
             margin=dict(l=0,r=0,t=36,b=0), height=h, showlegend=legend,
             legend=dict(bgcolor="rgba(255,255,255,0.9)",bordercolor="rgba(0,0,0,0.08)",
                         borderwidth=1,font=dict(size=11),orientation="h",
                         yanchor="bottom",y=1.02,xanchor="right",x=1),
             hoverlabel=dict(bgcolor="white",bordercolor="rgba(0,0,0,0.12)",font=dict(size=12)),
             hovermode="x unified",
             xaxis=dict(showgrid=False,zeroline=False,linecolor="#E5E7EB"),
             yaxis=dict(gridcolor="rgba(0,0,0,0.05)",zeroline=False,tickformat=","))
    d.update(kw); return d

@st.cache_resource(show_spinner=False)
def _creds():
    try: return dict(st.secrets["snowflake"])
    except Exception: pass
    try:
        import boto3
        sm = boto3.client("secretsmanager",region_name="us-east-1")
        return json.loads(sm.get_secret_value(SecretId="olist/snowflake/credentials")["SecretString"])
    except Exception: pass
    if os.environ.get("SNOWFLAKE_ACCOUNT"):
        return {"account":os.environ["SNOWFLAKE_ACCOUNT"],"user":os.environ["SNOWFLAKE_USER"],
                "password":os.environ["SNOWFLAKE_PASSWORD"],
                "role":os.environ.get("SNOWFLAKE_ROLE","SYSADMIN"),
                "database":os.environ.get("SNOWFLAKE_DATABASE","OLIST_DW"),
                "schema":os.environ.get("SNOWFLAKE_SCHEMA","MARTS"),
                "warehouse":os.environ.get("SNOWFLAKE_WAREHOUSE","OLIST_WH")}
    raise RuntimeError("No Snowflake credentials found.")

@st.cache_resource(show_spinner=False)
def _conn():
    c = _creds()
    return snowflake.connector.connect(
        account=c["account"],user=c["user"],password=c["password"],
        role=c.get("role","SYSADMIN"),database=c.get("database","OLIST_DW"),
        schema=c.get("schema","MARTS"),warehouse=c.get("warehouse","OLIST_WH"),
        session_parameters={"QUERY_TAG":"streamlit-v3"})

@st.cache_data(ttl=600, show_spinner=False)
def q(sql, label=""):
    try:
        cur = _conn().cursor(); cur.execute(sql)
        df  = cur.fetch_pandas_all(); df.columns=[c.lower() for c in df.columns]
        return df
    except Exception as e:
        log.error("FAIL[%s]:%s",label,e); st.error(f"Query error ({label}): {e}")
        return pd.DataFrame()

def brl(v):
    if v>=1e6: return f"R$ {v/1e6:.1f}M"
    if v>=1e3: return f"R$ {v/1e3:.0f}K"
    return f"R$ {v:,.0f}"
def num(v):
    if v>=1e6: return f"{v/1e6:.1f}M"
    if v>=1e3: return f"{v/1e3:.0f}K"
    return f"{int(v):,}"
def late_c(x): return C["red"] if x>15 else C["amber"] if x>8 else C["green"]

def load_freshness():
    df=q("SELECT MAX(order_purchase_timestamp) AS ts FROM OLIST_DW.MARTS.fct_orders","ts")
    if df.empty or df["ts"][0] is None: return None
    return pd.to_datetime(df["ts"][0])

def load_kpis(ycsv):
    return q(f"""SELECT COUNT(DISTINCT order_id) AS orders, COUNT(*) AS items,
        ROUND(SUM(order_item_revenue),2) AS gmv, ROUND(AVG(avg_review_score),2) AS review,
        ROUND(AVG(CASE WHEN delivery_days>=0 THEN delivery_days END),1) AS days,
        ROUND(100.0*SUM(is_late_delivery::INTEGER)/NULLIF(COUNT(*),0),1) AS late_pct,
        COUNT(DISTINCT customer_id) AS customers, COUNT(DISTINCT seller_id) AS sellers
        FROM OLIST_DW.MARTS.fct_orders WHERE YEAR(order_purchase_timestamp) IN ({ycsv})""","kpis")

def load_revenue(ycsv):
    return q(f"""SELECT order_year_month, SUM(revenue_brl) AS revenue,
        SUM(total_orders) AS orders, AVG(avg_review) AS avg_review
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE YEAR(TO_DATE(order_year_month||'-01')) IN ({ycsv})
        GROUP BY 1 ORDER BY 1""","rev")

def load_categories(ycsv,n):
    return q(f"""SELECT COALESCE(product_category,'unknown') AS category,
        ROUND(SUM(revenue_brl),0) AS revenue, SUM(total_orders) AS orders,
        ROUND(AVG(avg_review),2) AS avg_review, ROUND(AVG(avg_delivery_days),1) AS avg_days
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE YEAR(TO_DATE(order_year_month||'-01')) IN ({ycsv})
        GROUP BY 1 ORDER BY revenue DESC LIMIT {n}""","cat")

def load_status(ycsv):
    return q(f"""SELECT order_status, COUNT(DISTINCT order_id) AS cnt,
        ROUND(100.0*COUNT(DISTINCT order_id)/SUM(COUNT(DISTINCT order_id)) OVER(),1) AS pct
        FROM OLIST_DW.MARTS.fct_orders WHERE YEAR(order_purchase_timestamp) IN ({ycsv})
        GROUP BY 1 ORDER BY cnt DESC""","status")

def load_sellers(ycsv):
    return q(f"""SELECT f.seller_id, s.state, s.seller_tier AS tier,
        COUNT(DISTINCT f.order_id) AS orders, ROUND(SUM(f.order_item_revenue),0) AS gmv,
        ROUND(AVG(f.avg_review_score),2) AS score,
        ROUND(AVG(CASE WHEN f.delivery_days>=0 THEN f.delivery_days END),1) AS days,
        ROUND(100.0*SUM(f.is_late_delivery::INTEGER)/NULLIF(COUNT(*),0),1) AS late_pct
        FROM OLIST_DW.MARTS.fct_orders f
        JOIN OLIST_DW.MARTS.dim_sellers s ON f.seller_id=s.seller_id
        WHERE YEAR(f.order_purchase_timestamp) IN ({ycsv})
        GROUP BY 1,2,3 ORDER BY gmv DESC LIMIT 20""","sellers")

def load_regions(ycsv):
    return q(f"""SELECT customer_region AS region, customer_state AS state,
        customer_state_name AS state_name, ROUND(SUM(revenue_brl),0) AS revenue,
        SUM(total_orders) AS orders, ROUND(AVG(avg_review),2) AS avg_review,
        ROUND(AVG(avg_delivery_days),1) AS avg_days
        FROM OLIST_DW.MARTS.fct_monthly_revenue
        WHERE customer_region IS NOT NULL
          AND YEAR(TO_DATE(order_year_month||'-01')) IN ({ycsv})
        GROUP BY 1,2,3 ORDER BY revenue DESC""","regions")

def load_delivery(ycsv):
    return q(f"""SELECT customer_state,
        COUNT(DISTINCT order_id) AS orders,
        ROUND(AVG(CASE WHEN delivery_days>=0 THEN delivery_days END),1) AS avg_days,
        ROUND(100.0*SUM(is_late_delivery::INTEGER)/NULLIF(COUNT(*),0),1) AS late_pct,
        ROUND(AVG(avg_review_score),2) AS score
        FROM OLIST_DW.MARTS.fct_orders
        WHERE YEAR(order_purchase_timestamp) IN ({ycsv}) AND customer_state IS NOT NULL
        GROUP BY 1 HAVING COUNT(DISTINCT order_id)>100
        ORDER BY late_pct DESC LIMIT 15""","delivery")

def sec(t): st.markdown(f'<p class="sec">{t}</p>',unsafe_allow_html=True)
def dl(df,fname,label="⬇️ Export CSV"):
    if not df.empty:
        st.download_button(label,df.to_csv(index=False).encode(),fname,"text/csv")

def render_header(ts):
    l,r=st.columns([4,1])
    with l:
        st.markdown("<h2 style='margin:0;font-size:1.5rem;font-weight:700;color:#111827;"
                    "letter-spacing:-0.02em'>Olist Analytics</h2>",unsafe_allow_html=True)
        st.markdown("<p style='margin:2px 0 0;font-size:0.82rem;color:#9CA3AF'>"
                    "AWS Lambda · S3 · Glue · Snowflake · dbt · Streamlit</p>",
                    unsafe_allow_html=True)
    with r:
        if ts:
            age=(datetime.now(timezone.utc)-ts.replace(tzinfo=timezone.utc)).days
            cls="badge-o" if age>30 else "badge-g"
            lbl=f"📅 {ts.strftime('%Y-%m-%d')}" if age>30 else f"✓ {ts.strftime('%Y-%m-%d')}"
            st.markdown(f'<div style="text-align:right;margin-top:10px">'
                        f'<span class="{cls}">{lbl}</span></div>',unsafe_allow_html=True)

def render_kpis(df):
    if df.empty: st.info("No data."); return
    r=df.iloc[0]
    cols=st.columns(8)
    data=[("📦 Orders",num(r["orders"]),None,None),
          ("🛍️ Items sold",num(r["items"]),None,None),
          ("💰 Gross GMV",brl(r["gmv"]),None,None),
          ("👥 Customers",num(r["customers"]),None,None),
          ("🏪 Active sellers",num(r["sellers"]),None,None),
          ("⭐ Avg review",f"{r['review']:.2f}","/ 5.0","off"),
          ("🚚 Avg delivery",f"{r['days']:.1f} d",None,None),
          ("⏰ Late rate",f"{r['late_pct']:.1f}%",
           "↑ above target" if r["late_pct"]>10 else "↓ within target",
           "inverse" if r["late_pct"]>10 else "normal")]
    for col,(lbl,val,dlt,dcol) in zip(cols,data):
        col.metric(lbl,val,dlt,delta_color=dcol) if dlt else col.metric(lbl,val)

def tab_revenue(ycsv):
    cl,cr=st.columns([3,1],gap="medium")
    with cl:
        sec("Monthly revenue & order volume")
        rev=load_revenue(ycsv)
        if not rev.empty:
            fig=make_subplots(specs=[[{"secondary_y":True}]])
            fig.add_trace(go.Bar(x=rev["order_year_month"],y=rev["revenue"],
                name="Revenue (BRL)",marker=dict(color=C["orange"],opacity=0.85,line=dict(width=0)),
                hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"),secondary_y=False)
            fig.add_trace(go.Scatter(x=rev["order_year_month"],y=rev["orders"],
                name="Orders",mode="lines+markers",
                line=dict(color=C["blue"],width=2.5),
                marker=dict(size=6,line=dict(color="white",width=1.5)),
                hovertemplate="<b>%{x}</b><br>%{y:,} orders<extra></extra>"),secondary_y=True)
            fig.update_layout(**_L(h=300))
            fig.update_yaxes(title_text="Revenue (BRL)",tickprefix="R$ ",
                             gridcolor="rgba(0,0,0,0.04)",secondary_y=False)
            fig.update_yaxes(title_text="Orders",showgrid=False,secondary_y=True)
            st.plotly_chart(fig,use_container_width=True)
            dl(rev,"monthly_revenue.csv")
        else: st.info("No revenue data.")
    with cr:
        sec("Order status")
        status=load_status(ycsv)
        if not status.empty:
            colors=[STATUS_COLOR.get(s,"#9CA3AF") for s in status["order_status"]]
            total=int(status["cnt"].sum())
            fig=go.Figure(go.Pie(
                labels=status["order_status"].str.title(),values=status["cnt"],
                marker=dict(colors=colors,line=dict(color="white",width=2.5)),
                hole=0.58,textinfo="none",
                hovertemplate="<b>%{label}</b><br>%{value:,} · %{percent}<extra></extra>"))
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0,r=0,t=8,b=0),height=260,
                showlegend=True,
                legend=dict(orientation="v",x=1.0,y=0.5,font=dict(size=10),bgcolor="rgba(0,0,0,0)"),
                hoverlabel=dict(bgcolor="white",font_size=12),
                annotations=[dict(text=f"<b>{num(total)}</b><br>"
                    "<span style='font-size:11px;color:#6B7280'>orders</span>",
                    x=0.5,y=0.5,showarrow=False,font=dict(size=15,color="#111827"),
                    xref="paper",yref="paper")])
            st.plotly_chart(fig,use_container_width=True)
            d=status.copy()
            d["pct"]=d["pct"].apply(lambda x:f"{x:.1f}%")
            d["cnt"]=d["cnt"].apply(num)
            d=d.rename(columns={"order_status":"Status","cnt":"Orders","pct":"Share"})
            d["Status"]=d["Status"].str.title()
            st.dataframe(d[["Status","Orders","Share"]],hide_index=True,
                         use_container_width=True,height=200)

def tab_categories(ycsv,top_n):
    cat=load_categories(ycsv,top_n)
    if cat.empty: st.info("No data."); return
    c1,c2,c3=st.columns(3)
    c1.metric("Categories shown",str(len(cat)))
    c2.metric("Top category",cat.iloc[0]["category"].replace("_"," ").title())
    c3.metric("Top GMV",brl(cat.iloc[0]["revenue"]))
    fig=go.Figure(go.Bar(
        x=cat["revenue"],y=cat["category"],orientation="h",
        marker=dict(color=cat["avg_review"],
                    colorscale=[[0,"#FCA5A5"],[0.5,"#FCD34D"],[1,"#6EE7B7"]],
                    cmin=3.0,cmax=5.0,showscale=True,
                    colorbar=dict(title=dict(text="Review★",side="right"),
                                  thickness=10,len=0.6,
                                  tickvals=[3,4,5],ticktext=["3★","4★","5★"]),
                    line=dict(width=0)),
        customdata=cat[["orders","avg_review","avg_days"]].values,
        hovertemplate="<b>%{y}</b><br>Revenue: R$ %{x:,.0f}<br>"
                      "Orders: %{customdata[0]:,}<br>Review: %{customdata[1]:.2f}★<br>"
                      "Delivery: %{customdata[2]:.1f}d<extra></extra>"))
    fig.update_yaxes(autorange="reversed",tickfont=dict(size=11))
    fig.update_xaxes(tickprefix="R$ ",tickformat=",.0f")
    fig.update_layout(**_L(h=max(320,top_n*34),legend=False),
        title=dict(text=f"Top {top_n} product categories — colored by avg review",
                   font=dict(size=12,weight="bold"),x=0))
    st.plotly_chart(fig,use_container_width=True)
    dl(cat,"categories.csv")

def tab_geography(ycsv):
    reg=load_regions(ycsv)
    if reg.empty: st.info("No data."); return
    cl,cr=st.columns([1,2],gap="medium")
    with cl:
        sec("Revenue by region")
        agg=(reg.groupby("region").agg(revenue=("revenue","sum"),orders=("orders","sum"))
             .reset_index().sort_values("revenue",ascending=False))
        fig=go.Figure()
        for i,(idx,row) in enumerate(agg.iterrows()):
            fig.add_trace(go.Bar(x=[row["region"]],y=[row["revenue"]],
                name=row["region"],marker_color=PALETTE[i%len(PALETTE)],
                marker_line_width=0,customdata=[[row["orders"]]],
                hovertemplate=f"<b>{row['region']}</b><br>R$ %{{y:,.0f}}<br>"
                              f"Orders: %{{customdata[0][0]:,}}<extra></extra>"))
        fig.update_layout(**_L(h=240,legend=False),showlegend=False,bargap=0.3,
                          yaxis_tickprefix="R$ ",yaxis_tickformat=",.0f")
        st.plotly_chart(fig,use_container_width=True)
    with cr:
        sec("State breakdown")
        d=reg.copy()
        d["revenue"]=d["revenue"].apply(brl)
        d["orders"]=d["orders"].apply(num)
        d["avg_review"]=d["avg_review"].apply(lambda x:f"⭐ {x:.2f}")
        d["avg_days"]=d["avg_days"].apply(lambda x:f"{x:.1f} d")
        d=d.rename(columns={"state_name":"State","state":"Code","region":"Region",
                             "revenue":"Revenue","orders":"Orders",
                             "avg_review":"Review","avg_days":"Avg Delivery"})
        st.dataframe(d[["State","Code","Region","Revenue","Orders","Review","Avg Delivery"]],
                     use_container_width=True,height=320,hide_index=True)
    dl(reg,"geography.csv")

def tab_sellers(ycsv):
    sell=load_sellers(ycsv)
    if sell.empty: st.info("No data."); return
    tiers=["All"]+sorted(sell["tier"].dropna().unique().tolist())
    cf,_=st.columns([1,3])
    with cf: sel=st.selectbox("Filter by tier",tiers,key="tier")
    if sel!="All": sell=sell[sell["tier"]==sel]
    sec(f"Top sellers · {len(sell)} shown")
    d=sell.copy()
    d["tier"]=d["tier"].map(lambda t:f"{TIER_ICON.get(t,'🔵')} {t.title()}" if t else "—")
    d["gmv"]=d["gmv"].apply(brl)
    d["late_pct"]=d["late_pct"].apply(lambda x:f"{x:.1f}%")
    d["score"]=d["score"].apply(lambda x:f"⭐ {x:.2f}")
    d["days"]=d["days"].apply(lambda x:f"{x:.1f} d")
    d=d.rename(columns={"seller_id":"Seller","state":"State","tier":"Tier",
                         "orders":"Orders","gmv":"GMV","score":"Review",
                         "days":"Avg Delivery","late_pct":"Late %"})
    st.dataframe(d[["Seller","State","Tier","Orders","GMV","Review","Avg Delivery","Late %"]],
                 use_container_width=True,height=440,hide_index=True)
    dl(sell,"top_sellers.csv")

def tab_delivery(ycsv):
    df=load_delivery(ycsv)
    if df.empty: st.info("No data."); return
    c1,c2,c3=st.columns(3)
    c1.metric("Avg late rate",f"{df['late_pct'].mean():.1f}%")
    c2.metric("Avg delivery days",f"{df['avg_days'].mean():.1f}")
    c3.metric("States analyzed",str(len(df)))
    sec("Late delivery rate & avg delivery days by state")
    st.caption("🔴 Late > 15%  ·  🟡 > 8%  ·  🟢 Within target")
    fig=make_subplots(rows=1,cols=2,horizontal_spacing=0.16,
        subplot_titles=("Late delivery rate (%)","Avg delivery days"))
    late=df.sort_values("late_pct").tail(10)
    fig.add_trace(go.Bar(x=late["late_pct"],y=late["customer_state"],orientation="h",
        marker_color=[late_c(x) for x in late["late_pct"]],marker_line_width=0,
        hovertemplate="<b>%{y}</b>: %{x:.1f}%<extra></extra>",name="Late %"),row=1,col=1)
    days=df.sort_values("avg_days").tail(10)
    fig.add_trace(go.Bar(x=days["avg_days"],y=days["customer_state"],orientation="h",
        marker_color=C["blue"],marker_opacity=0.75,marker_line_width=0,
        hovertemplate="<b>%{y}</b>: %{x:.1f} days<extra></extra>",name="Days"),row=1,col=2)
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter,system-ui,sans-serif",size=12),
        margin=dict(l=0,r=0,t=36,b=0),height=300,showlegend=False)
    fig.update_xaxes(showgrid=False,zeroline=False)
    fig.update_yaxes(showgrid=False,tickfont=dict(size=10))
    st.plotly_chart(fig,use_container_width=True)
    dl(df,"delivery.csv")

def sidebar_controls():
    with st.sidebar:
        st.markdown("<div style='padding:8px 0 20px'>"
                    "<span style='font-size:1.3rem;font-weight:700;color:#FF6B35'>🛒 Olist</span>"
                    "<span style='font-size:0.75rem;color:#9CA3AF;margin-left:6px'>Analytics</span>"
                    "</div>",unsafe_allow_html=True)
        st.markdown("##### Filters")
        years=st.multiselect("Order year",[2016,2017,2018],default=[2017,2018])
        if not years: years=[2017,2018]
        top_n=st.slider("Top N categories",5,20,10)
        st.markdown("---")
        st.markdown("##### Options")
        show_raw=st.checkbox("Raw data explorer",False)
        st.markdown("---")
        if st.button("↺  Refresh data",use_container_width=True):
            st.cache_data.clear(); st.cache_resource.clear(); st.rerun()
        st.markdown("---")
        st.markdown("<div style='font-size:0.72rem;color:#9CA3AF;line-height:1.8'>"
                    "🗄️ Snowflake OLIST_DW<br>⚙️ AWS Glue · Step Functions<br>"
                    "🧪 dbt 9 models · 51 tests<br>"
                    "🐳 Docker · ECR · Streamlit Cloud</div>",unsafe_allow_html=True)
    return {"ycsv":",".join(str(y) for y in years),"top_n":top_n,"show_raw":show_raw}

def main():
    f=sidebar_controls()
    ts=load_freshness()
    render_header(ts)
    st.markdown("---")
    kpi=load_kpis(f["ycsv"])
    render_kpis(kpi)
    st.markdown("---")
    t1,t2,t3,t4,t5=st.tabs(["📈  Revenue","🏆  Categories",
                              "🗺️  Geography","🥇  Sellers","🚚  Delivery"])
    with t1: tab_revenue(f["ycsv"])
    with t2: tab_categories(f["ycsv"],f["top_n"])
    with t3: tab_geography(f["ycsv"])
    with t4: tab_sellers(f["ycsv"])
    with t5: tab_delivery(f["ycsv"])
    if f["show_raw"]:
        st.markdown("---")
        sec("Raw data explorer")
        cl,cr=st.columns([2,1])
        with cl: tbl=st.selectbox("Table",["fct_orders","fct_monthly_revenue",
                                            "dim_customers","dim_products","dim_sellers"])
        with cr: limit=st.slider("Rows",50,1000,200,50)
        raw=q(f"SELECT * FROM OLIST_DW.MARTS.{tbl} LIMIT {limit}",f"raw_{tbl}")
        if not raw.empty:
            st.dataframe(raw,use_container_width=True,height=320)
            dl(raw,f"{tbl}.csv",f"⬇️ Export {tbl}.csv")
    st.markdown("---")
    st.markdown(f"<p style='font-size:0.74rem;color:#9CA3AF;text-align:center'>"
                f"Olist Analytics · AWS + Snowflake + dbt + Streamlit · "
                f"Rendered {datetime.now(timezone.utc).strftime('%H:%M UTC')}</p>",
                unsafe_allow_html=True)

if __name__=="__main__":
    main()