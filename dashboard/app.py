import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# --- Page Config ---
st.set_page_config(
    page_title="E-Commerce Batch ETL Insights",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom Styling & Glassmorphism Theme ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
    }
    
    /* Global Background and Text */
    .stApp {
        background-color: #0b0f19;
        color: #f1f5f9;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #111827;
        border-right: 1px solid #1f2937;
    }
    
    /* Title header */
    .main-title {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #a78bfa 0%, #3b82f6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 20px;
    }
    
    /* Subtitle */
    .subtitle {
        font-size: 1.1rem;
        color: #9ca3af;
        margin-bottom: 30px;
    }
    
    /* Custom KPI Card Styling */
    .kpi-card {
        background: rgba(30, 41, 59, 0.45);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 16px;
        padding: 24px;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-4px);
        border-color: rgba(167, 139, 250, 0.4);
    }
    .kpi-label {
        font-size: 0.875rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #9ca3af;
        margin-bottom: 8px;
    }
    .kpi-value {
        font-size: 2.25rem;
        font-weight: 700;
        color: #ffffff;
        line-height: 1;
    }
    .kpi-trend {
        font-size: 0.875rem;
        margin-top: 8px;
        font-weight: 500;
    }
    .trend-up {
        color: #10b981;
    }
    
    /* Table headers */
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
    }
    </style>
""", unsafe_allow_html=True)

# --- Database Connection ---
@st.cache_resource
def get_db_engine():
    # Inside docker: postgres is the host.
    # Outside docker (e.g. running streamlit locally): localhost is host.
    db_host = os.environ.get("POSTGRES_HOST", "postgres")
    db_port = os.environ.get("POSTGRES_PORT", "5432")
    db_name = os.environ.get("POSTGRES_DB", "warehouse")
    db_user = os.environ.get("POSTGRES_USER", "postgres")
    db_pass = os.environ.get("POSTGRES_PASSWORD", "postgres")
    
    connection_string = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return create_engine(connection_string)

engine = get_db_engine()

# --- Load Data Helpers ---
def load_data(query):
    try:
        return pd.read_sql(query, con=engine)
    except Exception as e:
        return None

# --- Main Dashboard ---
st.markdown("<h1 class='main-title'>⚡ Real-Time E-Commerce ETL Insights</h1>", unsafe_allow_html=True)
st.markdown("<p class='subtitle'>Monitoring batch pipeline ingestion, processing, and warehouse metrics from MinIO (S3) & PySpark.</p>", unsafe_allow_html=True)

# Sidebar Info
st.sidebar.markdown("<h2 style='color:#a78bfa;'>⚙️ Pipeline Control</h2>", unsafe_allow_html=True)
st.sidebar.markdown("---")

# Query to check available dates
dates_df = load_data("SELECT DISTINCT date FROM daily_active_users ORDER BY date DESC")

if dates_df is None or dates_df.empty:
    st.warning("⚠️ No data found in the PostgreSQL warehouse yet.")
    st.info("💡 Make sure the Airflow DAG has successfully run the Spark job and loaded the data into PostgreSQL. You can monitor progress on the [Airflow UI](http://localhost:8080).")
    
    # Render fallback/mock metrics and charts for presentation purposes
    st.markdown("### 📋 Preview Dashboard (Mock Data)")
    st.write("This preview shows what the dashboard will look like once your Airflow DAG completes its run.")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown("""<div class='kpi-card'><div class='kpi-label'>Total Active Users</div><div class='kpi-value'>42,519</div><div class='kpi-trend trend-up'>↑ 12.4% vs yesterday</div></div>""", unsafe_allow_html=True)
    with col2:
        st.markdown("""<div class='kpi-card'><div class='kpi-label'>Total Purchases</div><div class='kpi-value'>2,940</div><div class='kpi-trend trend-up'>↑ 8.1% vs yesterday</div></div>""", unsafe_allow_html=True)
    with col3:
        st.markdown("""<div class='kpi-card'><div class='kpi-label'>Total Revenue</div><div class='kpi-value'>$164,810</div><div class='kpi-trend trend-up'>↑ 15.2% vs yesterday</div></div>""", unsafe_allow_html=True)
    with col4:
        st.markdown("""<div class='kpi-card'><div class='kpi-label'>Conversion Rate</div><div class='kpi-value'>3.12%</div><div class='kpi-trend trend-up'>↑ 0.4% vs yesterday</div></div>""", unsafe_allow_html=True)
        
    st.markdown("<br><br>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        # Mock Funnel
        fig = go.Figure(go.Funnel(
            y=["Page Views", "Clicks", "Add to Cart", "Purchases"],
            x=[600000, 250000, 120000, 30000],
            textinfo="value+percent initial",
            marker={"color": ["#3b82f6", "#60a5fa", "#a78bfa", "#f43f5e"]}
        ))
        fig.update_layout(
            title="Conversion Funnel (Mock)",
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family="Outfit")
        )
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        # Mock Category Sales
        df_mock_sales = pd.DataFrame({
            "Category": ["Electronics", "Apparel", "Home", "Books", "Beauty"],
            "Revenue": [65000, 35000, 30000, 15000, 19810]
        })
        fig = px.pie(
            df_mock_sales, values="Revenue", names="Category", hole=0.4,
            color_discrete_sequence=["#a78bfa", "#3b82f6", "#10b981", "#f59e0b", "#ec4899"]
        )
        fig.update_layout(
            title="Revenue Share by Product Category (Mock)",
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family="Outfit")
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    # Filter selection
    available_dates = sorted([d.strftime("%Y-%m-%d") for d in dates_df["date"]], reverse=True)
    selected_date = st.sidebar.selectbox("📅 Select Date View", available_dates)
    
    st.sidebar.success(f"Loaded records for: {selected_date}")
    
    # Fetch data for specific date
    dau_query = f"SELECT * FROM daily_active_users WHERE date = '{selected_date}'"
    sales_query = f"SELECT * FROM product_sales WHERE date = '{selected_date}'"
    funnel_query = f"SELECT * FROM funnel_metrics WHERE date = '{selected_date}'"
    
    dau_data = load_data(dau_query)
    sales_data = load_data(sales_query)
    funnel_data = load_data(funnel_query)
    
    # 1. Row KPI Cards
    if dau_data is not None and not dau_data.empty:
        total_users = dau_data.iloc[0]["unique_users"]
        total_events = dau_data.iloc[0]["total_events"]
    else:
        total_users, total_events = 0, 0
        
    if funnel_data is not None and not funnel_data.empty:
        total_purchases = funnel_data["purchases"].sum()
        total_page_views = funnel_data["page_views"].sum()
        overall_conv_rate = (total_purchases / total_page_views * 100) if total_page_views > 0 else 0
    else:
        total_purchases, total_page_views, overall_conv_rate = 0, 0, 0
        
    if sales_data is not None and not sales_data.empty:
        total_revenue = sales_data["revenue"].sum()
    else:
        total_revenue = 0
        
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-label'>Daily Active Users (DAU)</div>
                <div class='kpi-value'>{total_users:,}</div>
                <div class='kpi-trend trend-up'>🚀 Active Sessions</div>
            </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-label'>Total Purchases</div>
                <div class='kpi-value'>{total_purchases:,}</div>
                <div class='kpi-trend trend-up'>🛍️ Conversions</div>
            </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-label'>Total Sales Revenue</div>
                <div class='kpi-value'>${total_revenue:,.2f}</div>
                <div class='kpi-trend trend-up'>💰 Gross Sales</div>
            </div>
        """, unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-label'>Conversion Rate</div>
                <div class='kpi-value'>{overall_conv_rate:.2f}%</div>
                <div class='kpi-trend trend-up'>🎯 Purchase Rate</div>
            </div>
        """, unsafe_allow_html=True)
        
    st.markdown("<br><br>", unsafe_allow_html=True)
    
    # 2. Main Row Visualizations (Funnel & DAU trend)
    col_left, col_right = st.columns(2)
    
    with col_left:
        # Funnel chart
        if funnel_data is not None and not funnel_data.empty:
            agg_funnel = funnel_data.sum(numeric_only=True)
            stages = ["Page Views", "Clicks", "Add to Cart", "Purchases"]
            counts = [
                int(agg_funnel.get("page_views", 0)),
                int(agg_funnel.get("clicks", 0)),
                int(agg_funnel.get("adds_to_cart", 0)),
                int(agg_funnel.get("purchases", 0))
            ]
            
            fig_funnel = go.Figure(go.Funnel(
                y=stages,
                x=counts,
                textinfo="value+percent initial",
                marker={"color": ["#6366f1", "#4f46e5", "#818cf8", "#f43f5e"]}
            ))
            fig_funnel.update_layout(
                title=f"User Conversion Funnel for {selected_date}",
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(family="Outfit")
            )
            st.plotly_chart(fig_funnel, use_container_width=True)
        else:
            st.info("Funnel data loading failed.")
            
    with col_right:
        # Device traffic share
        if funnel_data is not None and not funnel_data.empty:
            fig_device = px.pie(
                funnel_data, values="page_views", names="device", hole=0.5,
                color_discrete_sequence=["#6366f1", "#3b82f6", "#14b8a6"]
            )
            fig_device.update_layout(
                title=f"Traffic Distribution by Device",
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(family="Outfit")
            )
            st.plotly_chart(fig_device, use_container_width=True)
            
    # 3. Third Row - Product Analysis
    st.markdown("<br><br>", unsafe_allow_html=True)
    col_prod_left, col_prod_right = st.columns([1, 1])
    
    with col_prod_left:
        # Sales by Product Category
        if sales_data is not None and not sales_data.empty:
            cat_sales = sales_data.groupby("category")["revenue"].sum().reset_index()
            fig_cat = px.bar(
                cat_sales.sort_values(by="revenue", ascending=True),
                x="revenue", y="category", orientation="h",
                color="revenue",
                color_continuous_scale="Viridis",
                labels={"revenue": "Revenue ($)", "category": "Category"}
            )
            fig_cat.update_layout(
                title="Revenue by Product Category",
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(family="Outfit"),
                coloraxis_showscale=False
            )
            st.plotly_chart(fig_cat, use_container_width=True)
            
    with col_prod_right:
        # Top 10 selling products
        if sales_data is not None and not sales_data.empty:
            top_prods = sales_data.sort_values(by="revenue", ascending=False).head(10)
            fig_prods = px.bar(
                top_prods, x="product_id", y="revenue",
                color="units_sold",
                color_continuous_scale="Purples",
                labels={"revenue": "Revenue ($)", "product_id": "Product ID", "units_sold": "Units Sold"}
            )
            fig_prods.update_layout(
                title="Top 10 Products by Revenue",
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(family="Outfit")
            )
            st.plotly_chart(fig_prods, use_container_width=True)

    # 4. Pipeline Trend History
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown("### 📈 Historical DAU Trend")
    all_daus = load_data("SELECT date, unique_users, total_events FROM daily_active_users ORDER BY date ASC")
    if all_daus is not None and not all_daus.empty:
        fig_trend = px.line(
            all_daus, x="date", y=["unique_users", "total_events"],
            labels={"value": "Count", "date": "Date", "variable": "Metric"},
            color_discrete_sequence=["#a78bfa", "#3b82f6"]
        )
        fig_trend.update_layout(
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family="Outfit")
        )
        st.plotly_chart(fig_trend, use_container_width=True)

# Add health metrics in the sidebar
st.sidebar.markdown("<br><br><h3>🖥️ System Nodes</h3>", unsafe_allow_html=True)
st.sidebar.info("""
- **Airflow**: http://localhost:8080
- **MinIO Console**: http://localhost:9001
- **Spark Master UI**: http://localhost:8081
- **Postgres DB**: localhost:5432
""")
