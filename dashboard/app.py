import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

# 1. Settings & Connection
st.set_page_config(page_title="Olist E-Commerce Analytics", layout="wide")
load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

def get_connection():
    # Get environment variable if in Docker, otherwise use local settings
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433") # 5433 for local, 5432 for Docker
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

engine = get_connection()

st.title("E-Commerce Analytics Dashboard")
st.markdown("---")

# 2. Top Metrics (KPIs)
st.subheader("Key Performance Indicators")
col1, col2, col3, col4, col5 = st.columns(5)

# Fetching summary data from Sales, Logistics Performance and Category Insights
df_sales = pd.read_sql("SELECT * FROM gold.view_sales_performance", engine)
df_logistics = pd.read_sql("SELECT * FROM gold.view_logistics_performance LIMIT 10", engine)
df_cat = pd.read_sql("SELECT * FROM gold.view_category_insights", engine)
df_review = pd.read_sql("SELECT AVG(review_score) as avg_score FROM gold.fact_orders", engine)
df_delivery = pd.read_sql("SELECT AVG(delivery_time_days) as avg_delivery FROM gold.fact_orders WHERE order_status = 'Delivered'", engine)

# latest_revenue = df_sales['total_revenue'].iloc[-1] # Monthly revenue is low because there is only 1 sale. Since September 2018 data is corrupted, showing "Total Revenue" is more reliable than "Last Month"
total_revenue = df_sales['total_revenue'].sum()
total_orders = df_sales['total_orders'].sum() #Correct
clean_growth = df_sales[df_sales['revenue_growth_pct'] < 500]['revenue_growth_pct'].mean() #Correct
avg_review_score = float(df_review['avg_score'].iloc[0])
avg_delivery = float(df_delivery['avg_delivery'].iloc[0])

col1.metric("Total Orders", f"{total_orders:,}")
col2.metric("Total Revenue", f"${total_revenue:,.2f}")
col3.metric("Avg. Monthly Growth", f"{clean_growth:.2f}%")
col4.metric("Avg. Review Score", f"{avg_review_score:.2f} / 5")
col5.metric("Avg. Delivery Time (Days)", f"{avg_delivery:.2f}")

st.markdown("---")

# 3. Visualizations
left_column, right_column = st.columns(2)

with left_column:
    st.subheader("Monthly Revenue Trend")
    # Created a new column by combining year and month (e.g. "2017-January")
    df_sales['year_month'] = df_sales['year'].astype(str) + "-" + df_sales['month_name']
    df_sales_filtered = df_sales.iloc[:-1]  # Removed the last month (September 2018) because the data is incomplete and was skewing the graph
    
    fig_rev = px.line(df_sales_filtered, 
                      x="year_month", 
                      y="total_revenue", 
                      title="Revenue over Months", 
                      markers=True)

    fig_rev.update_xaxes(type='category') # Ensures chronological order on X-axis
    st.plotly_chart(fig_rev, use_container_width=True)
    
with right_column:
    st.subheader("Logistics Performance by State")
    fig_log = px.bar(df_logistics, x="state", y="delay_rate_pct", 
                     title="Top 10 States by Delay Rate (%)", color="delay_rate_pct")
    st.plotly_chart(fig_log, use_container_width=True)

st.markdown("---")

col_left, col_right = st.columns(2)

with col_left:
    fig_pie = px.pie(
        df_cat.head(5), 
        values='total_item_revenue', 
        names='category_name_en', 
        title='Top 5 Categories by Revenue', 
        hole=.4
    )
    # Control chart size
    fig_pie.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=350)
    st.plotly_chart(fig_pie, use_container_width=True)

with col_right:
    # Make gauge chart smaller and remove extra spacing
    fig_gauge = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = avg_review_score,
        title = {'text': "Customer Satisfaction (CSAT)", 'font': {'size': 18}},
        gauge = {
            'axis': {'range': [1, 5], 'tickwidth': 1},
            'bar': {'color': "#2c3e50"},
            'steps' : [
                {'range': [1, 3], 'color': "#ff4b4b"},
                {'range': [3, 4], 'color': "#ffa500"},
                {'range': [4, 5], 'color': "#00cc96"}
            ],
            'threshold': {
                'line': {'color': "black", 'width': 3},
                'thickness': 0.75,
                'value': 4.5
            }
        }
    ))
    
    # Match gauge size with pie chart
    fig_gauge.update_layout(margin=dict(t=50, b=0, l=30, r=30), height=350)
    st.plotly_chart(fig_gauge, use_container_width=True)

# 4. Detailed Table
st.markdown("---")
st.subheader("Category Insights")
st.dataframe(df_cat, use_container_width=True)

# 5. Map Visualization
st.subheader("Customer Distribution Map")
# Fetch only customers with valid coordinates (NOT NULL)
df_map = pd.read_sql("""
    SELECT latitude, longitude 
    FROM gold.dim_customers 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL 
    ORDER BY RANDOM()  
    LIMIT 10000
""", engine)

# Random sampling used for a more balanced distribution since full dataset is too large
st.map(df_map)