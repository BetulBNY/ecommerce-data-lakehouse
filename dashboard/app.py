import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import os
from dotenv import load_dotenv

# 1. Ayarlar & Bağlantı
st.set_page_config(page_title="Olist E-Commerce Analytics", layout="wide")
load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

def get_connection():
    # Docker ortamındaysa ortam değişkenini al, yoksa local ayarı kullan
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433") # Local için 5433, Docker içi için 5432
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

engine = get_connection()

st.title("📊 Olist Data Lakehouse Dashboard")
st.markdown("---")

# 2. Üst Metrikler (KPIs)
st.subheader("Key Performance Indicators")
col1, col2, col3 = st.columns(3)

# View Sales Performance'dan özet veriler çekme
df_sales = pd.read_sql("SELECT * FROM gold.view_sales_performance", engine)
# latest_revenue = df_sales['total_revenue'].iloc[-1] # Son ayın geliri düşük çünkü 1 satış var. Eylül 2018 verisi bozuk olduğu için, "Son Ay" yerine "Total Revenue" (Tüm Zamanlar Cirosu) göstermek daha sağlıklı
total_revenue = df_sales['total_revenue'].sum()
total_orders = df_sales['total_orders'].sum() #Doğru
avg_growth = df_sales['revenue_growth_pct'].mean()

col1.metric("Total Orders", f"{total_orders:,}")
col2.metric("Total Revenue", f"${total_revenue:,.2f}")
col3.metric("Avg. Monthly Growth", f"{avg_growth:.2f}%")

st.markdown("---")

# 3. Görselleştirmeler
left_column, right_column = st.columns(2)

with left_column:
    st.subheader("Monthly Revenue Trend")
    fig_rev = px.line(df_sales, x="month_name", y="total_revenue", 
                      title="Revenue over Months", markers=True)
    st.plotly_chart(fig_rev, use_container_width=True)

with right_column:
    st.subheader("Logistics Performance by State")
    df_logistics = pd.read_sql("SELECT * FROM gold.view_logistics_performance LIMIT 10", engine)
    fig_log = px.bar(df_logistics, x="state", y="delay_rate_pct", 
                     title="Top 10 States by Delay Rate (%)", color="delay_rate_pct")
    st.plotly_chart(fig_log, use_container_width=True)

# 4. Detaylı Tablo
st.markdown("---")
st.subheader("Category Insights")
df_cat = pd.read_sql("SELECT * FROM gold.view_category_insights", engine)
st.dataframe(df_cat, use_container_width=True)