import streamlit as st
import numpy as np
from datetime import datetime, timedelta
import snowflake.snowpark.modin.plugin
import modin.pandas as pd
from snowflake.snowpark.session import Session
session = Session.builder.create()
# Simple Streamlit app title
st.title("Simple pandas on Snowflake Streamlit app")    
st.markdown("This app demonstrates how to use pandas on Snowflake with Streamlit. Feel free to use as template and build your own app.")


# Generate sample data (simulating Snowflake data)
def generate_synthetic_table(N,name): # Run the following to generate a synthetic dataset with X rows of transactions (from 2024-2025 current date)
    session.sql(f'''
    CREATE OR REPLACE TABLE revenue_transactions_{name} (
        Transaction_ID STRING,
        Date DATE,
        Revenue FLOAT
    );''').collect()
    session.sql('''SET num_days = (SELECT DATEDIFF(DAY, '2024-01-01', CURRENT_DATE));''').collect()
    session.sql(f'''INSERT INTO revenue_transactions_{name} (Transaction_ID, Date, Revenue)
    SELECT
        UUID_STRING() AS Transaction_ID,
        DATEADD(DAY, UNIFORM(0, $num_days, RANDOM()), '2024-01-01') AS Date,
        UNIFORM(10, 1000, RANDOM()) AS Revenue
    FROM TABLE(GENERATOR(ROWCOUNT => {N}));
    ''').collect()
@st.cache_data  # Cache the function to avoid re-running it on every page refresh
def create_table():
    generate_synthetic_table(10000000, "10M")

# Create table revenue_transactions_10M
create_table()


df = pd.read_snowflake("revenue_transactions_10M")

# Display data
st.subheader("Display dataframe sample")
st.dataframe(df) 

# Simple metrics
col1, col2, col3 = st.columns(3)
with col1:
    total_revenue = df['REVENUE'].sum()
    if total_revenue >= 1_000_000_000:
        formatted_revenue = f"${total_revenue/1_000_000_000:.2f}B"
    elif total_revenue >= 1_000_000:
        formatted_revenue = f"${total_revenue/1_000_000:.2f}M"
    elif total_revenue >= 1_000:
        formatted_revenue = f"${total_revenue/1_000:.2f}K"
    else:
        formatted_revenue = f"${total_revenue:,.2f}"
    st.metric("Total Revenue", formatted_revenue)
with col2:
    st.metric("Total Records", len(df))
with col3:
    st.metric("Average Revenue", f"${df['REVENUE'].mean():.2f}")

# Simple chart - Revenue by Month
st.subheader("Example Matplotlib Chart: Revenue by Month")
import matplotlib.pyplot as plt
df['Month'] = pd.to_datetime(df['DATE']).dt.month
monthly_revenue = df.groupby('Month')['REVENUE'].sum()

fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(monthly_revenue.index, monthly_revenue.values, marker='o', linewidth=2, markersize=6)
ax.set_xlabel('Month')
ax.set_ylabel('Total Revenue ($)')
ax.set_title('Revenue by Month')
ax.grid(True, alpha=0.3)
ax.ticklabel_format(style='plain', axis='y')
plt.tight_layout()
st.pyplot(fig)
# Additional chart - Revenue distribution
import altair as alt
st.subheader("Example Altair Chart: Revenue Distribution")
chart = alt.Chart(df).mark_bar().add_selection(
    alt.selection_interval()
).encode(
    alt.X('REVENUE:Q', bin=alt.Bin(maxbins=50), title='Revenue'),
    alt.Y('count()', title='Count'),
    tooltip=['count()']
).properties(
    width=600,
    height=300,
    title='Distribution of Revenue Amounts'
)
st.altair_chart(chart, use_container_width=True)

# Filter by date range
st.subheader("Filter by Date Range")
min_date = pd.to_datetime(df['DATE']).min().date()
max_date = pd.to_datetime(df['DATE']).max().date()

date_range = st.date_input(
    "Select date range:",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = df[(pd.to_datetime(df['DATE']).dt.date >= start_date) & 
                     (pd.to_datetime(df['DATE']).dt.date <= end_date)]
    
    st.write(f"Data from {start_date} to {end_date}:")
    st.write(f"Total Revenue in period: ${filtered_df['REVENUE'].sum():,.2f}")
    st.dataframe(filtered_df.head(100))  # Show first 100 rows of filtered data

