import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go  
from datetime import datetime, timedelta

# Set up Streamlit app
st.set_page_config(page_title="Starknify Data viewer", layout="wide")

# Function to connect to Snowflake using secrets
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )
    
    # Explicitly set the session context
    conn.cursor().execute(f"USE WAREHOUSE {st.secrets['snowflake']['warehouse']}")
    conn.cursor().execute(f"USE DATABASE {st.secrets['snowflake']['database']}")
    conn.cursor().execute(f"USE SCHEMA {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}")
    
    return conn

# Function to get all tables in the schema
def get_tables(conn):
    query = f"SHOW TABLES IN SCHEMA {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}"
    df = pd.read_sql(query, conn)
    return df['name'].tolist()

# Function to get columns for a selected table
def get_columns(conn, table_name):
    query = f"SHOW COLUMNS IN TABLE {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{table_name}"
    df = pd.read_sql(query, conn)
    return df['column_name'].tolist()

# Function to get data based on selected table, columns, and date range
def get_data(conn, table_name, columns, start_date, end_date):
    columns_str = ', '.join(columns)
    query = f"""
        SELECT {columns_str}, BLOCK_DATE
        FROM {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{table_name}
        WHERE BLOCK_DATE BETWEEN '{start_date}' AND '{end_date}'
    """
    df = pd.read_sql(query, conn)
    return df

# Function to determine x-axis interval and generate time series based on date range
def generate_time_series(start_date, end_date):
    days_diff = (end_date - start_date).days
    
    if days_diff == 0:
        # Same day, use hourly increments
        interval = 'H'
        times = pd.date_range(start=start_date, end=end_date, freq=interval)
    elif days_diff <= 10:
        interval = 'D'
        times = pd.date_range(start=start_date, end=end_date, freq=interval)
    elif days_diff <= 30:
        interval = '2D'
        times = pd.date_range(start=start_date, end=end_date, freq=interval)
    elif days_diff <= 60:
        interval = '5D'
        times = pd.date_range(start=start_date, end=end_date, freq=interval)
    else:
        interval = 'M'
        times = pd.date_range(start=start_date, end=end_date, freq=interval)
    
    return times, interval

# Function to resample data based on the generated time series
def resample_data(df, interval, columns):
    df.set_index('BLOCK_DATE', inplace=True)
    resampled_df = df.resample(interval).mean()  
    resampled_df.reset_index(inplace=True)
    return resampled_df

# Main app
st.title("Starknify Data viewer")

# Get connection to Snowflake
conn = get_snowflake_connection()

# Dropdown to select table
tables = get_tables(conn)
selected_table = st.selectbox("Select a table", tables)

# Dropdown to select columns from the table
if selected_table:
    columns = get_columns(conn, selected_table)
    selected_columns = st.multiselect("Select columns", columns)

# Date input for start and end date
start_date = st.date_input("Start date", datetime(2023, 1, 1))
end_date = st.date_input("End date", datetime.now())

# Button to fetch data and plot
if st.button("Fetch Data and Plot"):
    if selected_table and selected_columns and start_date and end_date:
        data = get_data(conn, selected_table, selected_columns, start_date, end_date)
        st.dataframe(data)
        
        if 'BLOCK_DATE' in data.columns:
            data['BLOCK_DATE'] = pd.to_datetime(data['BLOCK_DATE'])
            data.sort_values('BLOCK_DATE', inplace=True)
            
            times, interval = generate_time_series(start_date, end_date)
            resampled_data = resample_data(data, interval, selected_columns)
            
            # Plotting using graph_objects for more control
            fig = go.Figure()
            
            for column in selected_columns:
                fig.add_trace(go.Scatter(
                    x=resampled_data['BLOCK_DATE'],
                    y=resampled_data[column],
                    mode='lines+markers',
                    name=column
                ))
            
            # Customizing layout to mimic the provided graph
            fig.update_layout(
                title=f"Data from {selected_table} aggregated by {interval}",
                xaxis_title='Time',
                yaxis_title='Values',
                template='simple_white',
                xaxis=dict(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor='LightGray'
                ),
                yaxis=dict(
                    showgrid=True,
                    gridwidth=1,
                    gridcolor='LightGray'
                )
            )
            
            st.plotly_chart(fig)
        else:
            st.warning("The selected data does not have a 'BLOCK_DATE' column.")
    else:
        st.warning("Please select a table, columns, and date range.")


conn.close()
