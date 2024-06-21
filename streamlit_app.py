import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime

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
        SELECT {columns_str}
        FROM {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{table_name}
        WHERE BLOCK_DATE BETWEEN '{start_date}' AND '{end_date}'
    """
    df = pd.read_sql(query, conn)
    return df

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

# Button to fetch data
if st.button("Fetch Data"):
    if selected_table and selected_columns and start_date and end_date:
        data = get_data(conn, selected_table, selected_columns, start_date, end_date)
        st.dataframe(data)
    else:
        st.warning("Please select a table, columns, and date range.")

# Close the Snowflake connection
conn.close()
