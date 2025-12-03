cat > app.py << 'EOF'
import streamlit as st
import pandas as pd
import psycopg2
import os
from datetime import datetime

# Page config
st.set_page_config(
    page_title="WindBorne Finance",
    page_icon="📊",
    layout="wide"
)

# Database connection
@st.cache_resource
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', 5432),
            database=os.getenv('POSTGRES_DB', 'windborne_finance'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# Test connection
def test_connection():
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()
            return True, f"Connected! PostgreSQL {version[0][:20]}..."
        except Exception as e:
            return False, str(e)
    return False, "Connection is None"

# Main app
def main():
    st.title("📊 WindBorne Finance Dashboard")
    
    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Select Page", ["Home", "Companies", "Metrics", "ETL Logs"])
    
    # Connection test
    with st.sidebar.expander("🔌 Database Status"):
        success, msg = test_connection()
        if success:
            st.success(msg)
        else:
            st.error(msg)
    
    # Pages
    if page == "Home":
        show_home()
    elif page == "Companies":
        show_companies()
    elif page == "Metrics":
        show_metrics()
    else:
        show_etl_logs()

def show_home():
    st.header("Welcome to WindBorne Finance")
    
    col1, col2, col3 = st.columns(3)
    
    conn = get_db_connection()
    if not conn:
        st.error("Cannot connect to database")
        return
    
    try:
        # Count companies
        df = pd.read_sql("SELECT COUNT(*) as count FROM companies", conn)
        col1.metric("Companies", df['count'].iloc[0])
        
        # Count metrics
        df = pd.read_sql("SELECT COUNT(DISTINCT fiscal_year) as count FROM calculated_metrics", conn)
        col2.metric("Years of Data", df['count'].iloc[0])
        
        # Last ETL run
        df = pd.read_sql("SELECT run_date FROM etl_runs ORDER BY run_date DESC LIMIT 1", conn)
        if not df.empty:
            last_run = df['run_date'].iloc[0]
            col3.metric("Last ETL Run", last_run.strftime("%Y-%m-%d %H:%M"))
        else:
            col3.metric("Last ETL Run", "Never")
            
    except Exception as e:
        st.error(f"Error: {e}")

def show_companies():
    st.header("📈 Companies")
    
    conn = get_db_connection()
    if not conn:
        st.error("Cannot connect to database")
        return
    
    try:
        df = pd.read_sql("""
            SELECT 
                symbol,
                name,
                sector,
                industry,
                updated_at
            FROM companies
            ORDER BY symbol
        """, conn)
        
        if df.empty:
            st.warning("No companies found. Run ETL first!")
        else:
            st.dataframe(df, use_container_width=True)
            
    except Exception as e:
        st.error(f"Error: {e}")

def show_metrics():
    st.header("💰 Financial Metrics")
    
    conn = get_db_connection()
    if not conn:
        st.error("Cannot connect to database")
        return
    
    try:
        # Get available companies and years
        companies_df = pd.read_sql("SELECT DISTINCT symbol FROM companies ORDER BY symbol", conn)
        years_df = pd.read_sql("SELECT DISTINCT fiscal_year FROM calculated_metrics ORDER BY fiscal_year DESC", conn)
        
        if companies_df.empty or years_df.empty:
            st.warning("No data available. Run ETL first!")
            return
        
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            selected_companies = st.multiselect(
                "Select Companies",
                companies_df['symbol'].tolist(),
                default=companies_df['symbol'].tolist()
            )
        
        with col2:
            selected_years = st.multiselect(
                "Select Years",
                years_df['fiscal_year'].tolist(),
                default=years_df['fiscal_year'].tolist()[:3]
            )
        
        if not selected_companies or not selected_years:
            st.info("Please select at least one company and year")
            return
        
        # Query metrics
        companies_str = "','".join(selected_companies)
        years_str = ",".join(map(str, selected_years))
        
        query = f"""
            SELECT 
                c.symbol,
                c.name,
                cm.fiscal_year,
                cm.metric_name,
                cm.metric_value,
                cm.metric_category
            FROM calculated_metrics cm
            JOIN companies c ON cm.company_id = c.id
            WHERE c.symbol IN ('{companies_str}')
            AND cm.fiscal_year IN ({years_str})
            ORDER BY c.symbol, cm.fiscal_year DESC, cm.metric_name
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            st.warning("No metrics found for selected filters")
        else:
            st.success(f"Found {len(df)} metrics")
            
            # Pivot table for better view
            pivot_df = df.pivot_table(
                index=['symbol', 'fiscal_year'],
                columns='metric_name',
                values='metric_value',
                aggfunc='first'
            ).reset_index()
            
            st.dataframe(pivot_df, use_container_width=True)
            
    except Exception as e:
        st.error(f"Error: {e}")
        import traceback
        st.code(traceback.format_exc())

def show_etl_logs():
    st.header("📋 ETL Execution Logs")
    
    conn = get_db_connection()
    if not conn:
        st.error("Cannot connect to database")
        return
    
    try:
        df = pd.read_sql("""
            SELECT 
                run_date,
                workflow_name,
                companies_processed,
                api_calls_made,
                api_failures,
                execution_time_seconds,
                status
            FROM etl_runs
            ORDER BY run_date DESC
            LIMIT 20
        """, conn)
        
        if df.empty:
            st.info("No ETL runs yet")
        else:
            # Metrics
            col1, col2, col3, col4 = st.columns(4)
            
            total_runs = len(df)
            success_rate = (df['status'] == 'SUCCESS').sum() / total_runs * 100
            avg_time = df['execution_time_seconds'].mean()
            last_run = df['run_date'].iloc[0]
            
            col1.metric("Total Runs", total_runs)
            col2.metric("Success Rate", f"{success_rate:.1f}%")
            col3.metric("Avg Time", f"{avg_time:.0f}s")
            col4.metric("Last Run", last_run.strftime("%Y-%m-%d"))
            
            # Table
            st.dataframe(df, use_container_width=True)
            
    except Exception as e:
        st.error(f"Error: {e}")

if __name__ == "__main__":
    main()
EOF
