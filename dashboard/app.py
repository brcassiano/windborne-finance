import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import os
from datetime import datetime
import time


# Page config
st.set_page_config(
    page_title="WindBorne Finance",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #262730;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #4a4a55;
    }
    .stMetric label {
        font-size: 0.9rem !important;
        color: #b3b3b3;
    }
    .stMetric [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
    }
    div[data-testid="stExpander"] {
        border: 1px solid #4a4a55;
        border-radius: 8px;
    }
    </style>
""", unsafe_allow_html=True)


# Database connection with shorter cache time for refresh
@st.cache_resource(ttl=60)
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
        st.error(f"❌ Database connection failed: {e}")
        return None


# Test connection
def test_connection():
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()
            return True, f"PostgreSQL Connected"
        except Exception as e:
            return False, str(e)
    return False, "Connection is None"


def show_system_health():
    """System health and ETL monitoring"""
    st.markdown("## 🔧 System Health")
    
    conn = get_db_connection()
    if not conn:
        st.error("❌ Cannot connect to database")
        return
    
    try:
        # Latest ETL run
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
            LIMIT 1
        """, conn)
        
        if not df.empty:
            st.markdown("### 📊 Latest Execution")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                status = df['status'].iloc[0]
                st.metric(
                    "Status",
                    status,
                    delta="OK" if status == "SUCCESS" else "ERROR",
                    delta_color="normal" if status == "SUCCESS" else "inverse"
                )
            
            with col2:
                st.metric("Last Run", df['run_date'].iloc[0].strftime("%Y-%m-%d %H:%M"))
            
            with col3:
                st.metric("Duration", f"{df['execution_time_seconds'].iloc[0]}s")
            
            with col4:
                st.metric("Companies", df['companies_processed'].iloc[0])
            
            # API stats
            col5, col6 = st.columns(2)
            with col5:
                st.metric("API Calls", df['api_calls_made'].iloc[0])
            with col6:
                st.metric("API Failures", df['api_failures'].iloc[0])
        else:
            st.warning("⚠️ No ETL runs found. Run ETL pipeline first!")
        
        st.markdown("---")
        
        # Execution history
        st.markdown("### 📊 Execution History (Last 30 days)")
        
        df_history = pd.read_sql("""
            SELECT 
                run_date as "Date",
                status as "Status",
                companies_processed as "Companies",
                execution_time_seconds as "Duration (s)",
                api_calls_made as "API Calls",
                api_failures as "Failures"
            FROM etl_runs
            WHERE run_date > NOW() - INTERVAL '30 days'
            ORDER BY run_date DESC
        """, conn)
        
        if not df_history.empty:
            # Apply styling to status column
            def highlight_status(val):
                if val == 'SUCCESS':
                    return 'background-color: #28a745; color: white'
                elif val == 'FAILED':
                    return 'background-color: #dc3545; color: white'
                return ''
            
            styled_df = df_history.style.applymap(
                highlight_status, 
                subset=['Status']
            )
            
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            # Success rate chart
            st.markdown("### 📈 Execution Timeline")
            
            # Prepare data for chart
            chart_data = pd.read_sql("""
                SELECT 
                    run_date,
                    execution_time_seconds,
                    status
                FROM etl_runs
                WHERE run_date > NOW() - INTERVAL '30 days'
                ORDER BY run_date ASC
            """, conn)
            
            if not chart_data.empty:
                fig = go.Figure()
                
                # Color by status
                colors = chart_data['status'].map({
                    'SUCCESS': '#28a745',
                    'FAILED': '#dc3545'
                })
                
                fig.add_trace(go.Scatter(
                    x=chart_data['run_date'],
                    y=chart_data['execution_time_seconds'],
                    mode='lines+markers',
                    name='Execution Time',
                    line=dict(color='#636EFA', width=2),
                    marker=dict(
                        color=colors,
                        size=12,
                        line=dict(width=2, color='white')
                    ),
                    hovertemplate='<b>%{x}</b><br>Duration: %{y}s<extra></extra>'
                ))
                
                fig.update_layout(
                    title="ETL Execution Time Over Last 30 Days",
                    xaxis_title="Date",
                    yaxis_title="Time (seconds)",
                    template="plotly_dark",
                    height=400,
                    hovermode='x unified'
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Success rate metric
                success_rate = (chart_data['status'] == 'SUCCESS').mean() * 100
                st.metric(
                    "Success Rate (30 days)",
                    f"{success_rate:.1f}%",
                    delta=f"{len(chart_data[chart_data['status'] == 'SUCCESS'])} successful runs"
                )
        else:
            st.info("👉 No execution history yet. ETL will run daily at 8 AM.")
        
    except Exception as e:
        st.error(f"❌ Error loading system health: {e}")
        import traceback
        with st.expander("🐛 Debug Info"):
            st.code(traceback.format_exc())


# Main app
def main():
    # Header
    st.markdown("# 📊 WindBorne Finance Dashboard")
    st.markdown("### Real-time financial metrics for TEL, ST, and DD")
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.markdown("## 🎯 Navigation")
        page = st.radio(
            "Select View",
            ["📈 Overview", "💰 Profitability", "💧 Liquidity", "📊 All Metrics", "🔧 System Health", "📋 ETL Logs"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        
        # Refresh controls
        st.markdown("### 🔄 Data Refresh")
        
        col1, col2 = st.columns([2, 1])
        with col1:
            if st.button("🔄 Refresh Now", use_container_width=True):
                st.cache_resource.clear()
                st.cache_data.clear()
                st.success("✅ Refreshed!")
                time.sleep(0.5)
                st.rerun()
        
        with col2:
            auto_refresh = st.toggle("Auto")
        
        if auto_refresh:
            st.info("⏱️ Refreshing every 30s")
            time.sleep(30)
            st.rerun()
        
        st.caption(f"🕐 Last update: {datetime.now().strftime('%H:%M:%S')}")
        
        st.markdown("---")
        st.markdown("### 🔌 Connection Status")
        success, msg = test_connection()
        if success:
            st.success(msg)
        else:
            st.error(msg)
        
        st.markdown("---")
        st.markdown("### ℹ️ About")
        st.info("""
        **Data Source:** Alpha Vantage API
        
        **Companies:**
        - TEL - TE Connectivity
        - ST - Sensata Technologies
        - DD - DuPont de Nemours
        
        **Update:** Daily at 8 AM
        
        **Data Period:** Last 3 years
        """)
    
    # Routes
    if page == "📈 Overview":
        show_overview()
    elif page == "💰 Profitability":
        show_profitability()
    elif page == "💧 Liquidity":
        show_liquidity()
    elif page == "📊 All Metrics":
        show_all_metrics()
    elif page == "🔧 System Health":
        show_system_health()
    else:
        show_etl_logs()


def show_overview():
    """Overview page with key metrics"""
    conn = get_db_connection()
    if not conn:
        st.error("❌ Cannot connect to database")
        return
    
    try:
        # Quick stats
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            df = pd.read_sql("SELECT COUNT(*) as count FROM companies", conn)
            st.metric("🏢 Companies", df['count'].iloc[0])
        
        with col2:
            df = pd.read_sql("""
                SELECT COUNT(DISTINCT fiscal_year) as count 
                FROM calculated_metrics
            """, conn)
            st.metric("📅 Years of Data", df['count'].iloc[0])
        
        with col3:
            df = pd.read_sql("""
                SELECT COUNT(*) as count 
                FROM calculated_metrics
            """, conn)
            st.metric("📊 Total Metrics", f"{df['count'].iloc[0]:,}")
        
        with col4:
            df = pd.read_sql("""
                SELECT run_date FROM etl_runs 
                ORDER BY run_date DESC LIMIT 1
            """, conn)
            if not df.empty:
                last_run = df['run_date'].iloc[0]
                st.metric("🔄 Last Update", last_run.strftime("%m/%d/%Y"))
            else:
                st.metric("🔄 Last Update", "Never")
        
        st.markdown("---")
        
        # Latest metrics comparison
        st.markdown("## 📈 Latest Performance (Most Recent Year Per Company)")
        
        query = """
            WITH latest_per_company AS (
                SELECT 
                    company_id,
                    MAX(fiscal_year) as latest_year
                FROM calculated_metrics
                GROUP BY company_id
            )
            SELECT 
                c.symbol,
                c.name,
                cm.fiscal_year,
                MAX(CASE WHEN cm.metric_name = 'gross_margin_pct' 
                    THEN cm.metric_value END) as gross_margin,
                MAX(CASE WHEN cm.metric_name = 'operating_margin_pct' 
                    THEN cm.metric_value END) as operating_margin,
                MAX(CASE WHEN cm.metric_name = 'net_margin_pct' 
                    THEN cm.metric_value END) as net_margin,
                MAX(CASE WHEN cm.metric_name = 'current_ratio' 
                    THEN cm.metric_value END) as current_ratio,
                MAX(CASE WHEN cm.metric_name = 'revenue_yoy_pct' 
                    THEN cm.metric_value END) as revenue_growth
            FROM calculated_metrics cm
            JOIN companies c ON cm.company_id = c.id
            JOIN latest_per_company lpc ON cm.company_id = lpc.company_id 
                AND cm.fiscal_year = lpc.latest_year
            GROUP BY c.symbol, c.name, cm.fiscal_year
            ORDER BY c.symbol
        """
        
        df = pd.read_sql(query, conn)
        
        if not df.empty:
            # Show year info
            year_info = df.groupby('fiscal_year')['symbol'].apply(list).to_dict()
            year_text = " | ".join([f"{year}: {', '.join(symbols)}" for year, symbols in year_info.items()])
            st.info(f"📅 **Years shown:** {year_text}")
            
            # Create comparison charts
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 💰 Profitability Margins")
                fig = go.Figure()
                
                for col, name, color in [
                    ('gross_margin', 'Gross Margin', '#00CC96'),
                    ('operating_margin', 'Operating Margin', '#AB63FA'),
                    ('net_margin', 'Net Margin', '#FFA15A')
                ]:
                    fig.add_trace(go.Bar(
                        name=name,
                        x=df['symbol'],
                        y=df[col],
                        text=df[col].round(1).astype(str) + '%',
                        textposition='auto',
                        marker_color=color,
                        customdata=df[['fiscal_year']],
                        hovertemplate='<b>%{x}</b><br>Year: %{customdata[0]}<br>' + name + ': %{y:.2f}%<extra></extra>'
                    ))
                
                fig.update_layout(
                    barmode='group',
                    height=400,
                    xaxis_title="Company",
                    yaxis_title="Margin (%)",
                    template="plotly_dark",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("### 📊 Financial Health")
                fig = go.Figure()
                
                fig.add_trace(go.Bar(
                    name='Current Ratio',
                    x=df['symbol'],
                    y=df['current_ratio'],
                    text=df['current_ratio'].round(2).astype(str),
                    textposition='auto',
                    marker_color='#636EFA',
                    yaxis='y',
                    customdata=df[['fiscal_year']],
                    hovertemplate='<b>%{x}</b><br>Year: %{customdata[0]}<br>Current Ratio: %{y:.2f}<extra></extra>'
                ))
                
                fig.add_trace(go.Scatter(
                    name='Revenue Growth (%)',
                    x=df['symbol'],
                    y=df['revenue_growth'],
                    text=df['revenue_growth'].round(1).astype(str) + '%',
                    textposition='top center',
                    mode='lines+markers+text',
                    marker=dict(size=12, color='#EF553B'),
                    yaxis='y2',
                    line=dict(width=3),
                    customdata=df[['fiscal_year']],
                    hovertemplate='<b>%{x}</b><br>Year: %{customdata[0]}<br>Revenue Growth: %{y:.2f}%<extra></extra>'
                ))
                
                fig.update_layout(
                    height=400,
                    xaxis_title="Company",
                    yaxis=dict(title="Current Ratio", side="left"),
                    yaxis2=dict(title="Revenue Growth (%)", overlaying="y", side="right"),
                    template="plotly_dark",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Data table
            st.markdown("### 📋 Detailed Metrics")
            display_df = df.copy()
            display_df = display_df.round(2)
            display_df.columns = ['Symbol', 'Company', 'Year', 'Gross Margin %', 
                                   'Operating Margin %', 'Net Margin %', 
                                   'Current Ratio', 'Revenue Growth %']
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.warning("⚠️ No data available. Run ETL pipeline first!")
            st.info("👉 Go to Easypanel → etl → Terminal → Run: `python main.py`")
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
        import traceback
        with st.expander("🐛 Debug Info"):
            st.code(traceback.format_exc())


def show_profitability():
    """Profitability trends over time"""
    st.markdown("## 💰 Profitability Analysis")
    
    conn = get_db_connection()
    if not conn:
        st.error("❌ Cannot connect to database")
        return
    
    try:
        # Filters
        col1, col2 = st.columns(2)
        
        with col1:
            companies_df = pd.read_sql("""
                SELECT DISTINCT symbol FROM companies ORDER BY symbol
            """, conn)
            selected_companies = st.multiselect(
                "Select Companies",
                companies_df['symbol'].tolist(),
                default=companies_df['symbol'].tolist()
            )
        
        with col2:
            years_df = pd.read_sql("""
                SELECT DISTINCT fiscal_year FROM calculated_metrics 
                ORDER BY fiscal_year DESC
            """, conn)
            selected_years = st.multiselect(
                "Select Years",
                years_df['fiscal_year'].tolist(),
                default=years_df['fiscal_year'].tolist()
            )
        
        if not selected_companies or not selected_years:
            st.info("ℹ️ Please select at least one company and year")
            return
        
        # Query data
        companies_str = "','".join(selected_companies)
        years_str = ",".join(map(str, selected_years))
        
        query = f"""
            SELECT 
                c.symbol,
                cm.fiscal_year,
                MAX(CASE WHEN cm.metric_name = 'gross_margin_pct' 
                    THEN cm.metric_value END) as gross_margin,
                MAX(CASE WHEN cm.metric_name = 'operating_margin_pct' 
                    THEN cm.metric_value END) as operating_margin,
                MAX(CASE WHEN cm.metric_name = 'net_margin_pct' 
                    THEN cm.metric_value END) as net_margin
            FROM calculated_metrics cm
            JOIN companies c ON cm.company_id = c.id
            WHERE c.symbol IN ('{companies_str}')
            AND cm.fiscal_year IN ({years_str})
            GROUP BY c.symbol, cm.fiscal_year
            ORDER BY c.symbol, cm.fiscal_year
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            st.warning("⚠️ No data found for selected filters")
            return
        
        # Create trend charts
        metrics = [
            ('gross_margin', 'Gross Margin %', '#00CC96'),
            ('operating_margin', 'Operating Margin %', '#AB63FA'),
            ('net_margin', 'Net Margin %', '#FFA15A')
        ]
        
        for metric_col, title, color in metrics:
            st.markdown(f"### {title}")
            fig = px.line(
                df,
                x='fiscal_year',
                y=metric_col,
                color='symbol',
                markers=True,
                template="plotly_dark"
            )
            fig.update_layout(
                height=400,
                xaxis_title="Fiscal Year",
                yaxis_title=title,
                legend_title="Company",
                hovermode='x unified'
            )
            fig.update_traces(line=dict(width=3), marker=dict(size=10))
            st.plotly_chart(fig, use_container_width=True)
            
    except Exception as e:
        st.error(f"❌ Error: {e}")


def show_liquidity():
    """Liquidity metrics"""
    st.markdown("## 💧 Liquidity & Solvency")
    
    conn = get_db_connection()
    if not conn:
        st.error("❌ Cannot connect to database")
        return
    
    try:
        # Get data
        query = """
            SELECT 
                c.symbol,
                cm.fiscal_year,
                MAX(CASE WHEN cm.metric_name = 'current_ratio' 
                    THEN cm.metric_value END) as current_ratio,
                MAX(CASE WHEN cm.metric_name = 'quick_ratio' 
                    THEN cm.metric_value END) as quick_ratio
            FROM calculated_metrics cm
            JOIN companies c ON cm.company_id = c.id
            GROUP BY c.symbol, cm.fiscal_year
            ORDER BY c.symbol, cm.fiscal_year
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            st.warning("⚠️ No liquidity data available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Current Ratio Trend")
            fig = px.line(
                df,
                x='fiscal_year',
                y='current_ratio',
                color='symbol',
                markers=True,
                template="plotly_dark"
            )
            fig.add_hline(y=1.5, line_dash="dash", line_color="yellow", 
                          annotation_text="Healthy Level (1.5)")
            fig.update_layout(height=400, hovermode='x unified')
            fig.update_traces(line=dict(width=3), marker=dict(size=10))
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### Quick Ratio Trend")
            fig = px.line(
                df,
                x='fiscal_year',
                y='quick_ratio',
                color='symbol',
                markers=True,
                template="plotly_dark"
            )
            fig.add_hline(y=1.0, line_dash="dash", line_color="yellow",
                          annotation_text="Healthy Level (1.0)")
            fig.update_layout(height=400, hovermode='x unified')
            fig.update_traces(line=dict(width=3), marker=dict(size=10))
            st.plotly_chart(fig, use_container_width=True)
        
        # Latest ratios
        st.markdown("### 📊 Latest Liquidity Ratios")
        latest_year = df['fiscal_year'].max()
        latest_df = df[df['fiscal_year'] == latest_year]
        
        cols = st.columns(len(latest_df))
        for idx, (_, row) in enumerate(latest_df.iterrows()):
            with cols[idx]:
                st.metric(
                    f"{row['symbol']}",
                    f"Current: {row['current_ratio']:.2f}",
                    f"Quick: {row['quick_ratio']:.2f}" if row['quick_ratio'] else "N/A"
                )
        
    except Exception as e:
        st.error(f"❌ Error: {e}")


def show_all_metrics():
    """Show all metrics in table format"""
    st.markdown("## 📊 All Financial Metrics")
    
    conn = get_db_connection()
    if not conn:
        st.error("❌ Cannot connect to database")
        return
    
    try:
        # Filters
        col1, col2 = st.columns(2)
        
        with col1:
            companies_df = pd.read_sql("SELECT DISTINCT symbol FROM companies ORDER BY symbol", conn)
            selected_companies = st.multiselect(
                "Companies",
                companies_df['symbol'].tolist(),
                default=companies_df['symbol'].tolist()
            )
        
        with col2:
            years_df = pd.read_sql("""
                SELECT DISTINCT fiscal_year FROM calculated_metrics 
                ORDER BY fiscal_year DESC
            """, conn)
            selected_years = st.multiselect(
                "Years",
                years_df['fiscal_year'].tolist(),
                default=years_df['fiscal_year'].tolist()[:3]
            )
        
        if not selected_companies or not selected_years:
            st.info("ℹ️ Select filters above")
            return
        
        companies_str = "','".join(selected_companies)
        years_str = ",".join(map(str, selected_years))
        
        query = f"""
            SELECT 
                c.symbol,
                cm.fiscal_year,
                cm.metric_name,
                cm.metric_value,
                cm.metric_category
            FROM calculated_metrics cm
            JOIN companies c ON cm.company_id = c.id
            WHERE c.symbol IN ('{companies_str}')
            AND cm.fiscal_year IN ({years_str})
            ORDER BY c.symbol, cm.fiscal_year DESC, cm.metric_category, cm.metric_name
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            st.warning("⚠️ No data available")
            return
        
        st.success(f"✅ Found {len(df)} metrics")
        
        # Pivot for better view
        pivot_df = df.pivot_table(
            index=['symbol', 'fiscal_year'],
            columns='metric_name',
            values='metric_value',
            aggfunc='first'
        ).reset_index()
        
        # Format numbers
        numeric_cols = pivot_df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_cols:
            if col not in ['symbol', 'fiscal_year']:
                pivot_df[col] = pivot_df[col].round(2)
        
        st.dataframe(pivot_df, use_container_width=True, height=600)
        
        # Download button
        csv = pivot_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"windborne_metrics_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"❌ Error: {e}")


def show_etl_logs():
    """ETL execution logs"""
    st.markdown("## 📋 ETL Pipeline Logs")
    
    conn = get_db_connection()
    if not conn:
        st.error("❌ Cannot connect to database")
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
            LIMIT 50
        """, conn)
        
        if df.empty:
            st.info("ℹ️ No ETL runs yet. Trigger the workflow in n8n or run manually.")
            st.code("# In Easypanel → etl → Terminal:\npython main.py")
            return
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_runs = len(df)
        success_rate = (df['status'] == 'SUCCESS').sum() / total_runs * 100 if total_runs > 0 else 0
        avg_time = df['execution_time_seconds'].mean()
        last_run = df['run_date'].iloc[0]
        
        col1.metric("📊 Total Runs", total_runs)
        col2.metric("✅ Success Rate", f"{success_rate:.1f}%")
        col3.metric("⏱️ Avg Time", f"{avg_time:.0f}s")
        col4.metric("🔄 Last Run", last_run.strftime("%m/%d %H:%M"))
        
        st.markdown("---")
        
        # Execution timeline
        st.markdown("### 📈 Execution Timeline")
        
        # Preparar dados para o gráfico
        timeline_df = df.copy()
        timeline_df['date_str'] = timeline_df['run_date'].dt.strftime('%m/%d %H:%M')
        
        fig = go.Figure()
        
        for status in ['SUCCESS', 'FAILED']:
            status_df = timeline_df[timeline_df['status'] == status]
            if not status_df.empty:
                fig.add_trace(go.Scatter(
                    x=status_df['run_date'],
                    y=status_df['execution_time_seconds'],
                    mode='markers',
                    name=status,
                    marker=dict(
                        size=12,
                        color='#00CC96' if status == 'SUCCESS' else '#EF553B',
                        symbol='circle'
                    ),
                    text=status_df['date_str'],
                    hovertemplate='<b>%{text}</b><br>Time: %{y}s<br>Status: ' + status + '<extra></extra>'
                ))
        
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Execution Time (seconds)",
            height=350,
            template="plotly_dark",
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Logs table
        st.markdown("### 📋 Recent Executions")
        display_df = df.copy()
        display_df['run_date'] = display_df['run_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        display_df.columns = ['Date', 'Workflow', 'Companies', 'API Calls', 
                               'Failures', 'Time (s)', 'Status']
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        # Quick stats
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### ⏱️ Execution Time Stats")
            st.write(f"**Min:** {df['execution_time_seconds'].min():.0f}s")
            st.write(f"**Max:** {df['execution_time_seconds'].max():.0f}s")
            st.write(f"**Avg:** {df['execution_time_seconds'].mean():.0f}s")
        
        with col2:
            st.markdown("#### 📞 API Calls Stats")
            st.write(f"**Total Calls:** {df['api_calls_made'].sum()}")
            st.write(f"**Total Failures:** {df['api_failures'].sum()}")
            failure_rate = (df['api_failures'].sum() / df['api_calls_made'].sum() * 100) if df['api_calls_made'].sum() > 0 else 0
            st.write(f"**Failure Rate:** {failure_rate:.1f}%")
        
    except Exception as e:
        st.error(f"❌ Error: {e}")
        import traceback
        with st.expander("🐛 Debug Info"):
            st.code(traceback.format_exc())


if __name__ == "__main__":
    main()