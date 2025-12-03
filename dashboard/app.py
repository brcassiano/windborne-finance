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
    .production-box {
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #00CC96;
        margin: 10px 0;
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
    st.markdown("## 🔧 System Health & ETL Monitoring")
    
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
            st.markdown("### 📊 Latest Execution Status")
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
            st.markdown("### 📞 API Statistics")
            col5, col6, col7 = st.columns(3)
            with col5:
                st.metric("API Calls Made", df['api_calls_made'].iloc[0])
            with col6:
                st.metric("API Failures", df['api_failures'].iloc[0])
            with col7:
                failure_rate = (df['api_failures'].iloc[0] / df['api_calls_made'].iloc[0] * 100) if df['api_calls_made'].iloc[0] > 0 else 0
                st.metric("Failure Rate", f"{failure_rate:.1f}%")
        else:
            st.warning("⚠️ No ETL runs found. Run ETL pipeline first!")
            st.code("# Trigger via n8n webhook:\ncurl http://your-ip:5678/webhook/etl-manual")
        
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
                
                # Success rate and stats
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    success_rate = (chart_data['status'] == 'SUCCESS').mean() * 100
                    st.metric(
                        "Success Rate (30 days)",
                        f"{success_rate:.1f}%"
                    )
                
                with col2:
                    st.metric(
                        "Total Runs",
                        len(chart_data)
                    )
                
                with col3:
                    st.metric(
                        "Avg Duration",
                        f"{chart_data['execution_time_seconds'].mean():.0f}s"
                    )
        else:
            st.info("👉 No execution history yet. ETL will run daily at 8 AM BRT.")
        
    except Exception as e:
        st.error(f"❌ Error loading system health: {e}")
        import traceback
        with st.expander("🐛 Debug Info"):
            st.code(traceback.format_exc())


def show_about_production():
    """About & Production Strategy"""
    st.markdown("## 📚 About & Production Strategy")
    
    st.markdown("""
    This page explains the current architecture and production roadmap for scaling 
    the WindBorne Finance platform with enterprise-grade reliability.
    """)
    
    # Current Architecture
    st.markdown("---")
    st.markdown("### 🏗️ Current Architecture")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ```
        ┌─────────────────────────────────────────────────────────────┐
        │                     CURRENT STACK                           │
        └─────────────────────────────────────────────────────────────┘
        
        ┌──────────────┐
        │ Alpha Vantage│  ← External API (5 calls/min, 25/day)
        │     API      │
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐     ┌──────────────┐
        │ ETL Pipeline │────▶│  PostgreSQL  │
        │   (Python)   │     │   Database   │
        │   + Flask    │     │   (Docker)   │
        └──────┬───────┘     └──────┬───────┘
               │                    │
               │                    ▼
               │             ┌──────────────┐
               │             │  Streamlit   │
               │             │  Dashboard   │
               │             └──────────────┘
               ▼
        ┌──────────────┐
        │     n8n      │  ← Workflow Automation
        │  Scheduler   │     (Daily 8 AM BRT)
        └──────────────┘
        ```
        """)
    
    with col2:
        st.markdown("#### 🔧 Components")
        st.markdown("""
        **Data Layer:**
        - PostgreSQL 16
        - 3 Companies (TEL, ST, DD)
        - ~4 years historical data
        
        **Processing:**
        - Python ETL pipeline
        - Flask API for triggers
        - Calculated metrics engine
        
        **Automation:**
        - n8n workflow scheduler
        - Daily execution at 8 AM
        - Manual trigger via webhook
        
        **Visualization:**
        - Streamlit dashboard
        - Real-time metrics
        - Interactive charts
        """)
    
    # Production Roadmap
    st.markdown("---")
    st.markdown("### 🚀 Production Roadmap")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Google Sheets Integration",
        "⚡ Rate Limiting Strategy", 
        "🔄 Workflow Improvements",
        "📈 Scaling Plan"
    ])
    
    with tab1:
        st.markdown("#### 📊 Google Sheets Integration for Executives")
        
        st.markdown("""
        <div class="production-box">
        <h4>🎯 Objective</h4>
        Enable executives to perform ad-hoc financial analysis in Google Sheets with real-time data.
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("##### Implementation Approach:")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Option 1: Export + Manual Refresh** (Quick Win)
            
            ```
            # Add to n8n workflow after ETL success
            1. Query latest metrics from PostgreSQL
            2. Format as CSV/JSON
            3. Use Google Sheets API
            4. Update specific sheet range
            5. Preserve formulas and formatting
            ```
            
            ✅ **Pros:**
            - Simple to implement
            - No API quota concerns
            - Executives control refresh
            
            ⚠️ **Cons:**
            - Manual trigger needed
            - Not real-time
            """)
        
        with col2:
            st.markdown("""
            **Option 2: Real-time Sync** (Advanced)
            
            ```
            # Use Google Sheets as visualization layer
            1. Create Google Apps Script
            2. Connect to PostgreSQL via Cloud SQL
            3. Use QUERY() and IMPORTDATA()
            4. Auto-refresh every 5 minutes
            5. Custom functions for metrics
            ```
            
            ✅ **Pros:**
            - Always up-to-date
            - Familiar Excel-like interface
            
            ⚠️ **Cons:**
            - More complex setup
            - Potential performance issues
            """)
        
        st.markdown("##### Recommended Architecture:")
        
        st.code("""
# n8n Workflow: "Export to Google Sheets"

Trigger: Schedule (after ETL success)
  ↓
[PostgreSQL Query] → Get latest metrics
  ↓
[Transform Data] → Format for Sheets (pivot tables, etc)
  ↓
[Google Sheets Node] → Update "Executive Dashboard" sheet
  ↓
[Slack Notification] → "📊 Sheets updated with latest data"
        """, language="yaml")
        
        st.info("💡 **Best Practice:** Use a template sheet with pre-built charts and pivot tables. ETL updates data range only, preserving executive customizations.")
    
    with tab2:
        st.markdown("#### ⚡ Rate Limiting Strategy for Alpha Vantage")
        
        st.markdown("""
        <div class="production-box">
        <h4>⚠️ API Constraints</h4>
        <ul>
        <li><strong>5 calls per minute</strong></li>
        <li><strong>25 calls per day</strong> (free tier)</li>
        <li><strong>Current usage:</strong> 9 calls per run (3 companies × 3 statements)</li>
        </ul>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 🔐 Current Mitigation")
            st.markdown("""
            ```
            # Implemented in ETL:
            import time
            
            def fetch_with_rate_limit(symbol):
                # Wait 12 seconds between calls
                # = 5 calls/minute max
                time.sleep(12)
                return api.fetch(symbol)
            ```
            
            **Daily Run Strategy:**
            - Run once at 8 AM BRT
            - 9 API calls total
            - Leaves 16 calls for manual runs
            - ~2 minutes execution time
            """)
        
        with col2:
            st.markdown("##### 🚀 Production Strategy")
            st.markdown("""
            **For scaling to 10+ companies:**
            
            1. **Intelligent Caching**
               ```
               # Only fetch if data changed
               if last_fiscal_year_changed:
                   fetch_new_data()
               else:
                   skip_api_call()
               ```
            
            2. **Priority Queue**
               ```
               # Fetch most important first
               priority = {
                   'TEL': 1,  # High priority
                   'ST': 2,
                   'DD': 3
               }
               ```
            
            3. **Upgrade to Paid Tier**
               - $50/month = 75 calls/min
               - $500/month = unlimited
            """)
        
        st.warning("⚡ **Recommendation:** Implement caching first, then upgrade API tier only when exceeding 25 calls/day consistently.")
    
    with tab3:
        st.markdown("#### 🔄 Workflow Automation Improvements")
        
        st.markdown("##### Current n8n Workflow")
        st.code("""
[Schedule Trigger: 8 AM daily]
  ↓
[HTTP Request: POST /run-etl]
  ↓
[IF: status === 'success']
  ↓
┌─────────────┬─────────────┐
│   SUCCESS   │    FAILED   │
│   (Log)     │   (Alert)   │
└─────────────┴─────────────┘
        """, language="yaml")
        
        st.markdown("##### 🎯 Production-Ready Enhancements")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **1. Error Recovery**
            ```
            [IF: ETL Failed]
              ↓
            [Wait: 5 minutes]
              ↓
            [Retry: Max 3 attempts]
              ↓
            [IF: Still failing]
              ↓
            [Alert: Slack + Email]
            ```
            
            **2. Data Quality Checks**
            ```
            [After ETL Success]
              ↓
            [Query: Check row counts]
              ↓
            [IF: Data anomaly detected]
              ↓
            [Alert: Potential data issue]
            ```
            """)
        
        with col2:
            st.markdown("""
            **3. Multi-Stage Pipeline**
            ```
            Stage 1: Extract (Alpha Vantage)
              ↓
            Stage 2: Transform (Metrics)
              ↓
            Stage 3: Load (PostgreSQL)
              ↓
            Stage 4: Export (Google Sheets)
              ↓
            Stage 5: Notify (Stakeholders)
            ```
            
            **4. Monitoring Hooks**
            ```
            [Every stage completion]
              ↓
            [Log: Prometheus metrics]
              ↓
            [Dashboard: Grafana]
            ```
            """)
        
        st.success("✅ **Priority:** Implement retry logic and data quality checks first (highest ROI).")
    
    with tab4:
        st.markdown("#### 📈 Scaling Plan (10→100+ Companies)")
        
        st.markdown("##### Phase 1: Optimize Current Stack (1-10 companies)")
        st.markdown("""
        - ✅ Implement intelligent caching
        - ✅ Add retry logic and error recovery
        - ✅ Google Sheets integration
        - ✅ Basic monitoring (current System Health page)
        
        **Estimated capacity:** 10 companies, 30 API calls/day
        """)
        
        st.markdown("##### Phase 2: Horizontal Scaling (10-50 companies)")
        st.markdown("""
        - 🔄 Upgrade Alpha Vantage to paid tier ($50/month)
        - 🔄 Implement job queue (Redis + Celery)
        - 🔄 Parallel processing (process 5 companies simultaneously)
        - 🔄 Add Grafana for ops monitoring
        
        **Estimated capacity:** 50 companies, 150 API calls/day
        """)
        
        st.markdown("##### Phase 3: Enterprise Architecture (50-500 companies)")
        st.markdown("""
        ```
        ┌──────────────────────────────────────────────────┐
        │         LOAD BALANCER (Nginx)                    │
        └──────────────────┬───────────────────────────────┘
                           │
        ┌──────────────────┼───────────────────────────────┐
        │                  │                               │
        ▼                  ▼                               ▼
    [ETL Worker 1]   [ETL Worker 2]   [ETL Worker 3]
        │                  │                               │
        └──────────────────┴───────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────────────────┐
        │     PostgreSQL Cluster (Primary + Replicas)      │
        └──────────────────────────────────────────────────┘
        ```
        
        - 🚀 Multiple ETL workers (Docker Swarm or Kubernetes)
        - 🚀 PostgreSQL read replicas for dashboard
        - 🚀 Redis for caching and job queue
        - 🚀 CDN for dashboard static assets
        - 🚀 Premium Alpha Vantage tier (unlimited calls)
        
        **Estimated capacity:** 500+ companies, unlimited API calls
        """)
        
        st.markdown("##### Cost Estimation")
        
        cost_data = pd.DataFrame({
            'Phase': ['Phase 1', 'Phase 2', 'Phase 3'],
            'Companies': ['1-10', '10-50', '50-500'],
            'Monthly Cost': ['$0', '$150', '$2,000+'],
            'Components': [
                'VPS ($0-20) + Free tier',
                'VPS ($50) + API ($50) + Monitoring ($50)',
                'K8s Cluster ($500) + API ($500) + DB ($500) + CDN ($300) + Staff ($200+)'
            ]
        })
        
        st.dataframe(cost_data, use_container_width=True, hide_index=True)
    
    # Technical Stack Details
    st.markdown("---")
    st.markdown("### 🛠️ Technical Stack Details")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **Backend**
        - Python 3.11
        - Flask (API)
        - psycopg2 (PostgreSQL)
        - pandas (data processing)
        
        **Database**
        - PostgreSQL 16
        - Docker container
        - Persistent volumes
        """)
    
    with col2:
        st.markdown("""
        **Frontend**
        - Streamlit 1.28+
        - Plotly (charts)
        - Custom CSS styling
        
        **Automation**
        - n8n (workflows)
        - Cron scheduling
        - Webhook triggers
        """)
    
    with col3:
        st.markdown("""
        **Infrastructure**
        - Docker containers
        - Easypanel (orchestration)
        - VPS hosting
        
        **Monitoring**
        - Built-in health checks
        - Execution logs
        - API metrics tracking
        """)
    
    # Contact & Links
    st.markdown("---")
    st.markdown("### 📞 Additional Resources")
    
    st.info("""
    **📖 Documentation:**
    - [Alpha Vantage API Docs](https://www.alphavantage.co/documentation/)
    - [n8n Workflow Docs](https://docs.n8n.io/)
    - [PostgreSQL Best Practices](https://www.postgresql.org/docs/)
    - [Google Sheets API Guide](https://developers.google.com/sheets/api)
    
    **🔗 Quick Links:**
    - n8n Dashboard: `http://your-ip:5678`
    - ETL API: `http://your-ip:5000`
    - Database Admin: `http://your-ip:8081`
    
    **⚙️ Manual Operations:**
    ```
    # Trigger ETL manually
    curl -X POST http://your-ip:5000/run-etl
    
    # View logs
    docker logs windborne-finance-etl --tail 50
    
    # Database backup
    docker exec postgres pg_dump -U postgres windborne_finance > backup.sql
    ```
    """)


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
            [
                "📈 Overview",
                "💰 Profitability",
                "💧 Liquidity",
                "📊 All Metrics",
                "🔧 System Health",
                "📚 About & Production"
            ],
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
        
        **Update:** Daily at 8 AM BRT
        
        **Data Period:** Last 4 years
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
        show_about_production()


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
            st.info("👉 Trigger ETL via n8n webhook or API")
            
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


if __name__ == "__main__":
    main()