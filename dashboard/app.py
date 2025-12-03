import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from sqlalchemy import create_engine  # ← ADICIONAR
import urllib.parse  # ← ADICIONAR
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
    .warning-box {
        background-color: #2d1e1e;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #FFA15A;
        margin: 10px 0;
    }
    .code-box {
        background-color: #1a1a1a;
        padding: 15px;
        border-radius: 8px;
        font-family: 'Courier New', monospace;
        font-size: 0.9rem;
        line-height: 1.6;
        white-space: pre;
        overflow-x: auto;
    }
    </style>
""", unsafe_allow_html=True)


# Database engine para pandas (SQLAlchemy)
@st.cache_resource(ttl=300)  # Cache por 5 minutos
def get_db_engine():
    """Get SQLAlchemy engine for pandas queries"""
    try:
        password = urllib.parse.quote_plus(os.getenv('POSTGRES_PASSWORD', ''))
        host = os.getenv('POSTGRES_HOST', 'postgres')
        port = os.getenv('POSTGRES_PORT', 5432)
        database = os.getenv('POSTGRES_DB', 'windborne_finance')
        user = os.getenv('POSTGRES_USER', 'postgres')
        
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(
            connection_string, 
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10
        )
        
        return engine
    except Exception as e:
        st.error(f"❌ Database engine creation failed: {e}")
        return None


# Test connection (usa psycopg2 para testar)
def test_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', 5432),
            database=os.getenv('POSTGRES_DB', 'windborne_finance'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur


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
            st.info("👉 Trigger ETL via Flask API: curl -X POST http://your-ip:5000/run-etl")
        
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
    finally:
        if conn:
            conn.close()


def show_about_production():
    """About & Production Strategy"""
    st.markdown("## 📚 Production Strategy & Architecture")
    
    st.markdown("""
    This page explains the current architecture and addresses the key production questions 
    for scaling the WindBorne Finance platform.
    """)
    
    # Current Architecture
    st.markdown("---")
    st.markdown("### 🏗️ Current Architecture Overview")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("""
        <div class="code-box">
┌─────────────────────────────────────────────────────────┐
│                   WINDBORNE FINANCE                     │
│              Production Architecture                    │
└─────────────────────────────────────────────────────────┘

                 EXECUTION FLOW
                 
    ┌─────────────┐
    │    n8n      │ ← Orchestrator
    │  Scheduler  │   (8 AM daily)
    └──────┬──────┘
           │
           ↓ [Schedule Trigger]
           
    ┌─────────────┐
    │ HTTP Node   │ ← POST /run-etl
    └──────┬──────┘
           │
           ↓
           
    ┌─────────────┐
    │ Flask API   │ ← :5000
    └──────┬──────┘
           │
           ↓
           
    ┌─────────────┐      ┌──────────────┐
    │ ETL Python  │─────→│ Alpha Vantage│
    │  Pipeline   │←─────│     API      │
    └──────┬──────┘      └──────────────┘
           │              • 5 calls/min
           │              • 25 calls/day
           ↓
           
    ┌─────────────┐
    │ PostgreSQL  │ ← Storage
    │  Database   │   • companies
    │  (Docker)   │   • statements
    └──────┬──────┘   • metrics
           │
           ↑ [reads data]
           
    ┌─────────────┐
    │  Streamlit  │ ← Visualization
    │  Dashboard  │   :8501
    └─────────────┘
    
┌─────────────────────────────────────────────────────────┐
│  Infrastructure: Docker on VPS                          │
│  Management: Easypanel (Docker UI)                      │
└─────────────────────────────────────────────────────────┘
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("#### 🔧 Tech Stack")
        st.markdown("""
        **Data Layer:**
        - PostgreSQL 16
        - Normalized schema (3NF)
        - 4 main tables
        - Indexed for performance
        
        **Processing:**
        - Python 3.11
        - Flask API (trigger endpoint)
        - Pandas (data manipulation)
        - psycopg2 (DB connector)
        
        **Automation:**
        - n8n workflows
        - Schedule triggers (cron)
        - HTTP Request node
        - Error recovery
        
        **Visualization:**
        - Streamlit 1.28+
        - Plotly charts
        - Responsive design
        
        **Infrastructure:**
        - Docker containers
        - Easypanel (Docker UI)
        - VPS hosting
        - Persistent volumes
        """)
    
    # Database Schema with Indexing
    st.markdown("---")
    st.markdown("### 🗄️ Database Schema & Indexing Strategy")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="code-box">
-- Companies Table
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    industry VARCHAR(100),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_companies_symbol ON companies(symbol);
CREATE INDEX idx_companies_updated ON companies(updated_at DESC);

-- Financial Statements Table  
CREATE TABLE financial_statements (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    fiscal_year INTEGER NOT NULL,
    statement_type VARCHAR(20) NOT NULL,
    line_item VARCHAR(100) NOT NULL,
    value NUMERIC(20, 2),
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_statements_company_year 
    ON financial_statements(company_id, fiscal_year DESC);
CREATE INDEX idx_statements_type 
    ON financial_statements(statement_type, line_item);
CREATE INDEX idx_statements_composite 
    ON financial_statements(company_id, fiscal_year, statement_type);
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="code-box">
-- Calculated Metrics Table
CREATE TABLE calculated_metrics (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    fiscal_year INTEGER NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(15, 4),
    metric_category VARCHAR(50),
    calculated_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_metrics_company_year 
    ON calculated_metrics(company_id, fiscal_year DESC);
CREATE INDEX idx_metrics_name 
    ON calculated_metrics(metric_name);
CREATE INDEX idx_metrics_category 
    ON calculated_metrics(metric_category);
CREATE INDEX idx_metrics_composite 
    ON calculated_metrics(company_id, fiscal_year, metric_name);

-- ETL Audit Log
CREATE TABLE etl_runs (
    id SERIAL PRIMARY KEY,
    run_date TIMESTAMP DEFAULT NOW(),
    workflow_name VARCHAR(100),
    companies_processed INTEGER,
    api_calls_made INTEGER,
    api_failures INTEGER,
    execution_time_seconds INTEGER,
    status VARCHAR(20)
);
CREATE INDEX idx_etl_runs_date ON etl_runs(run_date DESC);
CREATE INDEX idx_etl_runs_status ON etl_runs(status);
        </div>
        """, unsafe_allow_html=True)
    
    st.info("""
    **🎯 Indexing Rationale:**
    - **Single-column indexes:** Fast lookups on primary query columns (symbol, year, metric_name)
    - **Composite indexes:** Optimized for multi-condition WHERE clauses (company + year + type)
    - **DESC indexes:** Efficient sorting for "latest year" queries
    - **Trade-off:** Slightly slower writes, but 10-100x faster reads (critical for dashboards)
    """)
    
    # Four Key Questions
    st.markdown("---")
    st.markdown("### 🎯 Production Questions & Answers")
    
    st.markdown("""
    <div class="production-box">
    <h3>Question 1: How would you schedule your code to run monthly?</h3>
    </div>
    """, unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["n8n Workflow (Implemented)", "Alternative Solutions"])
    
    with tab1:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("""
            <div class="code-box">
┌─────────────────────────────────────────────────────────┐
│         n8n Workflow: "WindBorne Monthly ETL"           │
└─────────────────────────────────────────────────────────┘

[1] Schedule Trigger
    ├─ Cron: 0 8 1 * *        # Every 1st of month at 8 AM
    ├─ Timezone: America/Sao_Paulo
    └─ Active: ✓
         │
         ↓
[2] HTTP Request Node
    ├─ Method: POST
    ├─ URL: http://etl:5000/run-etl
    ├─ Timeout: 300000ms (5 min)
    └─ Authentication: None
         │
         ↓
[3] IF Node (Check Success)
    ├─ Condition: {{ $json.status }} === "success"
    └─ Split into two paths
         │
    ┌────┴────┐
    │         │
    ↓         ↓
[4a] SET     [4b] SET
Success Log   Error Alert
    │         │
    ↓         ↓
[5a] Slack   [5b] Email
Notification  Alert + Retry Logic
    │
    ↓
[6] Google Sheets Export (optional)
    ├─ Query PostgreSQL for latest metrics
    ├─ Format as pivot table
    └─ Update "Executive Dashboard" sheet
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            **Pseudocode:**
            
            ```
            # n8n executes monthly
            def monthly_etl_workflow():
                # 1. Trigger
                if today.day == 1 and hour == 8:
                    
                    # 2. Execute ETL
                    response = requests.post(
                        'http://etl:5000/run-etl',
                        timeout=300
                    )
                    
                    # 3. Check result
                    if response.json()['status'] == 'success':
                        # 4a. Log success
                        log_to_database(response)
                        notify_slack("✅ ETL success")
                        
                        # 6. Export to Sheets
                        export_to_google_sheets()
                    else:
                        # 4b. Alert failure
                        send_email_alert(response['stderr'])
                        
                        # Retry logic
                        retry_etl(max_attempts=3)
            ```
            
            **Benefits:**
            - Visual workflow editor
            - No code deployment
            - Built-in retry logic
            - Easy to modify schedule
            - Manual trigger via Flask API
            """)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Alternative 1: Cron Job**
            ```
            # /etc/crontab
            0 8 1 * * python /app/main.py >> /var/log/etl.log 2>&1
            ```
            
            ✅ **Pros:**
            - Simple, no dependencies
            - Reliable, built into OS
            
            ❌ **Cons:**
            - No visual monitoring
            - Manual retry logic needed
            - Harder to modify
            """)
            
            st.markdown("""
            **Alternative 2: Airflow DAG**
            ```
            from airflow import DAG
            from airflow.operators.python import PythonOperator
            
            dag = DAG(
                'windborne_etl',
                schedule_interval='0 8 1 * *',
                catchup=False
            )
            
            run_etl = PythonOperator(
                task_id='run_etl',
                python_callable=execute_etl
            )
            ```
            
            ✅ **Pros:**
            - Enterprise-grade
            - Complex workflows
            - Great monitoring
            
            ❌ **Cons:**
            - Overkill for simple ETL
            - Resource intensive
            - Steep learning curve
            """)
        
        with col2:
            st.markdown("""
            **Alternative 3: Cloud Functions**
            ```
            # Google Cloud Scheduler + Cloud Function
            
            @functions_framework.http
            def run_etl(request):
                # Triggered by Cloud Scheduler
                result = execute_pipeline()
                return result
            ```
            
            ✅ **Pros:**
            - Serverless (no infra)
            - Auto-scaling
            - Pay-per-use
            
            ❌ **Cons:**
            - Cold start delays
            - Cloud vendor lock-in
            - Cost for frequent runs
            """)
            
            st.markdown("""
            **Alternative 4: GitHub Actions**
            ```
            # .github/workflows/etl.yml
            name: Monthly ETL
            on:
              schedule:
                - cron: '0 8 1 * *'
            jobs:
              run-etl:
                runs-on: ubuntu-latest
                steps:
                  - uses: actions/checkout@v2
                  - run: python main.py
            ```
            
            ✅ **Pros:**
            - Free for public repos
            - Git-based workflow
            
            ❌ **Cons:**
            - Limited to 6 hours runtime
            - Not designed for ETL
            """)
    
    st.success("**✅ Recommended:** n8n for this use case - perfect balance of simplicity and features.")
    
    # Question 2
    st.markdown("""
    <div class="production-box">
    <h3>Question 2: How would you handle API rate limit for 100 companies?</h3>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Current Situation (3 companies):**
        - 3 companies × 3 statements = **9 API calls**
        - Runtime: ~2 minutes (12s sleep between calls)
        - Daily limit usage: **9/25 (36%)**
        - ✅ Works fine!
        
        **Problem at 100 companies:**
        - 100 companies × 3 statements = **300 API calls needed**
        - Daily limit: **25 calls**
        - **Would take 12 days to complete one full run!**
        """)
        
        st.markdown("""
        <div class="warning-box">
        <h4>⚠️ Rate Limit Constraint</h4>
        <ul>
        <li><strong>5 calls/minute</strong> = 300 calls/hour max</li>
        <li><strong>25 calls/day</strong> = limiting factor</li>
        <li><strong>Need 300 calls</strong> for 100 companies</li>
        </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        **Solution: Intelligent Caching + API Upgrade**
        
        ```
        # Strategy 1: Cache-First Approach
        def fetch_with_cache(symbol, statement_type):
            # Check if data already exists for current year
            last_fiscal_year = get_last_fiscal_year(symbol)
            cached_year = get_cached_fiscal_year(symbol)
            
            if last_fiscal_year == cached_year:
                # No new fiscal year, skip API call
                logging.info(f"Using cached data for {symbol}")
                return get_from_cache(symbol)
            else:
                # New fiscal year available, fetch from API
                time.sleep(12)  # Rate limit: 5 calls/min
                return api.fetch(symbol, statement_type)
        
        # Strategy 2: Priority Queue
        priority_companies = [
            ('TEL', 1),    # High priority
            ('AAPL', 1),
            ('MSFT', 1),
            # ... other companies
            ('LOW_PRIORITY', 99)
        ]
        
        # Fetch high priority first within daily limit
        for symbol, priority in sorted(priority_companies, 
                                       key=lambda x: x):[1]
            if api_calls_today < DAILY_LIMIT:
                fetch_and_store(symbol)
        ```
        """)
    
    st.markdown("#### 📊 Scaling Strategy")
    
    conn = get_db_connection()
    if conn:
        try:
            scaling_data = pd.DataFrame({
                'Stage': ['Current', 'Phase 1', 'Phase 2', 'Phase 3'],
                'Companies': [3, 10, 50, 100],
                'Daily API Calls': [9, 30, 150, 300],
                'Strategy': [
                    'Free tier (25/day)',
                    'Smart caching + free tier',
                    'Paid tier $50/mo (75/min)',
                    'Paid tier $500/mo (unlimited)'
                ],
                'Cache Hit Rate': ['0%', '70%', '85%', '90%'],
                'Effective Calls': [9, 9, 22, 30],
                'Monthly Cost': ['$0', '$0', '$50', '$500']
            })
            
            st.dataframe(scaling_data, use_container_width=True, hide_index=True)
        finally:
            conn.close()
    
    st.info("""
    **💡 Key Insight:** With 90% cache hit rate (companies don't release statements daily), 
    100 companies only need ~30 fresh API calls per day = **works with paid tier!**
    """)
    
    # Question 3
    st.markdown("""
    <div class="production-box">
    <h3>Question 3: How would execs access this data in Google Sheets?</h3>
    </div>
    """, unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["🥇 Recommended: Export CSV", "🥈 Direct Connection", "🥉 BigQuery Sync"])
    
    with tab1:
        st.markdown("#### Option 1: Automated CSV Export (via n8n)")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("""
            **Implementation:**
            ```
            # n8n workflow step after ETL success
            
            [PostgreSQL Node]
              ↓ Query: SELECT * FROM calculated_metrics 
                       WHERE fiscal_year = MAX(fiscal_year)
              ↓
            [Transform Data]
              ↓ Pivot table format
              ↓ Add calculated columns
              ↓
            [Google Sheets Node]
              ↓ Spreadsheet ID: [exec dashboard]
              ↓ Range: 'Data'!A1:Z1000
              ↓ Write mode: Append or Replace
              ↓
            [Slack Notification]
              Message: "📊 Executive dashboard updated with latest metrics"
            ```
            
            **Exec Experience:**
            1. Open Google Sheet on 1st of month
            2. See fresh data automatically populated
            3. Pivot tables and charts auto-refresh
            4. Add custom analysis without breaking formulas
            5. Share insights with team
            """)
        
        with col2:
            st.markdown("""
            **✅ Pros:**
            - Familiar spreadsheet interface
            - No technical knowledge needed
            - Preserves custom formulas
            - Works offline after load
            - Collaboration built-in
            - Version history
            
            **❌ Cons:**
            - Not real-time (daily/monthly updates)
            - Manual refresh needed
            - Potential for data staleness
            
            **💰 Cost:** FREE
            
            **⏱️ Setup Time:** 1-2 hours
            
            **🎯 Best For:**
            - Monthly/quarterly reporting
            - Executives who prefer spreadsheets
            - Teams already using Google Workspace
            """)
    
    with tab2:
        st.markdown("#### Option 2: Direct PostgreSQL Connection")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("""
            **Implementation Options:**
            
            **A) Google Sheets + Connected Sheets (Enterprise only)**
            ```
            1. Google Workspace Enterprise required
            2. BigQuery integration required
            3. PostgreSQL → BigQuery sync needed
            4. Then: Sheets → BigQuery → Data
            ```
            
            **B) Third-party connector (e.g., SeekWell, Coefficient)**
            ```
            1. Install Google Sheets add-on
            2. Configure PostgreSQL connection
            3. Write SQL queries in sheets
            4. Auto-refresh on schedule
            ```
            
            **C) Custom Google Apps Script**
            ```
            // Apps Script in Google Sheets
            function refreshData() {
              // Connect to Cloud SQL (public IP)
              var conn = Jdbc.getConnection(
                'jdbc:postgresql://your-ip:5432/windborne',
                'user', 'pass'
              );
              
              var stmt = conn.createStatement();
              var rs = stmt.executeQuery(
                'SELECT * FROM calculated_metrics'
              );
              
              // Write to sheet
              var sheet = SpreadsheetApp.getActiveSheet();
              var data = [];
              while (rs.next()) {
                data.push([rs.getString(1), rs.getInt(2)]);
              }
              sheet.getRange(2, 1, data.length, 2).setValues(data);
            }
            
            // Auto-refresh trigger
            function createTrigger() {
              ScriptApp.newTrigger('refreshData')
                .timeBased()
                .everyDays(1)
                .atHour(9)
                .create();
            }
            ```
            """)
        
        with col2:
            st.markdown("""
            **✅ Pros:**
            - Real-time or near real-time data
            - No CSV export step
            - SQL query flexibility
            - Single source of truth
            
            **❌ Cons:**
            - Security risk (DB credentials in sheet)
            - Requires public IP or VPN
            - Enterprise features costly
            - Third-party tools = $20-50/user/month
            - More complex setup
            
            **💰 Cost:** 
            - DIY: FREE (but risky)
            - Coefficient: $49/user/month
            - Enterprise Workspace: $18/user/month
            
            **⏱️ Setup Time:** 4-8 hours
            
            **🎯 Best For:**
            - Real-time dashboards
            - Technical exec teams
            - High refresh requirements
            """)
    
    with tab3:
        st.markdown("#### Option 3: PostgreSQL → BigQuery → Google Sheets")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("""
            **Architecture:**
            ```
            ┌──────────────┐
            │ PostgreSQL   │
            │  (Source)    │
            └──────┬───────┘
                   │
                   ↓ (Daily Sync)
            ┌──────────────┐
            │  BigQuery    │ ← Google Cloud data warehouse
            │  (Staging)   │    -  Scalable
            └──────┬───────┘    -  Fast queries
                   │            -  BI-friendly
                   ↓
            ┌──────────────┐
            │ Google Sheets│
            │ (Connected)  │
            └──────────────┘
            ```
            
            **Implementation:**
            ```
            # Step 1: PostgreSQL → BigQuery Sync (daily)
            from google.cloud import bigquery
            import psycopg2
            
            def sync_to_bigquery():
                # Extract from PostgreSQL
                pg_conn = psycopg2.connect(...)
                df = pd.read_sql("SELECT * FROM calculated_metrics", pg_conn)
                
                # Load to BigQuery
                bq_client = bigquery.Client()
                table_id = "windborne.financial_metrics"
                
                job = bq_client.load_table_from_dataframe(
                    df, table_id, 
                    write_disposition="WRITE_TRUNCATE"  # Replace
                )
                job.result()
            
            # Step 2: Connected Sheets in Google Sheets
            # 1. Extensions → BigQuery → Create connection
            # 2. Select project → dataset → table
            # 3. Insert chart/pivot table
            # 4. Auto-refresh: hourly or daily
            ```
            """)
        
        with col2:
            st.markdown("""
            **✅ Pros:**
            - Enterprise-grade scalability
            - Fast queries (billions of rows)
            - Native Google Workspace integration
            - Advanced BI features
            - Data versioning
            - Audit logs
            
            **❌ Cons:**
            - Overkill for <100 companies
            - Requires Google Cloud setup
            - Learning curve
            - Ongoing cloud costs
            - Vendor lock-in
            
            **💰 Cost:** 
            - BigQuery: ~$5-20/month (small data)
            - Sheets: FREE
            - Total: $60-240/year
            
            **⏱️ Setup Time:** 8-16 hours
            
            **🎯 Best For:**
            - 100+ companies
            - Complex analytics
            - Multi-team access
            - Long-term data retention
            """)
    
    st.success("""
    **✅ Recommendation for Current Scale (3-10 companies):**  
    **Option 1: Automated CSV Export via n8n**
    
    - Simple, reliable, FREE
    - Meets monthly reporting needs
    - No security risks
    - Easy to maintain
    
    **When to upgrade:** Move to Option 3 (BigQuery) when you reach 50+ companies or need real-time dashboards.
    """)
    
    # Question 4
    st.markdown("""
    <div class="production-box">
    <h3>Question 4: What breaks first and how do you know?</h3>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🚨 Failure Modes (What Breaks First)")
        
        st.markdown("""
        **1. API Rate Limit Hit (Most Likely)**
        ```
        Symptom: HTTP 429 errors
        Cause: Exceeded 5 calls/min or 25 calls/day
        Impact: Partial data load, some companies missing
        MTTR: 1 day (wait for quota reset)
        ```
        **Detection:**
        - Monitor `api_failures` column in `etl_runs`
        - Alert if `api_failures > 3`
        - Check response headers for rate limit info
        
        **Prevention:**
        - Sleep 12s between API calls
        - Track calls with counter
        - Implement exponential backoff
        
        ---
        
        **2. API Key Expired/Invalid (Medium Likelihood)**
        ```
        Symptom: HTTP 401/403 errors
        Cause: API key revoked or expired
        Impact: Complete ETL failure
        MTTR: 1 hour (get new key)
        ```
        **Detection:**
        - ETL status = 'FAILED'
        - Error message contains "Invalid API key"
        - All companies fail immediately
        
        **Prevention:**
        - Store API key in env variable
        - Rotate keys before expiration
        - Have backup API key ready
        
        ---
        
        **3. Database Connection Failure (Low Likelihood)**
        ```
        Symptom: psycopg2.OperationalError
        Cause: PostgreSQL container down, network issue
        Impact: Cannot store data
        MTTR: 5-30 minutes (restart container)
        ```
        **Detection:**
        - Dashboard shows "Cannot connect to database"
        - ETL logs: "Connection refused"
        - Health check fails
        
        **Prevention:**
        - Database health checks every 60s
        - Auto-restart policy in Docker
        - Connection pooling
        
        ---
        
        **4. Bad Data from API (Medium Likelihood)**
        ```
        Symptom: Division by zero, NULL values
        Cause: Alpha Vantage data quality issue
        Impact: Incorrect metrics, NaN values
        MTTR: Manual investigation, 1-4 hours
        ```
        **Detection:**
        - Metrics calculation fails
        - Unusual values (margin > 100%)
        - NULL checks in validator
        
        **Prevention:**
        - Data validation layer
        - Range checks (0 < margin < 100)
        - Historical comparison
        """)
    
    with col2:
        st.markdown("#### 📊 Monitoring & Alerting Strategy")
        
        st.markdown("""
        **Current Implementation:**
        
        ```
        # 1. Built-in Health Checks
        class HealthMonitor:
            def check_database(self):
                try:
                    conn.cursor().execute("SELECT 1")
                    return "HEALTHY"
                except:
                    return "UNHEALTHY"
            
            def check_api_quota(self):
                if api_calls_today >= 25:
                    return "QUOTA_EXCEEDED"
                return "OK"
            
            def check_last_etl_run(self):
                last_run = get_last_etl_run()
                if last_run > 2_days_ago:
                    return "STALE_DATA"
                return "OK"
        
        # 2. Execution Logs
        # Stored in etl_runs table
        # Queryable via System Health page
        
        # 3. n8n Error Handling
        [IF: status !== 'success']
          ↓
        [Send Alert]
          ↓ Email: engineering@company.com
          ↓ Slack: #data-alerts channel
          ↓
        [Retry Logic]
          ↓ Wait 5 minutes
          ↓ Retry (max 3 attempts)
        ```
        
        ---
        
        **Production Improvements:**
        
        ```
        # Add these for production:
        
        # 1. Data Quality Checks
        def validate_metrics(df):
            issues = []
            
            # Range validation
            if (df['gross_margin'] > 100).any():
                issues.append("Margin > 100%")
            
            # NULL checks
            if df['revenue'].isna().sum() > 0:
                issues.append("Missing revenue data")
            
            # Historical comparison
            last_year = get_last_year_metrics()
            if abs(df['margin'] - last_year['margin']) > 50:
                issues.append("Suspicious margin change")
            
            return issues
        
        # 2. Anomaly Detection
        def detect_anomalies(current, historical):
            mean = historical.mean()
            std = historical.std()
            
            # 3-sigma rule
            if current > mean + 3*std:
                alert("Metric unusually high")
            if current < mean - 3*std:
                alert("Metric unusually low")
        
        # 3. External Monitoring (Uptime Robot)
        # Ping: http://dashboard:8501/health
        # Frequency: Every 5 minutes
        # Alert: If 3 consecutive failures
        ```
        
        ---
        
        **Alert Destinations:**
        
        | Severity | Channel | Response Time |
        |----------|---------|---------------|
        | CRITICAL | PagerDuty | Immediate |
        | HIGH | Slack + Email | 30 min |
        | MEDIUM | Slack only | 2 hours |
        | LOW | Daily digest | Next day |
        
        **CRITICAL Alerts:**
        - Database down
        - ETL failed 3x in a row
        - API key invalid
        
        **HIGH Alerts:**
        - API rate limit hit
        - Data staleness > 2 days
        - Suspicious metric values
        
        **MEDIUM Alerts:**
        - Single ETL failure
        - Slow query (>10s)
        
        **LOW Alerts:**
        - Cache miss
        - Minor data quality issue
        """)
    
    st.info("""
    **💡 Detection Strategy Summary:**
    
    1. **Proactive Monitoring:**
       - System Health dashboard (current implementation)
       - Database health checks every 60s
       - API quota tracking
    
    2. **Reactive Alerting:**
       - n8n error handling (implemented)
       - Email/Slack notifications
       - Automatic retries
    
    3. **Data Quality:**
       - Validation layer (partially implemented)
       - **Needs:** Range checks, anomaly detection, historical comparison
    
    4. **External Monitoring:**
       - **Needs:** Uptime monitoring service (UptimeRobot, Pingdom)
       - **Needs:** Log aggregation (Papertrail, Loggly)
    """)
    
    # Additional Resources
    st.markdown("---")
    st.markdown("### 📖 Additional Resources")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **📚 Documentation**
        - [Alpha Vantage API](https://www.alphavantage.co/documentation/)
        - [n8n Workflows](https://docs.n8n.io/)
        - [PostgreSQL Docs](https://www.postgresql.org/docs/)
        - [Google Sheets API](https://developers.google.com/sheets/api)
        """)
    
    with col2:
        st.markdown("""
        **🔗 Service URLs**
        - Dashboard: `http://your-ip:8501`
        - n8n: `http://your-ip:5678`
        - ETL API: `http://your-ip:5000`
        - pgWeb: `http://your-ip:8081`
        """)
    
    with col3:
        st.markdown("""
        **⚙️ Manual Operations**
        ```
        # Trigger ETL
        curl -X POST http://your-ip:5000/run-etl
        
        # View logs
        docker logs etl --tail 50
        
        # Database backup
        docker exec postgres pg_dump \\
          -U postgres windborne_finance \\
          > backup.sql
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
                "📚 Production Guide"
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
            st.info("👉 Trigger ETL via Flask API: curl -X POST http://your-ip:5000/run-etl")
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
        import traceback
        with st.expander("🐛 Debug Info"):
            st.code(traceback.format_exc())
    finally:
        if conn:
            conn.close()


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
    finally:
        if conn:
            conn.close()


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
    finally:
        if conn:
            conn.close()


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
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()