"""Production guide and architecture documentation"""
import streamlit as st
import pandas as pd


def show():
    """About & Production Strategy"""
    
    # CSS customizado para esta página
    st.markdown("""
    <style>
        .code-box {
            background-color: #1e1e1e;
            border: 1px solid #333;
            border-radius: 5px;
            padding: 15px;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            line-height: 1.4;
            overflow-x: auto;
            white-space: pre;
        }
        
        .production-box {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            border-radius: 10px;
            margin: 20px 0;
            color: white;
        }
        
        .production-box h3 {
            margin: 0;
            color: white;
        }
        
        .warning-box {
            background-color: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            border-radius: 5px;
            color: #856404;
        }
        
        .warning-box h4 {
            margin-top: 0;
            color: #856404;
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown("## 📚 Production Strategy & Architecture")
    
    st.markdown("""
    This page explains the current architecture and addresses the key production questions 
    for scaling the WindBorne Finance platform.
    """)
    
    _render_architecture()
    _render_database_schema()
    _render_question_1()
    _render_question_2()
    _render_question_3()
    _render_question_4()
    _render_resources()


def _render_architecture():
    """Render architecture overview section"""
    st.markdown("---")
    st.markdown("### 🛠️⚙️ Current Architecture Overview")
    
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


def _render_database_schema():
    """Render database schema section"""
    st.markdown("---")
    st.markdown("### 🛢 Database Schema & Indexing Strategy")
    
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


def _render_question_1():
    """Render Question 1: Scheduling"""
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


def _render_question_2():
    """Render Question 2: API Rate Limits"""
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
    
    st.info("""
    **💡 Key Insight:** With 90% cache hit rate (companies don't release statements daily), 
    100 companies only need ~30 fresh API calls per day = **works with paid tier!**
    """)

def _render_question_3():
    """Render Question 3: Google Sheets Access"""
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
                df = pd.read_sql("SELECT * FROM calculated_metrics", engine)
                
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


def _render_question_4():
    """Render Question 4: Failure Detection"""
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


def _render_resources():
    """Render additional resources section"""
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