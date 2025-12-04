# WindBorne Finance Automation

Automated financial analysis pipeline using Alpha Vantage API, PostgreSQL, n8n, and Streamlit.

## Tech Stack

- **ETL**: Python 3.11 + Flask API
- **Database**: PostgreSQL 15
- **Orchestration**: n8n (external)
- **Dashboard**: Streamlit (multi-page)
- **Deployment**: Easypanel (Docker)

---

## Project Structure

```
windborne-finance/
|-- .env.example                    # Environment variables template
|-- .gitignore
|-- README.md
|
|-- dashboard/                      # Streamlit Dashboard
|   |-- app.py                      # Main entry point
|   |-- database.py                 # Database connection helper
|   |-- Dockerfile
|   |-- requirements.txt
|   |-- components/
|   |   |-- __init__.py
|   |   +-- sidebar.py              # Sidebar navigation
|   +-- pages/
|       |-- __init__.py
|       |-- overview.py             # Main overview page
|       |-- profitability.py        # Profitability metrics
|       |-- liquidity.py            # Liquidity metrics
|       |-- production.py           # Production metrics
|       |-- all_metrics.py          # All metrics table
|       +-- system_health.py        # ETL monitoring & health
|
|-- etl/                            # ETL Pipeline
|   |-- api.py                      # Flask API for ETL control
|   |-- main.py                     # ETL pipeline entrypoint
|   |-- calculate_metrics.py        # Standalone metrics calculation
|   |-- config.py                   # Pydantic settings
|   |-- Dockerfile
|   |-- requirements.txt
|   |-- extractors/
|   |   |-- __init__.py
|   |   +-- alpha_vantage.py        # Alpha Vantage API client
|   |-- transformers/
|   |   |-- __init__.py
|   |   +-- financial_data.py       # Data transformation & validation
|   |-- loaders/
|   |   |-- __init__.py
|   |   +-- postgres_loader.py      # PostgreSQL UPSERT loader
|   +-- calculators/
|       |-- __init__.py
|       +-- financial_metrics.py    # Financial metrics calculation
|
+-- init-db/
    +-- schema.sql                  # PostgreSQL schema, views, seed data
```

---

## Quick Deploy on Easypanel

### Prerequisites

1. Easypanel installed on VPS
2. Alpha Vantage API key (free): https://www.alphavantage.co/support/#api-key

### Steps

1. **Create PostgreSQL service** (use Easypanel template)
2. **Execute SQL schema** via pgWeb or psql (file: `init-db/schema.sql`)
3. **Deploy ETL App**:
   - Create App from GitHub
   - Point to this repo
   - Dockerfile path: `etl/Dockerfile`
   - Set environment variables (see below)
4. **Deploy Dashboard**:
   - Create App from GitHub
   - Dockerfile path: `dashboard/Dockerfile`
   - Expose port 8501
5. **Configure n8n** (optional, for scheduled runs)

### Environment Variables

**ETL App** (`etl/`):

```
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=windborne_finance
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
ALPHA_VANTAGE_API_KEY=your_api_key_here
TARGET_COMPANIES=TEL,ST,DD
YEARS_TO_FETCH=3
ALPHA_VANTAGE_DELAY=15
```

**Dashboard App** (`dashboard/`):

```
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=windborne_finance
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
```

---

## ETL API Endpoints

The ETL service exposes a Flask API on port 5000:

| Endpoint    | Method | Description                     |
|-------------|--------|---------------------------------|
| `/health`   | GET    | Health check                    |
| `/run-etl`  | POST   | Trigger ETL pipeline execution  |
| `/status`   | GET    | Get last ETL run status from DB |

---

## Production Deployment and Explainers

### 1. Scheduling (n8n Workflow)

Monthly schedule via n8n:

- **Cron Trigger**: 1st day of month, 2 AM UTC
- **HTTP Request**: `POST http://etl-service:5000/run-etl`
- **ETL executes**: `main.py` runs, logs results in `etl_runs` table
- **Alert**: Send notification if status is not SUCCESS

**Workflow Diagram:**

```
[CRON Trigger] -> [HTTP Request: /run-etl] -> [ETL Pipeline] -> [Log Results] -> [Alert on Failure]
```

**Pseudocode:**

```python
def trigger_monthly():
    response = requests.post("http://etl-service:5000/run-etl")
    if response.status_code != 200:
        send_slack_alert("ETL Failed")
```

### 2. API Rate Limit and Scaling

| Scenario      | Companies | Statements | Total Calls |
|---------------|-----------|------------|-------------|
| Current       | 3         | 3          | 9           |
| 100 Companies | 100       | 3          | 300         |

- **Alpha Vantage free tier**: 25 calls/day, 5 calls/min
- **Strategy for 100+ companies**: Rotate companies daily (25/day = full refresh every 4 days)
- **Rate limiting**: Use `ALPHA_VANTAGE_DELAY` (default 15s between calls)
- **Tracking**: API failures logged in `etl_runs` table

### 3. Executive Access via Google Sheets

**Options:**

| Method            | Pros                    | Cons                    |
|-------------------|-------------------------|-------------------------|
| Direct PostgreSQL | Real-time, fast         | Requires secure network |
| CSV Export (cron) | Simple, no dependencies | Stale data              |
| Google Sheets API | Familiar UI for execs   | Requires setup          |

**Example CSV export:**

```bash
psql -h postgres -U user -d windborne_finance \
  -c "COPY (SELECT * FROM v_latest_metrics) TO STDOUT CSV HEADER" > metrics.csv
```

### 4. Monitoring and Alerts

**Built-in monitoring:**

- ETL runs logged in `etl_runs` table with status, duration, records processed
- Dashboard page `system_health.py` shows:
  - Success rate
  - Execution time trends
  - API failure counts
  - Last run details

**Alert logic (to implement in n8n):**

```python
if api_failures > 5:
    send_slack_alert("ETL: High API failures")
if execution_time_seconds > 300:
    send_slack_alert("ETL: Slow run detected")
```

---

## Database Schema

Key tables:

- `companies` - Company symbols and metadata
- `financial_statements` - Raw financial data (JSONB)
- `calculated_metrics` - Computed metrics per year
- `etl_runs` - ETL execution logs

Key views:

- `v_latest_metrics` - Latest metrics per company
- `v_etl_health` - ETL health summary

---

## Local Development

```bash
# Clone
git clone https://github.com/brcassiano/windborne-finance.git
cd windborne-finance

# Set up environment
cp .env.example .env
# Edit .env with your credentials

# Run ETL
cd etl
pip install -r requirements.txt
python main.py

# Run Dashboard
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

---

## License

MIT