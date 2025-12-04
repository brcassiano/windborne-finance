# WindBorne Finance Automation

Automated financial analysis pipeline using Alpha Vantage API, PostgreSQL, n8n, and Streamlit.

## Tech Stack

- **ETL**: Python 3.11
- **Database**: PostgreSQL 15
- **Orchestration**: n8n
- **Dashboard**: Streamlit
- **Monitoring**: Grafana
- **Deployment**: Easypanel (Docker)

## Quick Deploy on Easypanel

### Prerequisites
1. Easypanel installed on VPS
2. Alpha Vantage API key (free): https://www.alphavantage.co/support/#api-key

### Steps

1. **Create PostgreSQL service** (Template)
2. **Execute SQL schema** via pgWeb (file: `init-db/schema.sql`)
3. **Deploy ETL App**:
   - Create App from GitHub
   - Point to this repo
   - Dockerfile path: `etl/Dockerfile`
   - Set environment variables (see below)
4. **Deploy Dashboard**:
   - Create App from GitHub
   - Dockerfile path: `dashboard/Dockerfile`
5. **Import n8n workflows** (file: `n8n/workflows/*.json`)
6. **Import Grafana dashboard** (file: `grafana/dashboards/*.json`)

### Environment Variables

For ETL App:
```env
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=windborne_finance
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
ALPHA_VANTAGE_API_KEY=your_api_key_here
TARGET_COMPANIES=TEL,ST,DD
YEARS_TO_FETCH=3
```

For Dashboard App:
```env
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=windborne_finance
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
```

## Project Structure

Use the tree below to quickly visualise the repository layout:

```text
etl/
├── extractors/         # Alpha Vantage API client
├── transformers/       # Data transformation
├── loaders/            # PostgreSQL loader
├── calculators/        # Financial metrics
├── main.py             # ETL pipeline entrypoint
├── api.py              # Flask API for ETL control
├── requirements.txt
├── Dockerfile
dashboard/
├── app.py              # Streamlit dashboard
├── requirements.txt
├── Dockerfile
init-db/
├── schema.sql          # SQL schema
grafana/
└── dashboards/         # Monitoring dashboards
```

---

## Production Deployment & Explainers

### 1. Scheduling (n8n Workflow)

Monthly schedule via n8n:
- Cron Trigger (monthly, e.g. 1st day, 2 AM UTC)
- HTTP Request: `POST http://etl-service:5000/run-etl`
- ETL executes `main.py`, logs results in `etl_runs`
- Alert if status != 'SUCCESS'

Pseudocode:
```python
def trigger_monthly():
   response = requests.post("http://etl-service:5000/run-etl")
   if response.status_code != 200:
      send_slack_alert("ETL Failed")
```

Workflow Diagram:
```
[CRON Trigger] → [HTTP Request: /run-etl] → [ETL Pipeline] → [Log Results] → [Alert on Failure]
```

### 2. API Rate Limit & Scaling

- Current: 3 companies × 3 statements = 9 calls/run
- For 100 companies: 100 × 3 = 300 calls needed
- Alpha Vantage limit (free): 25 calls/day
- Strategy: rotate companies, e.g. 25/day, full refresh every 4 days

Notes:
- Respect 5 calls/min using `ALPHA_VANTAGE_DELAY`
- Track API failures in `etl_runs` for monitoring

### 3. Exec Access via Google Sheets

Options:
- Direct PostgreSQL connection (fast, needs secure network)
- CSV export (simple, cron-based)
- Google Sheets API (recommended for execs)

Example CSV export:
```bash
psql -h postgres -U user -d windborne_finance \
  -c "COPY (SELECT * FROM v_latest_metrics) TO STDOUT CSV" > metrics.csv
```

### 4. Monitoring & Alerts

- ETL runs logged in `etl_runs` table
- Dashboard shows success rate, execution time, API failures
- Add Slack/email alerts for high failure rate or slow runs

Example alert logic:
```python
if api_failures > 5:
   send_slack_alert("ETL: High API failures")
if execution_time_seconds > 300:
   send_slack_alert("ETL: Slow run detected")
```

## License

MIT

