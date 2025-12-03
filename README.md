# WindBorne Finance Automation

Automated financial analysis pipeline using Alpha Vantage API, PostgreSQL, n8n, and Streamlit.

## Tech Stack

- **ETL**: Python 3.11
- **Database**: PostgreSQL 15
- **Orchestration**: n8n
- **Dashboard**: Streamlit
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

For ETL App (example):
```env
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=windborne_finance
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
ALPHA_VANTAGE_API_KEY=your_api_key_here
```

For Dashboard App (example):
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
windborne-finance/
├── etl/                 # Python ETL pipeline
│   ├── extractors/      # Alpha Vantage API client
│   ├── transformers/    # Data transformation
│   ├── loaders/         # PostgreSQL loader
│   └── calculators/     # Financial metrics
├── dashboard/           # Streamlit dashboard
│   ├── app.py
   └── requirements.txt
├── init-db/             # SQL schema
│   └── schema.sql
└── grafana/             # Monitoring dashboards
    └── dashboards/
```

## License

MIT

