-- SCHEMA WINDBORNE FINANCE
-- Execute este SQL completo no pgWeb

-- 1. Tabela de empresas
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    industry VARCHAR(100),
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 2. Tabela de statements financeiros
CREATE TABLE IF NOT EXISTS financial_statements (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    statement_type VARCHAR(20) NOT NULL,
    fiscal_year INTEGER NOT NULL,
    fiscal_period VARCHAR(10) DEFAULT 'FY',
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(20, 2),
    reported_currency VARCHAR(3) DEFAULT 'USD',
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_statement_metric UNIQUE(
        company_id, statement_type, fiscal_year, 
        fiscal_period, metric_name
    )
);

-- 3. Tabela de métricas calculadas
CREATE TABLE IF NOT EXISTS calculated_metrics (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    fiscal_year INTEGER NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value NUMERIC(10, 4),
    metric_category VARCHAR(20),
    calculated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_calculated_metric UNIQUE(
        company_id, fiscal_year, metric_name
    )
);

-- 4. Tabela de logs de ETL
CREATE TABLE IF NOT EXISTS etl_runs (
    id SERIAL PRIMARY KEY,
    run_date TIMESTAMP DEFAULT NOW(),
    workflow_name VARCHAR(100),
    companies_processed INTEGER DEFAULT 0,
    api_calls_made INTEGER DEFAULT 0,
    api_failures INTEGER DEFAULT 0,
    data_quality_errors JSONB,
    execution_time_seconds INTEGER,
    status VARCHAR(20),
    error_details TEXT
);

-- 5. Índices para performance
CREATE INDEX IF NOT EXISTS idx_statements_company_year 
    ON financial_statements(company_id, fiscal_year);
    
CREATE INDEX IF NOT EXISTS idx_statements_type_metric 
    ON financial_statements(statement_type, metric_name);
    
CREATE INDEX IF NOT EXISTS idx_metrics_company_year 
    ON calculated_metrics(company_id, fiscal_year);
    
CREATE INDEX IF NOT EXISTS idx_etl_runs_date 
    ON etl_runs(run_date DESC);
    
CREATE INDEX IF NOT EXISTS idx_companies_updated 
    ON companies(updated_at);

-- 6. Views úteis
CREATE OR REPLACE VIEW v_latest_metrics AS
SELECT 
    c.symbol,
    c.name,
    cm.fiscal_year,
    cm.metric_category,
    cm.metric_name,
    cm.metric_value,
    cm.calculated_at
FROM calculated_metrics cm
JOIN companies c ON cm.company_id = c.id
ORDER BY c.symbol, cm.fiscal_year DESC, cm.metric_category;

CREATE OR REPLACE VIEW v_etl_health AS
SELECT 
    run_date,
    workflow_name,
    companies_processed,
    api_calls_made,
    api_failures,
    ROUND(100.0 * api_failures / NULLIF(api_calls_made, 0), 2) as failure_rate_pct,
    execution_time_seconds,
    status,
    CASE 
        WHEN status = 'SUCCESS' THEN '✅'
        WHEN status = 'PARTIAL' THEN '⚠️'
        ELSE '❌'
    END as status_icon
FROM etl_runs
ORDER BY run_date DESC
LIMIT 30;

-- 7. Inserir empresas do desafio
INSERT INTO companies (symbol, name, sector, industry) VALUES
('TEL', 'TE Connectivity', 'Technology', 'Electronic Components'),
('ST', 'Sensata Technologies', 'Technology', 'Electronic Components'),
('DD', 'DuPont de Nemours', 'Materials', 'Chemicals')
ON CONFLICT (symbol) DO NOTHING;

-- 8. Verificar criação
SELECT 'Tabelas criadas:' as status;
SELECT tablename FROM pg_tables WHERE schemaname = 'public';

SELECT 'Empresas inseridas:' as status;
SELECT * FROM companies;
