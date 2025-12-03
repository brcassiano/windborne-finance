import psycopg2
from psycopg2.extras import execute_values, Json
from typing import List, Dict, Optional
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

logger = logging.getLogger(__name__)

class PostgresLoader:
    """Load data into PostgreSQL"""
    
    def __init__(self):
        self.conn_params = {
            'host': settings.POSTGRES_HOST,
            'port': settings.POSTGRES_PORT,
            'database': settings.POSTGRES_DB,
            'user': settings.POSTGRES_USER,
            'password': settings.POSTGRES_PASSWORD
        }
    
    def get_connection(self):
        """Create database connection"""
        try:
            return psycopg2.connect(**self.conn_params)
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def get_company_id(self, symbol: str) -> Optional[int]:
        """Get company ID by symbol"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM companies WHERE symbol = %s",
                    (symbol,)
                )
                result = cur.fetchone()
                return result[0] if result else None
    
    def bulk_insert_statements(self, records: List[Dict]) -> int:
        """Bulk insert financial statements with UPSERT"""
        if not records:
            return 0
            
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO financial_statements (
                        company_id, statement_type, fiscal_year, 
                        fiscal_period, metric_name, metric_value, 
                        reported_currency, raw_data
                    ) VALUES %s
                    ON CONFLICT (company_id, statement_type, fiscal_year, fiscal_period, metric_name)
                    DO UPDATE SET
                        metric_value = EXCLUDED.metric_value,
                        raw_data = EXCLUDED.raw_data,
                        created_at = NOW()
                """
                
                values = [
                    (
                        r['company_id'],
                        r['statement_type'],
                        r['fiscal_year'],
                        r['fiscal_period'],
                        r['metric_name'],
                        r['metric_value'],
                        r['reported_currency'],
                        r['raw_data']
                    )
                    for r in records
                ]
                
                execute_values(cur, query, values)
                conn.commit()
                
                logger.info(f"✓ Inserted/updated {len(records)} statement records")
                return len(records)
    
    def update_company_timestamp(self, company_id: int):
        """Update company's updated_at timestamp"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE companies SET updated_at = NOW() WHERE id = %s",
                    (company_id,)
                )
                conn.commit()
    
    def log_etl_run(self, run_data: Dict):
        """Log ETL execution to etl_runs table"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO etl_runs (
                        workflow_name, companies_processed, api_calls_made,
                        api_failures, data_quality_errors, execution_time_seconds,
                        status, error_details
                    ) VALUES (
                        %(workflow_name)s, %(companies_processed)s, %(api_calls_made)s,
                        %(api_failures)s, %(data_quality_errors)s, %(execution_time_seconds)s,
                        %(status)s, %(error_details)s
                    )
                """, {
                    **run_data,
                    'data_quality_errors': Json(run_data.get('data_quality_errors', []))
                })
                conn.commit()
                logger.info("✓ Logged ETL run to database")
