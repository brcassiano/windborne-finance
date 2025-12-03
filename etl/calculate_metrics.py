#!/usr/bin/env python3
"""
Standalone script to calculate metrics for all companies
Can be called separately from n8n after data ingestion
"""

import logging
import sys
from loaders.postgres_loader import PostgresLoader
from calculators.financial_metrics import FinancialMetricsCalculator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Calculate metrics for all companies"""
    loader = PostgresLoader()
    calculator = FinancialMetricsCalculator(loader)
    
    # Get all companies
    with loader.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM companies")
            companies = cur.fetchall()
    
    logger.info(f"Calculating metrics for {len(companies)} companies...")
    
    for company_id, symbol in companies:
        logger.info(f"Processing {symbol}...")
        try:
            calculator.calculate_all_metrics(company_id)
            logger.info(f"✓ Completed {symbol}")
        except Exception as e:
            logger.error(f"Failed to calculate metrics for {symbol}: {e}")
    
    logger.info("✓ All metrics calculated")

if __name__ == "__main__":
    main()
