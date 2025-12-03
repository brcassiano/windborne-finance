#!/usr/bin/env python3
"""
Main ETL pipeline for WindBorne Finance
Extracts data from Alpha Vantage, transforms, and loads into PostgreSQL
"""

import logging
import time
from datetime import datetime
import sys
import json

from extractors.alpha_vantage import AlphaVantageClient
from transformers.financial_data import FinancialDataTransformer
from loaders.postgres_loader import PostgresLoader
from calculators.financial_metrics import FinancialMetricsCalculator
from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/etl.log')
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main ETL pipeline"""
    start_time = time.time()
    
    logger.info("="*60)
    logger.info("Starting WindBorne ETL Pipeline")
    logger.info("="*60)
    
    # Initialize components
    api_client = AlphaVantageClient()
    transformer = FinancialDataTransformer()
    loader = PostgresLoader()
    calculator = FinancialMetricsCalculator(loader)
    
    # Statistics tracking
    stats = {
        'workflow_name': 'windborne_etl',
        'companies_processed': 0,
        'api_calls_made': 0,
        'api_failures': 0,
        'data_quality_errors': [],
        'status': 'SUCCESS',
        'error_details': None
    }
    
    try:
        companies = settings.companies_list
        logger.info(f"Processing {len(companies)} companies: {companies}")
        
        for symbol in companies:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {symbol}")
            logger.info(f"{'='*60}")
            
            # Get company ID
            company_id = loader.get_company_id(symbol)
            if not company_id:
                logger.error(f"Company {symbol} not found in database")
                stats['api_failures'] += 1
                continue
            
            # Extract data from API
            statements = api_client.fetch_all_statements(symbol)
            stats['api_calls_made'] += len(statements) * 3  # 3 calls per statement type
            
            if not statements:
                logger.warning(f"No data fetched for {symbol}")
                stats['api_failures'] += 3
                continue
            
            # Transform and load each statement type
            total_records = 0
            all_errors = []
            
            for stmt_type, api_data in statements.items():
                # Transform
                records = transformer.transform_to_records(
                    company_id, stmt_type, api_data
                )
                
                # Validate
                errors = transformer.validate_data_quality(records)
                if errors:
                    all_errors.extend([{**e, 'company': symbol, 'statement': stmt_type} for e in errors])
                    logger.warning(f"Data quality issues found: {len(errors)}")
                
                # Load
                inserted = loader.bulk_insert_statements(records)
                total_records += inserted
            
            if all_errors:
                stats['data_quality_errors'].extend(all_errors)
            
            # Update company timestamp
            loader.update_company_timestamp(company_id)
            
            logger.info(f"✓ Loaded {total_records} records for {symbol}")
            
            # Calculate metrics
            logger.info(f"Calculating metrics for {symbol}...")
            calculator.calculate_all_metrics(company_id)
            
            stats['companies_processed'] += 1
        
        logger.info(f"\n{'='*60}")
        logger.info("✓ ETL completed successfully!")
        logger.info(f"{'='*60}")
        
    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        stats['status'] = 'FAILED'
        stats['error_details'] = str(e)
        raise
    
    finally:
        # Log execution
        stats['execution_time_seconds'] = int(time.time() - start_time)
        
        try:
            loader.log_etl_run(stats)
        except Exception as e:
            logger.error(f"Failed to log ETL run: {e}")
        
        # Print summary
        logger.info(f"\n{'='*60}")
        logger.info("Execution Summary:")
        logger.info(f"  Companies processed: {stats['companies_processed']}")
        logger.info(f"  API calls made: {stats['api_calls_made']}")
        logger.info(f"  API failures: {stats['api_failures']}")
        logger.info(f"  Data quality errors: {len(stats['data_quality_errors'])}")
        logger.info(f"  Execution time: {stats['execution_time_seconds']}s")
        logger.info(f"  Status: {stats['status']}")
        logger.info(f"{'='*60}")
        
        # Exit with appropriate code
        sys.exit(0 if stats['status'] == 'SUCCESS' else 1)

if __name__ == "__main__":
    main()
