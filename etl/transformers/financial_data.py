import logging
from typing import List, Dict
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class FinancialDataTransformer:
    """Transform Alpha Vantage responses to database schema"""
    
    # Field mappings from API to standardized names
    FIELD_MAPPINGS = {
        'INCOME': {
            'totalRevenue': 'total_revenue',
            'costOfRevenue': 'cost_of_revenue',
            'grossProfit': 'gross_profit',
            'operatingIncome': 'operating_income',
            'netIncome': 'net_income',
            'ebitda': 'ebitda',
            'researchAndDevelopment': 'research_and_development',
            'operatingExpenses': 'operating_expenses',
        },
        'BALANCE': {
            'totalAssets': 'total_assets',
            'totalCurrentAssets': 'current_assets',
            'cashAndCashEquivalentsAtCarryingValue': 'cash_and_equivalents',
            'inventory': 'inventory',
            'totalLiabilities': 'total_liabilities',
            'totalCurrentLiabilities': 'current_liabilities',
            'totalShareholderEquity': 'total_equity',
            'longTermDebt': 'long_term_debt',
            'currentDebt': 'current_debt',
        },
        'CASHFLOW': {
            'operatingCashflow': 'operating_cashflow',
            'cashflowFromInvestment': 'investing_cashflow',
            'cashflowFromFinancing': 'financing_cashflow',
            'capitalExpenditures': 'capital_expenditures',
        }
    }
    
    def transform_to_records(
        self, 
        company_id: int,
        statement_type: str, 
        api_response: Dict
    ) -> List[Dict]:
        """Transform API response to list of database records"""
        from config import settings
        from datetime import datetime
        import json

        records = []

        # Alpha Vantage returns 'annualReports' for annual data
        all_reports = api_response.get('annualReports', [])

        if not all_reports:
            logger.warning(f"No annual reports found for {statement_type}")
            return records

        # Filter to only last N years (from config)
        current_year = datetime.now().year
        years_to_fetch = settings.YEARS_TO_FETCH
        min_year = current_year - years_to_fetch

        reports = []
        for report in all_reports:
            fiscal_date = report.get('fiscalDateEnding', '')
            if fiscal_date:
                fiscal_year = int(fiscal_date[:4])
                if fiscal_year >= min_year:
                    reports.append(report)

        logger.info(
            f"{statement_type}: Filtered {len(all_reports)} reports to "
            f"{len(reports)} (years {min_year}-{current_year})"
        )

        field_map = self.FIELD_MAPPINGS.get(statement_type, {})

        for report in reports:
            fiscal_date = report.get('fiscalDateEnding', '')
            fiscal_year = int(fiscal_date[:4]) if fiscal_date else None

            if not fiscal_year:
                continue

            # Transform each field in report to a record
            for api_field, db_field in field_map.items():
                value = report.get(api_field)

                # Convert string to numeric
                if value and value != 'None':
                    try:
                        numeric_value = float(value)
                    except (ValueError, TypeError):
                        numeric_value = None
                else:
                    numeric_value = None

                records.append({
                    'company_id': company_id,
                    'statement_type': statement_type,
                    'fiscal_year': fiscal_year,
                    'fiscal_period': 'FY',
                    'metric_name': db_field,
                    'metric_value': numeric_value,
                    'reported_currency': 'USD',
                    'raw_data': json.dumps(report)
                })

        logger.info(f"Transformed {len(records)} records for {statement_type}")
        return records
    
    def validate_data_quality(self, records: List[Dict]) -> List[Dict]:
        """Validate data quality and return list of errors"""
        errors = []
        
        # Group records by year for validation
        by_year = {}
        for record in records:
            year = record['fiscal_year']
            if year not in by_year:
                by_year[year] = {}
            by_year[year][record['metric_name']] = record['metric_value']
        
        for year, metrics in by_year.items():
            # Check 1: Revenue should be positive
            revenue = metrics.get('total_revenue')
            if revenue is not None and revenue < 0:
                errors.append({
                    'year': year,
                    'error': 'negative_revenue',
                    'value': revenue
                })
            
            # Check 2: Balance sheet should balance
            assets = metrics.get('total_assets')
            liabilities = metrics.get('total_liabilities')
            equity = metrics.get('total_equity')
            
            if all([assets, liabilities, equity]):
                diff = abs(assets - (liabilities + equity))
                # Allow 1% tolerance due to rounding
                tolerance = assets * 0.01 if assets > 0 else 1000
                
                if diff > tolerance:
                    errors.append({
                        'year': year,
                        'error': 'balance_sheet_mismatch',
                        'difference': diff,
                        'assets': assets,
                        'liabilities_equity': liabilities + equity
                    })
            
            # Check 3: Missing critical fields
            required = ['total_revenue', 'net_income', 'total_assets']
            missing = [f for f in required if metrics.get(f) is None]
            if missing:
                errors.append({
                    'year': year,
                    'error': 'missing_fields',
                    'fields': missing
                })
        
        if errors:
            logger.warning(f"Found {len(errors)} data quality issues")
        
        return errors
