import logging
from typing import Dict, List
import psycopg2.extras
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from loaders.postgres_loader import PostgresLoader

logger = logging.getLogger(__name__)

class FinancialMetricsCalculator:
    """Calculate financial metrics from statements"""
    
    def __init__(self, postgres_loader: PostgresLoader = None):
        self.loader = postgres_loader or PostgresLoader()
    
    def get_statement_data(self, company_id: int, fiscal_year: int) -> Dict:
        """Fetch all statement data for a company/year"""
        with self.loader.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT metric_name, metric_value
                    FROM financial_statements
                    WHERE company_id = %s AND fiscal_year = %s
                """, (company_id, fiscal_year))
                
                return {row['metric_name']: row['metric_value'] for row in cur.fetchall()}
    
    def calculate_profitability_metrics(self, data: Dict) -> List[Dict]:
        """Calculate profitability ratios"""
        metrics = []
        revenue = data.get('total_revenue') or 0
        
        if revenue and revenue > 0:
            # Gross Margin %
            cogs = data.get('cost_of_revenue') or 0
            gross_margin = ((revenue - cogs) / revenue) * 100
            metrics.append({
                'metric_name': 'gross_margin_pct',
                'metric_value': round(gross_margin, 2),
                'metric_category': 'PROFITABILITY'
            })
            
            # Operating Margin %
            operating_income = data.get('operating_income') or 0
            if operating_income:
                operating_margin = (operating_income / revenue) * 100
                metrics.append({
                    'metric_name': 'operating_margin_pct',
                    'metric_value': round(operating_margin, 2),
                    'metric_category': 'PROFITABILITY'
                })
            
            # Net Margin %
            net_income = data.get('net_income') or 0
            if net_income:
                net_margin = (net_income / revenue) * 100
                metrics.append({
                    'metric_name': 'net_margin_pct',
                    'metric_value': round(net_margin, 2),
                    'metric_category': 'PROFITABILITY'
                })
        
        return metrics
    
    def calculate_liquidity_metrics(self, data: Dict) -> List[Dict]:
        """Calculate liquidity ratios"""
        metrics = []
        current_assets = data.get('current_assets') or 0
        current_liabilities = data.get('current_liabilities') or 0
        
        if current_liabilities and current_liabilities > 0:
            # Current Ratio
            current_ratio = current_assets / current_liabilities
            metrics.append({
                'metric_name': 'current_ratio',
                'metric_value': round(current_ratio, 2),
                'metric_category': 'LIQUIDITY'
            })
            
            # Quick Ratio
            inventory = data.get('inventory') or 0
            quick_ratio = (current_assets - inventory) / current_liabilities
            metrics.append({
                'metric_name': 'quick_ratio',
                'metric_value': round(quick_ratio, 2),
                'metric_category': 'LIQUIDITY'
            })
        
        return metrics
    
    def calculate_efficiency_metrics(self, data: Dict, prev_data: Dict) -> List[Dict]:
        """Calculate efficiency ratios"""
        metrics = []
        revenue = data.get('total_revenue') or 0
        
        # Asset Turnover (requires previous year for average)
        current_assets = data.get('total_assets') or 0
        prev_assets = prev_data.get('total_assets') or 0
        
        if current_assets and prev_assets:
            avg_assets = (current_assets + prev_assets) / 2
            if avg_assets > 0 and revenue:
                asset_turnover = revenue / avg_assets
                metrics.append({
                    'metric_name': 'asset_turnover',
                    'metric_value': round(asset_turnover, 2),
                    'metric_category': 'EFFICIENCY'
                })
        
        return metrics
    
    def calculate_growth_metrics(self, data: Dict, prev_data: Dict) -> List[Dict]:
        """Calculate YoY growth metrics"""
        metrics = []
        
        # Revenue YoY %
        revenue = data.get('total_revenue') or 0
        prev_revenue = prev_data.get('total_revenue') or 0
        
        if prev_revenue and prev_revenue > 0:
            revenue_yoy = ((revenue - prev_revenue) / prev_revenue) * 100
            metrics.append({
                'metric_name': 'revenue_yoy_pct',
                'metric_value': round(revenue_yoy, 2),
                'metric_category': 'GROWTH'
            })
        
        # Net Income YoY %
        net_income = data.get('net_income') or 0
        prev_net_income = prev_data.get('net_income') or 0
        
        if prev_net_income and prev_net_income != 0:
            ni_yoy = ((net_income - prev_net_income) / abs(prev_net_income)) * 100
            metrics.append({
                'metric_name': 'net_income_yoy_pct',
                'metric_value': round(ni_yoy, 2),
                'metric_category': 'GROWTH'
            })
        
        return metrics
    
    def calculate_all_metrics(self, company_id: int):
        """Calculate all metrics for a company across all years"""
        with self.loader.get_connection() as conn:
            with conn.cursor() as cur:
                # Get distinct years for this company
                cur.execute("""
                    SELECT DISTINCT fiscal_year 
                    FROM financial_statements 
                    WHERE company_id = %s 
                    ORDER BY fiscal_year DESC
                """, (company_id,))
                
                years = [row[0] for row in cur.fetchall()]
        
        logger.info(f"Calculating metrics for company {company_id}, years: {years}")
        
        for i, year in enumerate(years):
            data = self.get_statement_data(company_id, year)
            prev_data = self.get_statement_data(company_id, years[i+1]) if i+1 < len(years) else {}
            
            all_metrics = []
            all_metrics.extend(self.calculate_profitability_metrics(data))
            all_metrics.extend(self.calculate_liquidity_metrics(data))
            
            if prev_data:
                all_metrics.extend(self.calculate_efficiency_metrics(data, prev_data))
                all_metrics.extend(self.calculate_growth_metrics(data, prev_data))
            
            # Insert metrics
            self._insert_metrics(company_id, year, all_metrics)
            logger.info(f"✓ Calculated {len(all_metrics)} metrics for year {year}")
    
    def _insert_metrics(self, company_id: int, fiscal_year: int, metrics: List[Dict]):
        """Insert calculated metrics into database"""
        if not metrics:
            return
            
        with self.loader.get_connection() as conn:
            with conn.cursor() as cur:
                for metric in metrics:
                    cur.execute("""
                        INSERT INTO calculated_metrics (
                            company_id, fiscal_year, metric_name, 
                            metric_value, metric_category
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (company_id, fiscal_year, metric_name)
                        DO UPDATE SET
                            metric_value = EXCLUDED.metric_value,
                            metric_category = EXCLUDED.metric_category,
                            calculated_at = NOW()
                    """, (
                        company_id,
                        fiscal_year,
                        metric['metric_name'],
                        metric['metric_value'],
                        metric['metric_category']
                    ))
                conn.commit()
