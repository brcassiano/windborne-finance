import requests
import time
import logging
from typing import Dict, Optional
from tenacity import retry, wait_exponential, stop_after_attempt
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AlphaVantageClient:
    """Client for Alpha Vantage API"""
    
    STATEMENT_FUNCTIONS = {
        'INCOME': 'INCOME_STATEMENT',
        'BALANCE': 'BALANCE_SHEET',
        'CASHFLOW': 'CASH_FLOW'
    }
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or settings.ALPHA_VANTAGE_API_KEY
        self.base_url = settings.ALPHA_VANTAGE_BASE_URL
        self.delay = settings.ALPHA_VANTAGE_DELAY
        
    @retry(
        wait=wait_exponential(multiplier=1, min=15, max=60),
        stop=stop_after_attempt(3)
    )
    def fetch_statement(
        self, 
        symbol: str, 
        statement_type: str
    ) -> Optional[Dict]:
        """Fetch financial statement from Alpha Vantage"""
        
        function = self.STATEMENT_FUNCTIONS.get(statement_type)
        if not function:
            logger.error(f"Invalid statement type: {statement_type}")
            return None
            
        params = {
            'function': function,
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        logger.info(f"Fetching {statement_type} for {symbol}...")
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
                
            if 'Note' in data:
                logger.warning(f"API Rate Limit: {data['Note']}")
                time.sleep(60)
                return None
                
            # Respect rate limit
            time.sleep(self.delay)
            
            logger.info(f"✓ Fetched {statement_type} for {symbol}")
            return data
            
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None
    
    def fetch_all_statements(self, symbol: str) -> Dict[str, Dict]:
        """Fetch all 3 statement types for a company"""
        results = {}
        
        for stmt_type in ['INCOME', 'BALANCE', 'CASHFLOW']:
            data = self.fetch_statement(symbol, stmt_type)
            if data:
                results[stmt_type] = data
                
        return results
