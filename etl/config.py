from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings):
    # Database
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "windborne_finance"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str
    
    # Alpha Vantage API
    ALPHA_VANTAGE_API_KEY: str
    ALPHA_VANTAGE_BASE_URL: str = "https://www.alphavantage.co/query"
    ALPHA_VANTAGE_RATE_LIMIT: int = 5
    ALPHA_VANTAGE_DELAY: int = 12
    
    # Application
    TARGET_COMPANIES: str = "TEL,ST,DD"
    YEARS_TO_FETCH: int = 3
    
    class Config:
        env_file = ".env"
        case_sensitive = True
    
    @property
    def companies_list(self) -> List[str]:
        return [c.strip() for c in self.TARGET_COMPANIES.split(',')]

settings = Settings()
