"""Database connection management"""
import os
from sqlalchemy import create_engine
import urllib.parse
import streamlit as st


@st.cache_resource
def get_db_engine():
    """Get SQLAlchemy engine for pandas queries (cached)"""
    try:
        password = urllib.parse.quote_plus(os.getenv('POSTGRES_PASSWORD', ''))
        host = os.getenv('POSTGRES_HOST', 'postgres')
        port = os.getenv('POSTGRES_PORT', 5432)
        database = os.getenv('POSTGRES_DB', 'windborne_finance')
        user = os.getenv('POSTGRES_USER', 'postgres')
        
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10
        )
        
        return engine
    except Exception as e:
        st.error(f"❌ Database engine creation failed: {e}")
        return None


def test_connection():
    """Test database connection"""
    engine = get_db_engine()
    if engine:
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))  # ← text() wrapper!
                version = result.fetchone()
                return True, "PostgreSQL Connected"
        except Exception as e:
            return False, str(e)
    return False, "Engine is None"