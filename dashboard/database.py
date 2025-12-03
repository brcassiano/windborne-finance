"""Database connection management"""
import os
import psycopg2
import streamlit as st


def get_db_connection():
    """Get fresh psycopg2 connection for each query"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', 5432),
            database=os.getenv('POSTGRES_DB', 'windborne_finance'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        return None


def test_connection():
    """Test database connection"""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()
            cur.close()
            conn.close()
            return True, "PostgreSQL Connected"
        except Exception as e:
            if conn:
                conn.close()
            return False, str(e)
    return False, "Connection is None"