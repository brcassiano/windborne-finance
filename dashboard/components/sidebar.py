"""Sidebar component with navigation and controls"""
import streamlit as st
from datetime import datetime
import time
from database import test_connection


def render_sidebar():
    """Render sidebar with navigation and controls"""
    with st.sidebar:
        st.markdown("## 🎯 Navigation")
        page = st.radio(
            "Select View",
            [
                "📈 Overview",
                "💰 Profitability",
                "💧 Liquidity",
                "📊 All Metrics",
                "🔧 System Health",
                "📚 Production Guide"
            ],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        
        # Refresh controls
        st.markdown("### 🔄 Data Refresh")
        
        col1, col2 = st.columns([2, 1])
        with col1:
            if st.button("🔄 Refresh Now", use_container_width=True):
                st.cache_resource.clear()
                st.cache_data.clear()
                st.success("✅ Refreshed!")
                time.sleep(0.5)
                st.rerun()
        
        with col2:
            auto_refresh = st.toggle("Auto")
        
        if auto_refresh:
            st.info("⏱️ Refreshing every 30s")
            time.sleep(30)
            st.rerun()
        
        st.caption(f"🕐 Last update: {datetime.now().strftime('%H:%M:%S')}")
        
        st.markdown("---")
        st.markdown("### 🔌 Connection Status")
        success, msg = test_connection()
        if success:
            st.success(msg)
        else:
            st.error(msg)
        
        st.markdown("---")
        st.markdown("### ℹ️ About")
        st.info("""
        **Data Source:** Alpha Vantage API
        
        **Companies:**
        - TEL - TE Connectivity
        - ST - Sensata Technologies
        - DD - DuPont de Nemours
        
        **Update:** Daily at 8 AM BRT
        
        **Data Period:** Last 4 years
        """)
    
    return page