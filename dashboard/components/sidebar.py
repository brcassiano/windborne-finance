"""Sidebar component with navigation and controls"""
import streamlit as st
import pandas as pd
from database import test_connection, get_db_engine


def render_sidebar():
    """Render sidebar with navigation and data refresh"""
    with st.sidebar:
        st.markdown("## 🧭 Navigation")
        
        page = st.radio(
            "Select Page",
            ["📊 Overview", "💰 Profitability", "💧 Liquidity", 
             "📈 All Metrics", "🏥 System Health", "📚 Production Guide"],
            key="main_navigation",
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        
        # Data Refresh section
        st.markdown("## 🔄 Data Refresh")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            if st.button("🔄 Refresh Now", use_container_width=True):
                st.cache_data.clear()
                st.rerun()
        
        with col2:
            auto_refresh = st.toggle("Auto", value=False, key="auto_refresh_toggle")
        
        if auto_refresh:
            import time
            time.sleep(300)  # 5 minutes
            st.rerun()
        
        # Last update info
        from datetime import datetime
        st.caption(f"🕐 Last update: {datetime.now().strftime('%H:%M:%S')}")
        
        st.markdown("---")
        
        # Connection Status
        st.markdown("## 🔌 Connection Status")
        connected, message = test_connection()
        
        if connected:
            st.success("✅ PostgreSQL Connected")
        else:
            st.error(f"❌ {message}")
        
        st.markdown("---")
        
        # About section (DINÂMICO)
        st.markdown("## 📘 About")
        
        # Buscar empresas do banco de dados
        engine = get_db_engine()
        if engine:
            try:
                df_companies = pd.read_sql(
                    "SELECT symbol, name FROM companies ORDER BY symbol",
                    engine
                )
                
                if not df_companies.empty:
                    st.markdown("**Data Source:** Alpha Vantage API")
                    st.markdown(f"**Total Companies:** {len(df_companies)}")
                    st.markdown("")
                    
                    # Listar empresas dinamicamente
                    for _, row in df_companies.iterrows():
                        st.markdown(f"• **{row['symbol']}** - {row['name']}")
                else:
                    st.warning("⚠️ No companies in database")
            except Exception as e:
                st.error(f"❌ Error: {str(e)[:50]}")
        else:
            # Fallback se banco estiver indisponível
            st.markdown("""
            **Data Source:** Alpha Vantage API
            
            **Status:** Database unavailable
            """)
    
    return page