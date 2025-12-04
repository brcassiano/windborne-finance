"""Sidebar component for navigation and controls"""
import streamlit as st
from datetime import datetime
from database import test_connection  # se o nome for diferente, ajuste aqui


def render_sidebar() -> str:
    """Render the left sidebar and return selected page name."""

    with st.sidebar:
        st.markdown("## 🧭 Navigation")

        page = st.radio(
            "Select View",
            ["📊 Overview", "💰 Profitability", "💧 Liquidity",
             "📈 All Metrics", "🏥 System Health", "📚 Production Guide"],
            label_visibility="collapsed",
        )

        st.markdown("---")
        st.markdown("### 🔄 Data Refresh")

        # Inicializa estado do auto_refresh
        if "auto_refresh" not in st.session_state:
            st.session_state.auto_refresh = True  # ligado por padrão

        col1, col2 = st.columns([2, 1])

        with col1:
            if st.button("Refresh Now", use_container_width=True):
                st.cache_resource.clear()
                st.cache_data.clear()
                st.success("Refreshed!")
                st.experimental_rerun()

        with col2:
            st.toggle("Auto", key="auto_refresh")

        st.caption(f"Last update: {datetime.now().strftime('%H:%M:%S')}")
        st.markdown("---")

        st.markdown("### 🌐 Connection Status")
        success, msg = test_connection()
        if success:
            st.success(msg)
        else:
            st.error(msg)

        st.markdown("---")
        st.markdown("### ℹ️ About")
        st.info(
            "Data Source: Alpha Vantage API\n\n"
            "Total Companies: 3\n\n"
            "- DD - DuPont de Nemours\n"
            "- ST - Sensata Technologies\n"
            "- TEL - TE Connectivity\n\n"
            "Update: Daily at 8 AM (America/Sao_Paulo)\n"
            "Data Period: Last 4 years"
        )

    return page