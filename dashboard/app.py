"""Main Streamlit application"""
import streamlit as st
from datetime import datetime, time

# Page config deve ser a primeira coisa
st.set_page_config(
    page_title="WindBorne Finance",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from components.sidebar import render_sidebar
from pages import overview, profitability, liquidity, all_metrics, system_health, production

# CSS global
st.markdown(
    """
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #262730;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #4a4a55;
    }
    .stMetric label {
        font-size: 0.9rem !important;
        color: #b3b3b3;
    }
    .stMetric [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
    }
    div[data-testid="stExpander"] {
        border: 1px solid #4a4a55;
        border-radius: 8px;
    }
    .production-box {
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #00CC96;
        margin: 10px 0;
    }
    .warning-box {
        background-color: #2d1e1e;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #FFA15A;
        margin: 10px 0;
    }
    .code-box {
        background-color: #1a1a1a;
        padding: 15px;
        border-radius: 8px;
        font-family: 'Courier New', monospace;
        font-size: 0.9rem;
        line-height: 1.6;
        white-space: pre;
        overflow-x: auto;
    }
    header {visibility: hidden;}
    .block-container {padding-top: 2rem;}

    /* === ESCONDER MENU PADRÃO DO APP NA SIDEBAR === */
    /* Navegação multipage nativa */
    [data-testid="stSidebarNav"] {
        display: none !important;
    }
    /* Bloco superior onde aparece o título "app" */
    section[data-testid="stSidebar"] > div:nth-of-type(1) {
        display: none !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def check_auto_refresh():
    """
    Recarrega dados 1x por dia após o ETL (8h SP),
    se o toggle 'Auto' estiver ligado.
    """
    now = datetime.now()

    if "auto_refresh" not in st.session_state:
        st.session_state.auto_refresh = True

    if not st.session_state.auto_refresh:
        return

    if "last_refresh_date" not in st.session_state:
        st.session_state.last_refresh_date = now.date()

    # 8h30 local (São Paulo)
    refresh_time = time(8, 30)

    if now.time() > refresh_time and st.session_state.last_refresh_date < now.date():
        st.session_state.last_refresh_date = now.date()
        st.cache_data.clear()
        st.cache_resource.clear()
        st.experimental_rerun()


def main():
    check_auto_refresh()

    st.markdown("# 📊 WindBorne Finance Dashboard")
    st.markdown("### Real-time financial metrics for TEL, ST, and DD")
    st.markdown("---")

    page = render_sidebar()

    if page == "📊 Overview":
        overview.show()
    elif page == "💰 Profitability":
        profitability.show()
    elif page == "💧 Liquidity":
        liquidity.show()
    elif page == "📈 All Metrics":
        all_metrics.show()
    elif page == "🏥 System Health":
        system_health.show()
    elif page == "📚 Production Guide":
        production.show()


if __name__ == "__main__":
    main()