"""Main Streamlit application"""
import streamlit as st
from datetime import datetime, time

# Page config DEVE ser a PRIMEIRA coisa (antes de qualquer outro código)
st.set_page_config(
    page_title="WindBorne Finance",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# AGORA sim, silenciar warnings (depois do set_page_config)
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# Imports do projeto
from components.sidebar import render_sidebar
from pages import overview, profitability, liquidity, all_metrics, system_health, production


# OCULTAR MENU HAMBURGER E PÁGINAS AUTOMÁTICAS
st.markdown("""
    <style>
        /* Ocultar menu hamburger */
        [data-testid="stSidebarNav"] {display: none;}
        
        /* Ocultar header do Streamlit */
        header {visibility: hidden;}
        
        /* Ajustar padding do topo */
        .block-container {padding-top: 2rem;}
    </style>
""", unsafe_allow_html=True)


# Custom CSS
st.markdown("""
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
    </style>
""", unsafe_allow_html=True)


# AUTO-REFRESH: Verificar se precisa recarregar dados (1x por dia às 8:30 AM)
def check_auto_refresh():
    """Check if data needs to be refreshed based on ETL schedule"""
    now = datetime.now()
    
    # Inicializar session state
    if 'last_refresh_date' not in st.session_state:
        st.session_state.last_refresh_date = now.date()
    
    # Verificar se passou das 8:30 AM e ainda não atualizou hoje
    refresh_time = time(8, 30)  # 8:30 AM (30min após o ETL às 8:00 AM)
    
    if now.time() > refresh_time and st.session_state.last_refresh_date < now.date():
        st.session_state.last_refresh_date = now.date()
        st.cache_data.clear()  # Limpar cache do Streamlit
        st.rerun()  # Recarregar aplicação


def main():
    """Main application logic"""
    
    # Verificar auto-refresh
    check_auto_refresh()
    
    # Header
    st.markdown("# 📊 WindBorne Finance Dashboard")
    st.markdown("### Real-time financial metrics for TEL, ST, and DD")
    st.markdown("---")
    
    # Render sidebar and get selected page
    page = render_sidebar()
    
    # Route to appropriate page
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