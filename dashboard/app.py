"""Main Streamlit application"""
import streamlit as st

# Page config DEVE ser a PRIMEIRA coisa (antes de qualquer outro código)
st.set_page_config(
    page_title="WindBorne Finance",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

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


def main():
    """Main application logic"""
    
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