"""Main Streamlit application"""
import streamlit as st
from datetime import datetime, time

# Page config deve ser a primeira coisa
st.set_page_config(
    page_title="WindBorne Finance Dashboard",
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
    /* === RESPONSIVIDADE GERAL === */
    .main {
        padding: 0rem 1rem;
        max-width: 100%;
        overflow-x: hidden;
    }
    
    /* Força container a respeitar largura */
    .block-container {
        padding-top: 2rem;
        max-width: 100% !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }
    
    /* === SIDEBAR RETRÁTIL === */
    section[data-testid="stSidebar"] {
        width: 280px !important;
        transition: width 0.3s ease;
    }
    
    section[data-testid="stSidebar"][aria-expanded="false"] {
        width: 0px !important;
        margin-left: -280px;
    }
    
    /* Botão de colapsar sidebar mais visível */
    button[kind="header"] {
        background-color: #1f6feb !important;
        color: white !important;
    }
    
    /* === MÉTRICAS === */
    .stMetric {
        background-color: #262730;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #4a4a55;
        min-width: 150px;
    }
    .stMetric label {
        font-size: 0.9rem !important;
        color: #b3b3b3;
        white-space: nowrap;
    }
    .stMetric [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
    }
    
    /* === GRÁFICOS RESPONSIVOS === */
    .js-plotly-plot, .plotly {
        width: 100% !important;
        height: auto !important;
    }
    
    /* Força gráficos a não ultrapassarem container */
    div[data-testid="stPlotlyChart"] {
        width: 100% !important;
        overflow: hidden;
    }
    
    /* === COLUNAS RESPONSIVAS === */
    div[data-testid="column"] {
        min-width: 300px !important;
    }
    
    /* === TABELAS === */
    div[data-testid="stDataFrame"] {
        overflow-x: auto;
    }
    
    /* === EXPANDERS === */
    div[data-testid="stExpander"] {
        border: 1px solid #4a4a55;
        border-radius: 8px;
    }
    
    /* === BOXES CUSTOMIZADOS === */
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
    
    /* === HEADER === */
    header {visibility: hidden;}
    
    /* === ESCONDER MENU DE PÁGINAS PADRÃO === */
    [data-testid="stSidebarNav"] {
        display: none !important;
    }
    
    /* === MEDIA QUERIES PARA RESPONSIVIDADE === */
    
    /* Tablets e telas médias */
    @media (max-width: 1024px) {
        .block-container {
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
        
        .stMetric {
            min-width: 120px;
        }
        
        div[data-testid="column"] {
            min-width: 250px !important;
        }
    }
    
    /* Mobile */
    @media (max-width: 768px) {
        .main {
            padding: 0rem 0.5rem;
        }
        
        .block-container {
            padding-left: 0.5rem !important;
            padding-right: 0.5rem !important;
        }
        
        /* Força colunas a ficarem empilhadas em mobile */
        div[data-testid="column"] {
            width: 100% !important;
            min-width: 100% !important;
        }
        
        .stMetric {
            min-width: 100px;
            padding: 10px;
        }
        
        .stMetric [data-testid="stMetricValue"] {
            font-size: 1.4rem !important;
        }
        
        /* Ajusta altura dos gráficos em mobile */
        .js-plotly-plot {
            height: 300px !important;
        }
    }
    
    /* Telas muito pequenas */
    @media (max-width: 480px) {
        h1 {
            font-size: 1.5rem !important;
        }
        
        h2 {
            font-size: 1.2rem !important;
        }
        
        h3 {
            font-size: 1rem !important;
        }
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