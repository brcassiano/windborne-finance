"""Liquidity and solvency metrics page"""
import streamlit as st
import pandas as pd
import plotly.express as px
from database import get_db_engine


def show():
    """Display liquidity metrics"""
    st.markdown("## 💧 Liquidity & Solvency")
    
    engine = get_db_engine()
    if not engine:
        st.error("❌ Cannot connect to database")
        return
    
    try:
        # Get data
        query = """
            SELECT 
                c.symbol,
                cm.fiscal_year,
                MAX(CASE WHEN cm.metric_name = 'current_ratio' 
                    THEN cm.metric_value END) as current_ratio,
                MAX(CASE WHEN cm.metric_name = 'quick_ratio' 
                    THEN cm.metric_value END) as quick_ratio
            FROM calculated_metrics cm
            JOIN companies c ON cm.company_id = c.id
            GROUP BY c.symbol, cm.fiscal_year
            ORDER BY c.symbol, cm.fiscal_year
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            st.warning("⚠️ No liquidity data available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Current Ratio Trend")
            fig = px.line(
                df,
                x='fiscal_year',
                y='current_ratio',
                color='symbol',
                markers=True,
                template="plotly_dark"
            )
            fig.add_hline(y=1.5, line_dash="dash", line_color="yellow", 
                          annotation_text="Healthy Level (1.5)")
            fig.update_layout(height=400, hovermode='x unified')
            fig.update_traces(line=dict(width=3), marker=dict(size=10))
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### Quick Ratio Trend")
            fig = px.line(
                df,
                x='fiscal_year',
                y='quick_ratio',
                color='symbol',
                markers=True,
                template="plotly_dark"
            )
            fig.add_hline(y=1.0, line_dash="dash", line_color="yellow",
                          annotation_text="Healthy Level (1.0)")
            fig.update_layout(height=400, hovermode='x unified')
            fig.update_traces(line=dict(width=3), marker=dict(size=10))
            st.plotly_chart(fig, use_container_width=True)
        
        # Latest ratios
        st.markdown("### 📊 Latest Liquidity Ratios")
        latest_year = df['fiscal_year'].max()
        latest_df = df[df['fiscal_year'] == latest_year]
        
        cols = st.columns(len(latest_df))
        for idx, (_, row) in enumerate(latest_df.iterrows()):
            with cols[idx]:
                st.metric(
                    f"{row['symbol']}",
                    f"Current: {row['current_ratio']:.2f}",
                    f"Quick: {row['quick_ratio']:.2f}" if row['quick_ratio'] else "N/A"
                )
        
    except Exception as e:
        st.error(f"❌ Error: {e}")