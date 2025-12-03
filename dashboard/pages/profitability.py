"""Profitability analysis page with margin trends"""
import streamlit as st
import pandas as pd
import plotly.express as px
from database import get_db_connection


def show():
    """Display profitability trends over time"""
    st.markdown("## 💰 Profitability Analysis")
    
    conn = get_db_connection()
    if not conn:
        st.error("❌ Cannot connect to database")
        return
    
    try:
        # Filters
        col1, col2 = st.columns(2)
        
        with col1:
            companies_df = pd.read_sql("""
                SELECT DISTINCT symbol FROM companies ORDER BY symbol
            """, conn)
            selected_companies = st.multiselect(
                "Select Companies",
                companies_df['symbol'].tolist(),
                default=companies_df['symbol'].tolist()
            )
        
        with col2:
            years_df = pd.read_sql("""
                SELECT DISTINCT fiscal_year FROM calculated_metrics 
                ORDER BY fiscal_year DESC
            """, conn)
            selected_years = st.multiselect(
                "Select Years",
                years_df['fiscal_year'].tolist(),
                default=years_df['fiscal_year'].tolist()
            )
        
        if not selected_companies or not selected_years:
            st.info("ℹ️ Please select at least one company and year")
            return
        
        # Query data
        companies_str = "','".join(selected_companies)
        years_str = ",".join(map(str, selected_years))
        
        query = f"""
            SELECT 
                c.symbol,
                cm.fiscal_year,
                MAX(CASE WHEN cm.metric_name = 'gross_margin_pct' 
                    THEN cm.metric_value END) as gross_margin,
                MAX(CASE WHEN cm.metric_name = 'operating_margin_pct' 
                    THEN cm.metric_value END) as operating_margin,
                MAX(CASE WHEN cm.metric_name = 'net_margin_pct' 
                    THEN cm.metric_value END) as net_margin
            FROM calculated_metrics cm
            JOIN companies c ON cm.company_id = c.id
            WHERE c.symbol IN ('{companies_str}')
            AND cm.fiscal_year IN ({years_str})
            GROUP BY c.symbol, cm.fiscal_year
            ORDER BY c.symbol, cm.fiscal_year
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            st.warning("⚠️ No data found for selected filters")
            return
        
        # Create trend charts
        metrics = [
            ('gross_margin', 'Gross Margin %', '#00CC96'),
            ('operating_margin', 'Operating Margin %', '#AB63FA'),
            ('net_margin', 'Net Margin %', '#FFA15A')
        ]
        
        for metric_col, title, color in metrics:
            st.markdown(f"### {title}")
            fig = px.line(
                df,
                x='fiscal_year',
                y=metric_col,
                color='symbol',
                markers=True,
                template="plotly_dark"
            )
            fig.update_layout(
                height=400,
                xaxis_title="Fiscal Year",
                yaxis_title=title,
                legend_title="Company",
                hovermode='x unified'
            )
            fig.update_traces(line=dict(width=3), marker=dict(size=10))
            st.plotly_chart(fig, use_container_width=True)
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
    finally:
        if conn:
            conn.close()