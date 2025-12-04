"""Overview page with key financial metrics"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from database import get_db_engine


def show():
    """Display overview page with latest metrics"""
    st.markdown("## 📊 Overview")
    
    engine = get_db_engine()
    if not engine:
        st.error("❌ Cannot connect to database")
        return
    
    # Top-level metrics
    st.markdown("### 📈 Key Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        companies_count = pd.read_sql("SELECT COUNT(*) as count FROM companies", engine)
        st.metric("🏢 Companies", companies_count['count'].iloc[0])
    
    with col2:
        years_query = """
            SELECT COUNT(DISTINCT fiscal_year) as count 
            FROM calculated_metrics
        """
        years_count = pd.read_sql(years_query, engine)
        st.metric("📅 Years of Data", years_count['count'].iloc[0])
    
    with col3:
        metrics_query = """
            SELECT COUNT(*) as count 
            FROM calculated_metrics
        """
        metrics_count = pd.read_sql(metrics_query, engine)
        st.metric("📊 Total Metrics", metrics_count['count'].iloc[0])
    
    with col4:
        last_update_query = """
            SELECT MAX(updated_at) as last_update 
            FROM companies
        """
        last_update = pd.read_sql(last_update_query, engine)
        if not last_update.empty and pd.notna(last_update['last_update'].iloc[0]):
            st.metric("🕐 Last Update", last_update['last_update'].iloc[0].strftime("%m/%d/%Y"))
        else:
            st.metric("🕐 Last Update", "N/A")
    
    st.markdown("---")
    
    # Latest Performance Section
    st.markdown("## 📊 Latest Performance (Most Recent Year Per Company)")
    
    query = """
        SELECT 
            c.symbol,
            c.name,
            cm.fiscal_year,
            MAX(CASE WHEN cm.metric_name = 'gross_margin_pct' THEN cm.metric_value END) as gross_margin,
            MAX(CASE WHEN cm.metric_name = 'operating_margin_pct' THEN cm.metric_value END) as operating_margin,
            MAX(CASE WHEN cm.metric_name = 'net_margin_pct' THEN cm.metric_value END) as net_margin,
            MAX(CASE WHEN cm.metric_name = 'current_ratio' THEN cm.metric_value END) as current_ratio,
            MAX(CASE WHEN cm.metric_name = 'revenue_yoy_pct' THEN cm.metric_value END) as revenue_growth
        FROM companies c
        JOIN calculated_metrics cm ON c.id = cm.company_id
        WHERE cm.fiscal_year = (
            SELECT MAX(fiscal_year) 
            FROM calculated_metrics 
            WHERE company_id = c.id
        )
        GROUP BY c.symbol, c.name, cm.fiscal_year
        ORDER BY c.symbol
    """
    
    df = pd.read_sql(query, engine)
    
    if not df.empty:
        # Info box showing which years are displayed
        years_shown = df.groupby('symbol')['fiscal_year'].first().to_dict()
        years_text = ", ".join([f"{symbol}: {year}" for symbol, year in years_shown.items()])
        st.info(f"📅 Years shown: {years_text}")
        
        st.markdown("---")
        
        # Two column layout for charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 💰 Profitability Margins")
            fig = go.Figure()
            
            for col_name, name, color in [
                ('gross_margin', 'Gross Margin', '#00CC96'),
                ('operating_margin', 'Operating Margin', '#AB63FA'),
                ('net_margin', 'Net Margin', '#FFA15A')
            ]:
                fig.add_trace(go.Bar(
                    name=name,
                    x=df['symbol'],
                    y=df[col_name],
                    text=df[col_name].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A"),
                    textposition='auto',
                    textfont=dict(
                        size=14,
                        color='white',
                        family='Arial Black'
                    ),
                    marker_color=color,
                    customdata=df[['fiscal_year']],
                    hovertemplate='<b>%{x}</b><br>Year: %{customdata[0]}<br>' + name + ': %{y:.2f}%<extra></extra>'
                ))
            
            fig.update_layout(
                barmode='group',
                height=400,
                xaxis_title="Company",
                yaxis_title="Margin (%)",
                template="plotly_dark",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### 📊 Financial Health")
            
            fig = go.Figure()
            
            # Current Ratio (barras)
            fig.add_trace(go.Bar(
                name='Current Ratio',
                x=df['symbol'],
                y=df['current_ratio'],
                yaxis='y',
                marker_color='#636EFA',
                text=df['current_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"),
                textposition='auto',
                textfont=dict(
                    size=14,
                    color='white',
                    family='Arial Black'
                ),
                customdata=df[['fiscal_year']],
                hovertemplate='<b>%{x}</b><br>Year: %{customdata[0]}<br>Current Ratio: %{y:.2f}<extra></extra>'
            ))
            
            # Revenue Growth (linha)
            fig.add_trace(go.Scatter(
                name='Revenue Growth (%)',
                x=df['symbol'],
                y=df['revenue_growth'],
                yaxis='y2',
                mode='lines+markers+text',
                line=dict(color='#EF553B', width=3),
                marker=dict(size=12, color='#EF553B'),
                text=df['revenue_growth'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A"),
                textposition='top center',
                textfont=dict(
                    size=14,
                    color='white',
                    family='Arial Black'
                ),
                customdata=df[['fiscal_year']],
                hovertemplate='<b>%{x}</b><br>Year: %{customdata[0]}<br>Revenue Growth: %{y:.2f}%<extra></extra>'
            ))
            
            fig.update_layout(
                height=400,
                xaxis_title="Company",
                yaxis=dict(title="Current Ratio", side='left'),
                yaxis2=dict(title="Revenue Growth (%)", overlaying='y', side='right'),
                template="plotly_dark",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode='x unified',
                margin=dict(r=50)  # ← MARGEM DIREITA
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        # Detailed Metrics Table
        st.markdown("## 📋 Detailed Metrics")
        
        display_df = df[['symbol', 'name', 'fiscal_year', 'gross_margin', 'operating_margin', 
                         'net_margin', 'current_ratio', 'revenue_growth']].copy()
        
        display_df.columns = ['Symbol', 'Company', 'Year', 'Gross Margin %', 
                              'Operating Margin %', 'Net Margin %', 'Current Ratio', 'Revenue Growth %']
        
        # Format percentages and ratios
        for col in ['Gross Margin %', 'Operating Margin %', 'Net Margin %', 'Revenue Growth %']:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        
        display_df['Current Ratio'] = display_df['Current Ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.warning("⚠️ No data available. Please run ETL pipeline first.")