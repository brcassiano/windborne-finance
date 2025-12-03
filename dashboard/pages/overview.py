"""Overview page with key metrics and latest performance"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from database import get_db_engine


def show():
    """Display overview page with key metrics"""
    engine = get_db_engine()
    if not engine:
        st.error("❌ Cannot connect to database")
        return
    
    try:
        # Quick stats
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            df = pd.read_sql("SELECT COUNT(*) as count FROM companies", engine)
            st.metric("🏢 Companies", df['count'].iloc[0])
        
        with col2:
            df = pd.read_sql("""
                SELECT COUNT(DISTINCT fiscal_year) as count 
                FROM calculated_metrics
            """, engine)
            st.metric("📅 Years of Data", df['count'].iloc[0])
        
        with col3:
            df = pd.read_sql("""
                SELECT COUNT(*) as count 
                FROM calculated_metrics
            """, engine)
            st.metric("📊 Total Metrics", f"{df['count'].iloc[0]:,}")
        
        with col4:
            df = pd.read_sql("""
                SELECT run_date FROM etl_runs 
                ORDER BY run_date DESC LIMIT 1
            """, engine)
            if not df.empty:
                last_run = df['run_date'].iloc[0]
                st.metric("🔄 Last Update", last_run.strftime("%m/%d/%Y"))
            else:
                st.metric("🔄 Last Update", "Never")
        
        st.markdown("---")
        
        # Latest metrics comparison
        st.markdown("## 📈 Latest Performance (Most Recent Year Per Company)")
        
        query = """
            WITH latest_per_company AS (
                SELECT 
                    company_id,
                    MAX(fiscal_year) as latest_year
                FROM calculated_metrics
                GROUP BY company_id
            )
            SELECT 
                c.symbol,
                c.name,
                cm.fiscal_year,
                MAX(CASE WHEN cm.metric_name = 'gross_margin_pct' 
                    THEN cm.metric_value END) as gross_margin,
                MAX(CASE WHEN cm.metric_name = 'operating_margin_pct' 
                    THEN cm.metric_value END) as operating_margin,
                MAX(CASE WHEN cm.metric_name = 'net_margin_pct' 
                    THEN cm.metric_value END) as net_margin,
                MAX(CASE WHEN cm.metric_name = 'current_ratio' 
                    THEN cm.metric_value END) as current_ratio,
                MAX(CASE WHEN cm.metric_name = 'revenue_yoy_pct' 
                    THEN cm.metric_value END) as revenue_growth
            FROM calculated_metrics cm
            JOIN companies c ON cm.company_id = c.id
            JOIN latest_per_company lpc ON cm.company_id = lpc.company_id 
                AND cm.fiscal_year = lpc.latest_year
            GROUP BY c.symbol, c.name, cm.fiscal_year
            ORDER BY c.symbol
        """
        
        df = pd.read_sql(query, engine)
        
        if not df.empty:
            # Show year info
            year_info = df.groupby('fiscal_year')['symbol'].apply(list).to_dict()
            year_text = " | ".join([f"{year}: {', '.join(symbols)}" for year, symbols in year_info.items()])
            st.info(f"📅 **Years shown:** {year_text}")
            
            # Create comparison charts
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
                        text=df[col_name].round(1).astype(str) + '%',
                        textposition='auto',
                        textfont=dict(
                            size=14,
                            color='white',
                            family='Arial Black'
                        ),
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
                
                fig.add_trace(go.Bar(
                    name='Current Ratio',
                    x=df['symbol'],
                    y=df['current_ratio'],
                    text=df['current_ratio'].round(2).astype(str),
                    textposition='auto',
                    marker_color='#636EFA',
                    yaxis='y',
                    customdata=df[['fiscal_year']],
                    hovertemplate='<b>%{x}</b><br>Year: %{customdata[0]}<br>Current Ratio: %{y:.2f}<extra></extra>'
                ))
                
                fig.add_trace(go.Scatter(
                    name='Revenue Growth (%)',
                    x=df['symbol'],
                    y=df['revenue_growth'],
                    text=df['revenue_growth'].round(1).astype(str) + '%',
                    textposition='top center',
                    mode='lines+markers+text',
                    marker=dict(size=12, color='#EF553B'),
                    yaxis='y2',
                    line=dict(width=3),
                    customdata=df[['fiscal_year']],
                    hovertemplate='<b>%{x}</b><br>Year: %{customdata[0]}<br>Revenue Growth: %{y:.2f}%<extra></extra>'
                ))
                
                fig.update_layout(
                    height=400,
                    xaxis_title="Company",
                    yaxis=dict(title="Current Ratio", side="left"),
                    yaxis2=dict(title="Revenue Growth (%)", overlaying="y", side="right"),
                    template="plotly_dark",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Data table
            st.markdown("### 📋 Detailed Metrics")
            display_df = df.copy()
            display_df = display_df.round(2)
            display_df.columns = ['Symbol', 'Company', 'Year', 'Gross Margin %', 
                                   'Operating Margin %', 'Net Margin %', 
                                   'Current Ratio', 'Revenue Growth %']
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.warning("⚠️ No data available. Run ETL pipeline first!")
            st.info("👉 Trigger ETL via Flask API: curl -X POST http://your-ip:5000/run-etl")
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
        import traceback
        with st.expander("🐛 Debug Info"):
            st.code(traceback.format_exc())