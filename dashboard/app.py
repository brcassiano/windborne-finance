import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Page config
st.set_page_config(
    page_title="WindBorne Finance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main > div {
        padding-top: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
    }
    </style>
""", unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'windborne_finance'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

@st.cache_data(ttl=3600)
def load_metrics_data():
    """Load all calculated metrics"""
    conn = get_db_connection()
    query = """
        SELECT 
            c.symbol,
            c.name,
            cm.fiscal_year,
            cm.metric_name,
            cm.metric_value,
            cm.metric_category
        FROM calculated_metrics cm
        JOIN companies c ON cm.company_id = c.id
        ORDER BY c.symbol, cm.fiscal_year DESC
    """
    df = pd.read_sql(query, conn)
    return df

@st.cache_data(ttl=3600)
def load_etl_logs():
    """Load ETL execution logs"""
    conn = get_db_connection()
    query = """
        SELECT * FROM v_etl_health
        ORDER BY run_date DESC
        LIMIT 30
    """
    df = pd.read_sql(query, conn)
    return df

def main():
    # Sidebar
    st.sidebar.title("📊 WindBorne Finance")
    st.sidebar.markdown("---")
    
    page = st.sidebar.radio(
        "Navigation",
        ["📈 Financial Metrics", "🔍 ETL Monitoring"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info(
        "Automated financial analysis for **TEL**, **ST**, and **DD**.\n\n"
        "Data source: Alpha Vantage API"
    )
    
    if page == "📈 Financial Metrics":
        show_financial_metrics()
    else:
        show_etl_monitoring()

def show_financial_metrics():
    st.title("📈 Financial Performance Dashboard")
    st.markdown("Comprehensive analysis of key financial metrics")
    
    # Load data
    df = load_metrics_data()
    
    if df.empty:
        st.warning("⚠️ No data available. Run the ETL pipeline first.")
        st.code("# In n8n, trigger the 'WindBorne ETL' workflow")
        return
    
    # Filters
    col1, col2 = st.columns(2)
    
    with col1:
        companies = st.multiselect(
            "Select Companies",
            options=sorted(df['symbol'].unique()),
            default=sorted(df['symbol'].unique())
        )
    
    with col2:
        years = st.multiselect(
            "Select Years",
            options=sorted(df['fiscal_year'].unique(), reverse=True),
            default=sorted(df['fiscal_year'].unique(), reverse=True)[:3]
        )
    
    if not companies or not years:
        st.warning("Please select at least one company and year")
        return
    
    # Filter data
    filtered_df = df[
        (df['symbol'].isin(companies)) & 
        (df['fiscal_year'].isin(years))
    ]
    
    # KPIs
    st.markdown("---")
    st.subheader("📊 Key Performance Indicators")
    
    latest_year = max(years)
    
    cols = st.columns(len(companies))
    for idx, company in enumerate(companies):
        with cols[idx]:
            st.markdown(f"### {company}")
            company_data = filtered_df[
                (filtered_df['symbol'] == company) & 
                (filtered_df['fiscal_year'] == latest_year)
            ]
            
            # Display key metrics
            metrics_to_show = [
                ('gross_margin_pct', 'Gross Margin', '%'),
                ('operating_margin_pct', 'Operating Margin', '%'),
                ('net_margin_pct', 'Net Margin', '%'),
                ('current_ratio', 'Current Ratio', ''),
                ('revenue_yoy_pct', 'Revenue Growth', '%')
            ]
            
            for metric_name, label, suffix in metrics_to_show:
                metric_row = company_data[company_data['metric_name'] == metric_name]
                if not metric_row.empty:
                    value = metric_row.iloc[0]['metric_value']
                    if value is not None:
                        st.metric(
                            label=label,
                            value=f"{value:.2f}{suffix}"
                        )
    
    # Profitability Trends
    st.markdown("---")
    st.subheader("💰 Profitability Trends")
    
    profit_metrics = filtered_df[
        filtered_df['metric_category'] == 'PROFITABILITY'
    ]
    
    fig = go.Figure()
    colors = {'gross_margin_pct': '#1f77b4', 'operating_margin_pct': '#ff7f0e', 'net_margin_pct': '#2ca02c'}
    
    for company in companies:
        for metric in ['gross_margin_pct', 'operating_margin_pct', 'net_margin_pct']:
            company_metric = profit_metrics[
                (profit_metrics['symbol'] == company) &
                (profit_metrics['metric_name'] == metric)
            ].sort_values('fiscal_year')
            
            if not company_metric.empty:
                fig.add_trace(go.Scatter(
                    x=company_metric['fiscal_year'],
                    y=company_metric['metric_value'],
                    name=f"{company} - {metric.replace('_', ' ').title()}",
                    mode='lines+markers',
                    line=dict(width=2)
                ))
    
    fig.update_layout(
        title="Profitability Margins Over Time",
        xaxis_title="Fiscal Year",
        yaxis_title="Margin (%)",
        hovermode='x unified',
        height=500,
        template='plotly_white'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Liquidity Comparison
    st.markdown("---")
    st.subheader("💧 Liquidity Ratios")
    
    col1, col2 = st.columns(2)
    
    with col1:
        liquidity = filtered_df[
            (filtered_df['metric_category'] == 'LIQUIDITY') &
            (filtered_df['fiscal_year'] == latest_year) &
            (filtered_df['metric_name'] == 'current_ratio')
        ]
        
        if not liquidity.empty:
            fig2 = px.bar(
                liquidity,
                x='symbol',
                y='metric_value',
                title=f"Current Ratio - {latest_year}",
                labels={'metric_value': 'Ratio', 'symbol': 'Company'},
                color='symbol',
                text='metric_value'
            )
            fig2.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            fig2.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig2, use_container_width=True)
    
    with col2:
        quick_ratio = filtered_df[
            (filtered_df['metric_category'] == 'LIQUIDITY') &
            (filtered_df['fiscal_year'] == latest_year) &
            (filtered_df['metric_name'] == 'quick_ratio')
        ]
        
        if not quick_ratio.empty:
            fig3 = px.bar(
                quick_ratio,
                x='symbol',
                y='metric_value',
                title=f"Quick Ratio - {latest_year}",
                labels={'metric_value': 'Ratio', 'symbol': 'Company'},
                color='symbol',
                text='metric_value'
            )
            fig3.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            fig3.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig3, use_container_width=True)
    
    # Growth Metrics
    st.markdown("---")
    st.subheader("📈 Growth Analysis")
    
    growth = filtered_df[filtered_df['metric_category'] == 'GROWTH']
    
    if not growth.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            revenue_growth = growth[growth['metric_name'] == 'revenue_yoy_pct']
            if not revenue_growth.empty:
                fig4 = px.bar(
                    revenue_growth,
                    x='fiscal_year',
                    y='metric_value',
                    color='symbol',
                    barmode='group',
                    title="Revenue YoY Growth %",
                    labels={'metric_value': 'Growth %', 'fiscal_year': 'Year'}
                )
                fig4.update_layout(height=400)
                st.plotly_chart(fig4, use_container_width=True)
        
        with col2:
            ni_growth = growth[growth['metric_name'] == 'net_income_yoy_pct']
            if not ni_growth.empty:
                fig5 = px.bar(
                    ni_growth,
                    x='fiscal_year',
                    y='metric_value',
                    color='symbol',
                    barmode='group',
                    title="Net Income YoY Growth %",
                    labels={'metric_value': 'Growth %', 'fiscal_year': 'Year'}
                )
                fig5.update_layout(height=400)
                st.plotly_chart(fig5, use_container_width=True)
    
    # Raw Data Table
    st.markdown("---")
    with st.expander("📋 View Raw Metrics Data"):
        pivot = filtered_df.pivot_table(
            index=['symbol', 'fiscal_year'],
            columns='metric_name',
            values='metric_value',
            aggfunc='first'
        ).reset_index()
        st.dataframe(pivot, use_container_width=True)

def show_etl_monitoring():
    st.title("🔍 ETL Pipeline Monitoring")
    st.markdown("Track ETL executions and data quality")
    
    # Load ETL logs
    etl_logs = load_etl_logs()
    
    if etl_logs.empty:
        st.info("No ETL runs logged yet")
        return
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        success_count = len(etl_logs[etl_logs['status'] == 'SUCCESS'])
        success_rate = (success_count / len(etl_logs)) * 100
        st.metric(
            "Success Rate (30 runs)",
            f"{success_rate:.1f}%",
            delta=None,
            delta_color="normal"
        )
    
    with col2:
        avg_time = etl_logs['execution_time_seconds'].mean()
        st.metric("Avg Execution Time", f"{avg_time:.0f}s")
    
    with col3:
        total_failures = etl_logs['api_failures'].sum()
        st.metric("Total API Failures", int(total_failures))
    
    with col4:
        if not etl_logs.empty:
            last_run = pd.to_datetime(etl_logs.iloc[0]['run_date'])
            st.metric("Last Run", last_run.strftime("%Y-%m-%d %H:%M"))
    
    # Execution Time Trend
    st.markdown("---")
    st.subheader("⏱️ Execution Time Trend")
    
    fig_time = px.line(
        etl_logs,
        x='run_date',
        y='execution_time_seconds',
        title="ETL Execution Time Over Time",
        markers=True,
        labels={'execution_time_seconds': 'Time (seconds)', 'run_date': 'Run Date'}
    )
    fig_time.update_layout(height=400)
    st.plotly_chart(fig_time, use_container_width=True)
    
    # Status Distribution
    col1, col2 = st.columns(2)
    
    with col1:
        status_count = etl_logs['status'].value_counts()
        fig_status = px.pie(
            values=status_count.values,
            names=status_count.index,
            title="ETL Status Distribution"
        )
        fig_status.update_layout(height=400)
        st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        fig_failures = px.bar(
            etl_logs.head(10),
            x='run_date',
            y='api_failures',
            title="Recent API Failures",
            labels={'api_failures': 'Failures', 'run_date': 'Run Date'}
        )
        fig_failures.update_layout(height=400)
        st.plotly_chart(fig_failures, use_container_width=True)
    
    # Full History Table
    st.markdown("---")
    st.subheader("📊 ETL Execution History")
    
    display_cols = [
        'status_icon', 'run_date', 'workflow_name',
        'companies_processed', 'api_calls_made',
        'failure_rate_pct', 'execution_time_seconds'
    ]
    
    st.dataframe(
        etl_logs[display_cols],
        use_container_width=True,
        hide_index=True
    )
    
    # Footer
    st.markdown("---")
    st.caption("💡 Data updated every hour | Powered by Alpha Vantage API")

if __name__ == "__main__":
    main()
