"""System health and ETL monitoring page"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from database import get_db_engine


def show():
    """System health and ETL monitoring"""
    st.markdown("## 🔧 System Health & ETL Monitoring")
    
    engine = get_db_engine()
    if not engine:
        st.error("❌ Cannot connect to database")
        return
    
    try:
        # Latest ETL run
        df = pd.read_sql("""
            SELECT 
                run_date,
                workflow_name,
                companies_processed,
                api_calls_made,
                api_failures,
                execution_time_seconds,
                status
            FROM etl_runs
            ORDER BY run_date DESC
            LIMIT 1
        """, engine)
        
        if not df.empty:
            st.markdown("### 📊 Latest Execution Status")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                status = df['status'].iloc[0]
                st.metric(
                    "Status",
                    status,
                    delta="OK" if status == "SUCCESS" else "ERROR",
                    delta_color="normal" if status == "SUCCESS" else "inverse"
                )
            
            with col2:
                st.metric("Last Run", df['run_date'].iloc[0].strftime("%Y-%m-%d %H:%M"))
            
            with col3:
                st.metric("Duration", f"{df['execution_time_seconds'].iloc[0]}s")
            
            with col4:
                st.metric("Companies", df['companies_processed'].iloc[0])
            
            # API stats
            st.markdown("### 📞 API Statistics")
            col5, col6, col7 = st.columns(3)
            with col5:
                st.metric("API Calls Made", df['api_calls_made'].iloc[0])
            with col6:
                st.metric("API Failures", df['api_failures'].iloc[0])
            with col7:
                failure_rate = (df['api_failures'].iloc[0] / df['api_calls_made'].iloc[0] * 100) if df['api_calls_made'].iloc[0] > 0 else 0
                st.metric("Failure Rate", f"{failure_rate:.1f}%")
        else:
            st.warning("⚠️ No ETL runs found. Run ETL pipeline first!")
            st.info("👉 Trigger ETL via Flask API: curl -X POST http://your-ip:5000/run-etl")
        
        st.markdown("---")
        
        # Execution history
        st.markdown("### 📊 Execution History (Last 30 days)")
        
        df_history = pd.read_sql("""
            SELECT 
                run_date as "Date",
                status as "Status",
                companies_processed as "Companies",
                execution_time_seconds as "Duration (s)",
                api_calls_made as "API Calls",
                api_failures as "Failures"
            FROM etl_runs
            WHERE run_date > NOW() - INTERVAL '30 days'
            ORDER BY run_date DESC
        """, engine)
        
        if not df_history.empty:
            # Apply styling to status column
            def highlight_status(val):
                if val == 'SUCCESS':
                    return 'background-color: #28a745; color: white'
                elif val == 'FAILED':
                    return 'background-color: #dc3545; color: white'
                return ''
            
            styled_df = df.style.map(color_status, subset=['Status']) 
            
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            # Success rate chart
            st.markdown("### 📈 Execution Timeline")
            
            # Prepare data for chart
            chart_data = pd.read_sql("""
                SELECT 
                    run_date,
                    execution_time_seconds,
                    status
                FROM etl_runs
                WHERE run_date > NOW() - INTERVAL '30 days'
                ORDER BY run_date ASC
            """, engine)
            
            if not chart_data.empty:
                fig = go.Figure()
                
                # Color by status
                colors = chart_data['status'].map({
                    'SUCCESS': '#28a745',
                    'FAILED': '#dc3545'
                })
                
                fig.add_trace(go.Scatter(
                    x=chart_data['run_date'],
                    y=chart_data['execution_time_seconds'],
                    mode='lines+markers',
                    name='Execution Time',
                    line=dict(color='#636EFA', width=2),
                    marker=dict(
                        color=colors,
                        size=12,
                        line=dict(width=2, color='white')
                    ),
                    hovertemplate='<b>%{x}</b><br>Duration: %{y}s<extra></extra>'
                ))
                
                fig.update_layout(
                    title="ETL Execution Time Over Last 30 Days",
                    xaxis_title="Date",
                    yaxis_title="Time (seconds)",
                    template="plotly_dark",
                    height=400,
                    hovermode='x unified'
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Success rate and stats
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    success_rate = (chart_data['status'] == 'SUCCESS').mean() * 100
                    st.metric(
                        "Success Rate (30 days)",
                        f"{success_rate:.1f}%"
                    )
                
                with col2:
                    st.metric(
                        "Total Runs",
                        len(chart_data)
                    )
                
                with col3:
                    st.metric(
                        "Avg Duration",
                        f"{chart_data['execution_time_seconds'].mean():.0f}s"
                    )
        else:
            st.info("👉 No execution history yet. ETL will run daily at 8 AM BRT.")
        
    except Exception as e:
        st.error(f"❌ Error loading system health: {e}")
        import traceback
        with st.expander("🐛 Debug Info"):
            st.code(traceback.format_exc())