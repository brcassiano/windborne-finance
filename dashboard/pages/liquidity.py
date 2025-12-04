"""Liquidity analysis page"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from database import get_db_engine


def show():
    """Display liquidity analysis"""
    st.markdown("## 💧 Liquidity Analysis")
    
    engine = get_db_engine()
    if not engine:
        st.error("❌ Cannot connect to database")
        return
    
    # Latest Liquidity Ratios (Horizontal com wrap automático)
    st.markdown("## 📊 Latest Liquidity Ratios")
    
    query_latest = """
        SELECT 
            c.symbol,
            c.name,
            cm.fiscal_year,
            MAX(CASE WHEN cm.metric_name = 'current_ratio' THEN cm.metric_value END) as current_ratio,
            MAX(CASE WHEN cm.metric_name = 'quick_ratio' THEN cm.metric_value END) as quick_ratio,
            MAX(CASE WHEN cm.metric_name = 'cash_ratio' THEN cm.metric_value END) as cash_ratio
        FROM companies c
        JOIN calculated_metrics cm ON c.id = cm.company_id
        WHERE cm.fiscal_year = (
            SELECT MAX(fiscal_year) 
            FROM calculated_metrics 
            WHERE company_id = c.id
        )
        AND cm.metric_name IN ('current_ratio', 'quick_ratio', 'cash_ratio')
        GROUP BY c.symbol, c.name, cm.fiscal_year
        ORDER BY c.symbol
    """
    
    df_latest = pd.read_sql(query_latest, engine)
    
    if not df_latest.empty:
        # Layout responsivo: 3 empresas por linha
        companies_per_row = 3
        rows_needed = (len(df_latest) + companies_per_row - 1) // companies_per_row
        
        idx = 0
        for row_num in range(rows_needed):
            cols = st.columns(companies_per_row)
            
            for col_num, col in enumerate(cols):
                if idx < len(df_latest):
                    row_data = df_latest.iloc[idx]
                    
                    with col:
                        st.markdown(f"### {row_data['symbol']}")
                        st.caption(row_data['name'])
                        
                        current = row_data['current_ratio']
                        quick = row_data['quick_ratio']
                        cash = row_data['cash_ratio']
                        
                        st.metric(
                            "Current",
                            f"{current:.2f}" if pd.notna(current) else "N/A"
                        )
                        st.metric(
                            "Quick",
                            f"{quick:.2f}" if pd.notna(quick) else "N/A",
                            delta=f"↑ {quick:.2f}" if pd.notna(quick) and quick > 1 else None
                        )
                        st.metric(
                            "Cash",
                            f"{cash:.2f}" if pd.notna(cash) else "N/A"
                        )
                    
                    idx += 1
    else:
        st.warning("⚠️ No liquidity data available")
    
    st.markdown("---")
    
    # Liquidity Ratios Comparison (Bar Chart)
    st.markdown("## 📊 Liquidity Ratios Comparison")
    
    if not df_latest.empty:
        fig = go.Figure()
        
        # Current Ratio
        fig.add_trace(go.Bar(
            name='Current Ratio',
            x=df_latest['symbol'],
            y=df_latest['current_ratio'].fillna(0),
            marker_color='#636EFA',
            text=df_latest['current_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"),
            textposition='auto',
            textfont=dict(size=14, color='white', family='Arial Black')
        ))
        
        # Quick Ratio
        fig.add_trace(go.Bar(
            name='Quick Ratio',
            x=df_latest['symbol'],
            y=df_latest['quick_ratio'].fillna(0),
            marker_color='#00CC96',
            text=df_latest['quick_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"),
            textposition='auto',
            textfont=dict(size=14, color='white', family='Arial Black')
        ))
        
        # Cash Ratio
        fig.add_trace(go.Bar(
            name='Cash Ratio',
            x=df_latest['symbol'],
            y=df_latest['cash_ratio'].fillna(0),
            marker_color='#FFA15A',
            text=df_latest['cash_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"),
            textposition='auto',
            textfont=dict(size=14, color='white', family='Arial Black')
        ))
        
        # Linha de referência em 1.0
        fig.add_hline(
            y=1.0,
            line_dash="dash",
            line_color="gray",
            annotation_text="Minimum Safe Level (1.0)",
            annotation_position="right"
        )
        
        fig.update_layout(
            barmode='group',
            height=500,
            xaxis_title="Company",
            yaxis_title="Ratio",
            template="plotly_dark",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Interpretação
        with st.expander("📖 How to interpret liquidity ratios"):
            st.markdown("""
            **Current Ratio = Current Assets / Current Liabilities**
            - Above 1.5: Healthy liquidity
            - 1.0 - 1.5: Adequate liquidity
            - Below 1.0: Potential liquidity issues
            
            **Quick Ratio = (Current Assets - Inventory) / Current Liabilities**
            - Above 1.0: Good short-term liquidity
            - Below 1.0: May struggle with immediate obligations
            
            **Cash Ratio = Cash / Current Liabilities**
            - Above 0.5: Strong cash position
            - 0.2 - 0.5: Adequate cash reserves
            - Below 0.2: Limited cash cushion
            """)
    
    st.markdown("---")
    
    # Historical Liquidity Trend
    st.markdown("## 📈 Historical Liquidity Trend")
    
    query_history = """
        SELECT 
            c.symbol,
            cm.fiscal_year,
            MAX(CASE WHEN cm.metric_name = 'current_ratio' THEN cm.metric_value END) as current_ratio,
            MAX(CASE WHEN cm.metric_name = 'quick_ratio' THEN cm.metric_value END) as quick_ratio
        FROM companies c
        JOIN calculated_metrics cm ON c.id = cm.company_id
        WHERE cm.metric_name IN ('current_ratio', 'quick_ratio')
        GROUP BY c.symbol, cm.fiscal_year
        ORDER BY c.symbol, cm.fiscal_year
    """
    
    df_history = pd.read_sql(query_history, engine)
    
    if not df_history.empty:
        # Current Ratio Timeline
        fig_current = go.Figure()
        
        for symbol in df_history['symbol'].unique():
            df_symbol = df_history[df_history['symbol'] == symbol]
            
            fig_current.add_trace(go.Scatter(
                x=df_symbol['fiscal_year'],
                y=df_symbol['current_ratio'],
                mode='lines+markers',
                name=symbol,
                line=dict(width=3),
                marker=dict(size=10),
                hovertemplate='<b>%{fullData.name}</b><br>Year: %{x}<br>Current Ratio: %{y:.2f}<extra></extra>'
            ))
        
        fig_current.add_hline(
            y=1.0,
            line_dash="dash",
            line_color="gray",
            annotation_text="Safe Level"
        )
        
        fig_current.update_layout(
            title="Current Ratio Over Time",
            xaxis_title="Fiscal Year",
            yaxis_title="Current Ratio",
            template="plotly_dark",
            height=400,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig_current, use_container_width=True)
        
        # Quick Ratio Timeline
        fig_quick = go.Figure()
        
        for symbol in df_history['symbol'].unique():
            df_symbol = df_history[df_history['symbol'] == symbol]
            
            fig_quick.add_trace(go.Scatter(
                x=df_symbol['fiscal_year'],
                y=df_symbol['quick_ratio'],
                mode='lines+markers',
                name=symbol,
                line=dict(width=3),
                marker=dict(size=10),
                hovertemplate='<b>%{fullData.name}</b><br>Year: %{x}<br>Quick Ratio: %{y:.2f}<extra></extra>'
            ))
        
        fig_quick.add_hline(
            y=1.0,
            line_dash="dash",
            line_color="gray",
            annotation_text="Safe Level"
        )
        
        fig_quick.update_layout(
            title="Quick Ratio Over Time",
            xaxis_title="Fiscal Year",
            yaxis_title="Quick Ratio",
            template="plotly_dark",
            height=400,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig_quick, use_container_width=True)
    else:
        st.info("👉 No historical data available yet")
    
    st.markdown("---")
    
    # Detailed Metrics Table
    st.markdown("## 📋 Detailed Liquidity Metrics")
    
    if not df_latest.empty:
        # Preparar dados para tabela
        table_data = df_latest[['symbol', 'name', 'fiscal_year', 'current_ratio', 'quick_ratio', 'cash_ratio']].copy()
        table_data.columns = ['Symbol', 'Company', 'Year', 'Current Ratio', 'Quick Ratio', 'Cash Ratio']
        
        # Formatar valores
        for col in ['Current Ratio', 'Quick Ratio', 'Cash Ratio']:
            table_data[col] = table_data[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        
        st.dataframe(table_data, use_container_width=True, hide_index=True)