"""All metrics page with data table and export"""
import streamlit as st
import pandas as pd
from datetime import datetime
from database import get_db_engine


def show():
    """Show all metrics in table format"""
    st.markdown("## 📊 All Financial Metrics")
    
    engine = get_db_engine()
    if not engine:
        st.error("❌ Cannot connect to database")
        return
    
    try:
        # Filters
        col1, col2 = st.columns(2)
        
        with col1:
            companies_df = pd.read_sql("SELECT DISTINCT symbol FROM companies ORDER BY symbol", engine)
            selected_companies = st.multiselect(
                "Companies",
                companies_df['symbol'].tolist(),
                default=companies_df['symbol'].tolist()
            )
        
        with col2:
            years_df = pd.read_sql("""
                SELECT DISTINCT fiscal_year FROM calculated_metrics 
                ORDER BY fiscal_year DESC
            """, engine)
            selected_years = st.multiselect(
                "Years",
                years_df['fiscal_year'].tolist(),
                default=years_df['fiscal_year'].tolist()[:3] if len(years_df) >= 3 else years_df['fiscal_year'].tolist()
            )
        
        if not selected_companies or not selected_years:
            st.info("ℹ️ Select filters above")
            return
        
        companies_str = "','".join(selected_companies)
        years_str = ",".join(map(str, selected_years))
        
        query = f"""
            SELECT 
                c.symbol,
                cm.fiscal_year,
                cm.metric_name,
                cm.metric_value,
                cm.metric_category
            FROM calculated_metrics cm
            JOIN companies c ON cm.company_id = c.id
            WHERE c.symbol IN ('{companies_str}')
            AND cm.fiscal_year IN ({years_str})
            ORDER BY c.symbol, cm.fiscal_year DESC, cm.metric_category, cm.metric_name
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            st.warning("⚠️ No data available")
            return
        
        st.success(f"✅ Found {len(df)} metrics")
        
        # Pivot for better view
        pivot_df = df.pivot_table(
            index=['symbol', 'fiscal_year'],
            columns='metric_name',
            values='metric_value',
            aggfunc='first'
        ).reset_index()
        
        # Format numbers
        numeric_cols = pivot_df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_cols:
            if col not in ['symbol', 'fiscal_year']:
                pivot_df[col] = pivot_df[col].round(2)
        
        st.dataframe(pivot_df, use_container_width=True, height=600)
        
        # Download button
        csv = pivot_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"windborne_metrics_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"❌ Error: {e}")