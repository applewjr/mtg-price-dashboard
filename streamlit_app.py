# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
import pandas as pd
import time

# Lazy connection - only connect when actually needed
def get_snowflake_session():
    """Create fresh Snowflake session only when needed"""
    try:
        session = get_active_session()
        # Test the connection with a simple query
        session.sql("SELECT 1").collect()
        return session, "Connected using active Snowflake session"
    except:
        try:
            connection_parameters = {
                "account": st.secrets["snowflake"]["account"],
                "user": st.secrets["snowflake"]["user"], 
                "password": st.secrets["snowflake"]["password"],
                "warehouse": st.secrets["snowflake"]["warehouse"],
                "database": st.secrets["snowflake"]["database"],
                "schema": st.secrets["snowflake"]["schema"]
            }
            session = Session.builder.configs(connection_parameters).create()
            # Test the new connection
            session.sql("SELECT 1").collect()
            return session, "Connected to Snowflake using credentials"
        except Exception as e:
            st.error(f"Failed to connect to Snowflake: {e}")
            st.stop()

def execute_query_with_retry(query, max_retries=2):
    """Execute query with automatic retry on connection failure"""
    for attempt in range(max_retries + 1):
        try:
            session, _ = get_snowflake_session()
            result = session.sql(query)
            return result.to_pandas()
        except Exception as e:
            if attempt == max_retries:
                st.error(f"Query failed after {max_retries + 1} attempts: {str(e)}")
                return pd.DataFrame()
            time.sleep(1)

# Cache price data for 24 hours (prices only update once per day)
@st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours, hide spinner for cached data
def get_price_after_launch():
    """Get price after launch data and cache results"""
    try:
        query = "SELECT * FROM price_after_launch"
        return execute_query_with_retry(query)
    except Exception as e:
        st.error(f"Error querying price after launch data: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=86400, show_spinner=False)  # Cache for 24 hours, hide spinner for cached data
def get_price_after_launch_foil():
    """Get foil price after launch data and cache results"""
    try:
        query = "SELECT * FROM price_after_launch_foil"
        return execute_query_with_retry(query)
    except Exception as e:
        st.error(f"Error querying foil price after launch data: {str(e)}")
        return pd.DataFrame()

# Page configuration
st.set_page_config(
    page_title="MTG Price Analysis",
    page_icon="📊",
    layout="wide"
)

# Main title
st.title("📊 MTG Price Analysis")
st.write("Average price trends of cards over the first 300 days after set release")

# Load and display the data
with st.spinner("Loading price analysis data..."):
    launch_df = get_price_after_launch()
    foil_df = get_price_after_launch_foil()

# Regular Card Prices
st.subheader("💰 Regular Card Prices")
if not launch_df.empty:
    # Prepare data for chart (pivot to get sets as separate series)
    chart_data = launch_df.pivot(index='DATE_DIFF', columns='SET_NAME', values='AVG_USD')
    
    # Sort by date_diff for proper line chart
    chart_data = chart_data.sort_index()
    
    # Create the line chart
    st.line_chart(
        chart_data, 
        x_label="Days After Launch", 
        y_label="Average USD Price",
        use_container_width=True
    )
    
    # Show summary statistics in an expandable section
    with st.expander("📈 Regular Card Analysis", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Sets Tracked:**")
            sets = launch_df['SET_NAME'].unique()
            for set_name in sorted(sets):
                set_data = launch_df[launch_df['SET_NAME'] == set_name]
                avg_price = set_data['AVG_USD'].mean()
                st.write(f"- {set_name}: ${avg_price:.2f} avg")
        
        with col2:
            st.write("**Dataset Overview:**")
            st.write(f"- Total data points: {len(launch_df):,}")
            st.write(f"- Tracking period: 1-300 days after release")
            st.write(f"- Card rarities: Mythic & Rare only")
            st.write(f"- Sets analyzed: {len(sets)} expansion sets")
            
            # Additional insights
            st.write("**Key Insights:**")
            overall_avg = launch_df['AVG_USD'].mean()
            st.write(f"- Overall average price: ${overall_avg:.2f}")
            
            # Find the set with highest average price
            set_averages = launch_df.groupby('SET_NAME')['AVG_USD'].mean()
            highest_set = set_averages.idxmax()
            highest_price = set_averages.max()
            st.write(f"- Highest average set: {highest_set} (${highest_price:.2f})")

else:
    st.error("Unable to load regular price analysis data. Please try refreshing the page.")

# Foil Card Prices
st.subheader("✨ Foil Card Prices")
if not foil_df.empty:
    # Prepare data for chart (pivot to get sets as separate series)
    foil_chart_data = foil_df.pivot(index='DATE_DIFF', columns='SET_NAME', values='AVG_USD_FOIL')
    
    # Sort by date_diff for proper line chart
    foil_chart_data = foil_chart_data.sort_index()
    
    # Create the line chart
    st.line_chart(
        foil_chart_data, 
        x_label="Days After Launch", 
        y_label="Average USD Foil Price",
        use_container_width=True
    )
    
    # Show summary statistics in an expandable section
    with st.expander("✨ Foil Card Analysis", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Sets Tracked:**")
            foil_sets = foil_df['SET_NAME'].unique()
            for set_name in sorted(foil_sets):
                set_data = foil_df[foil_df['SET_NAME'] == set_name]
                avg_price = set_data['AVG_USD_FOIL'].mean()
                st.write(f"- {set_name}: ${avg_price:.2f} avg")
        
        with col2:
            st.write("**Dataset Overview:**")
            st.write(f"- Total data points: {len(foil_df):,}")
            st.write(f"- Tracking period: 1-300 days after release")
            st.write(f"- Card type: All expansion cards")
            st.write(f"- Sets analyzed: {len(foil_sets)} expansion sets")
            
            # Additional insights
            st.write("**Key Insights:**")
            overall_foil_avg = foil_df['AVG_USD_FOIL'].mean()
            st.write(f"- Overall average foil price: ${overall_foil_avg:.2f}")
            
            # Find the set with highest average foil price
            foil_set_averages = foil_df.groupby('SET_NAME')['AVG_USD_FOIL'].mean()
            highest_foil_set = foil_set_averages.idxmax()
            highest_foil_price = foil_set_averages.max()
            st.write(f"- Highest average set: {highest_foil_set} (${highest_foil_price:.2f})")

else:
    st.error("Unable to load foil price analysis data. Please try refreshing the page.")
    st.info("If the problem persists, there may be an issue with the data source.")

# Footer
st.markdown("---")
st.caption("💡 Data updates once daily and is cached for 24 hours to optimize performance.")