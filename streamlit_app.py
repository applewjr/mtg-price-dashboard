# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
import pandas as pd
import time
from typing import Dict, List, Optional, Any

# =============================================================================
# CONFIGURATION SECTION - CUSTOMIZE THIS FOR EACH DASHBOARD
# =============================================================================

DASHBOARD_CONFIG = {
    "page_title": "MTG Price Analysis",
    "page_icon": "📊",
    "main_title": "📊 MTG Price Analysis",
    "main_description": "Average price trends of cards over the first 300 days after set release",
    "cache_hours": 24  # How long to cache data (in hours)
}

# Define your charts here - each becomes its own page
CHART_CONFIGS = [
    {
        "page_name": "Card Prices",
        "page_icon": "💰",
        "title": "Regular Card Prices",
        "description": "Average price trends for cards across different sets",
        "table_name": "price_after_launch",
        "x_column": "DATE_DIFF",
        "y_column": "AVG_USD", 
        "group_by_column": "SET_NAME",
        "x_label": "Days After Launch",
        "y_label": "Average USD Price",
        "expanded_title": "Regular Card Analysis",
        "analysis_columns": {
            "Total data points": "count_rows",
            "Tracking period": "1-300 days after release", 
            "Card rarities": "All expansion cards",
            "Sets analyzed": "count_unique_groups"
        },
        "insights": {
            "Overall average price": ("mean", "AVG_USD"),
            "Highest average set": ("max_group_avg", "AVG_USD", "SET_NAME")
        }
    },
    {
        "page_name": "Card Prices - Mythic/Rare",
        "page_icon": "💰",
        "title": "Regular Card Prices",
        "description": "Average price trends for mythic and rare cards across different sets",
        "table_name": "price_after_launch_rare_mythic",
        "x_column": "DATE_DIFF",
        "y_column": "AVG_USD", 
        "group_by_column": "SET_NAME",
        "x_label": "Days After Launch",
        "y_label": "Average USD Price",
        "expanded_title": "Regular Card Analysis",
        "analysis_columns": {
            "Total data points": "count_rows",
            "Tracking period": "1-300 days after release", 
            "Card rarities": "Mythic & Rare only",
            "Sets analyzed": "count_unique_groups"
        },
        "insights": {
            "Overall average price": ("mean", "AVG_USD"),
            "Highest average set": ("max_group_avg", "AVG_USD", "SET_NAME")
        }
    },
    {
        "page_name": "Foil Card Prices",
        "page_icon": "✨",
        "title": "Foil Card Prices",
        "description": "Average price trends for foil cards across different sets",
        "table_name": "price_after_launch_foil",
        "x_column": "DATE_DIFF",
        "y_column": "AVG_USD",
        "group_by_column": "SET_NAME",
        "x_label": "Days After Launch", 
        "y_label": "Average USD Foil Price",
        "expanded_title": "Foil Card Analysis",
        "analysis_columns": {
            "Total data points": "count_rows",
            "Tracking period": "1-300 days after release",
            "Card type": "All expansion cards", 
            "Sets analyzed": "count_unique_groups"
        },
        "insights": {
            "Overall average foil price": ("mean", "AVG_USD"),
            "Highest average set": ("max_group_avg", "AVG_USD", "SET_NAME")
        }
    },
    {
        "page_name": "Foil Card Prices - Mythic/Rare",
        "page_icon": "✨",
        "title": "Foil Card Prices",
        "description": "Average price trends for mythic and rare foil cards across different sets",
        "table_name": "price_after_launch_foil_rare_mythic",
        "x_column": "DATE_DIFF",
        "y_column": "AVG_USD",
        "group_by_column": "SET_NAME",
        "x_label": "Days After Launch", 
        "y_label": "Average USD Foil Price",
        "expanded_title": "Foil Card Analysis",
        "analysis_columns": {
            "Total data points": "count_rows",
            "Tracking period": "1-300 days after release",
            "Card type": "All expansion cards", 
            "Sets analyzed": "count_unique_groups"
        },
        "insights": {
            "Overall average foil price": ("mean", "AVG_USD"),
            "Highest average set": ("max_group_avg", "AVG_USD", "SET_NAME")
        }
    },
    {
        "page_name": "Pre-Release",
        "page_icon": "💰",
        "title": "Regular Card Prices",
        "description": "Average price trends for cards across different sets",
        "table_name": "price_before_launch",
        "x_column": "DATE_DIFF",
        "y_column": "AVG_USD", 
        "group_by_column": "SET_NAME",
        "x_label": "Days Before Launch",
        "y_label": "Average USD Price",
        "expanded_title": "Regular Card Analysis",
        "analysis_columns": {
            "Total data points": "count_rows",
            "Tracking period": "45 days before release", 
            "Card rarities": "All expansion cards",
            "Sets analyzed": "count_unique_groups"
        },
        "insights": {
            "Overall average price": ("mean", "AVG_USD"),
            "Highest average set": ("max_group_avg", "AVG_USD", "SET_NAME")
        }
    },
    {
        "page_name": "Pre-Release Foil",
        "page_icon": "✨",
        "title": "Foil Card Prices",
        "description": "Average price trends for foil cards across different sets",
        "table_name": "price_before_launch_foil",
        "x_column": "DATE_DIFF",
        "y_column": "AVG_USD",
        "group_by_column": "SET_NAME",
        "x_label": "Days Before Launch", 
        "y_label": "Average USD Foil Price",
        "expanded_title": "Foil Card Analysis",
        "analysis_columns": {
            "Total data points": "count_rows",
            "Tracking period": "45 days before release",
            "Card type": "All expansion cards", 
            "Sets analyzed": "count_unique_groups"
        },
        "insights": {
            "Overall average foil price": ("mean", "AVG_USD"),
            "Highest average set": ("max_group_avg", "AVG_USD", "SET_NAME")
        }
    }
]

# =============================================================================
# CORE FUNCTIONALITY - GENERALLY SHOULDN'T NEED TO MODIFY
# =============================================================================

def get_snowflake_session():
    """Create fresh Snowflake session only when needed"""
    try:
        session = get_active_session()
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
            session.sql("SELECT 1").collect()
            return session, "Connected to Snowflake using credentials"
        except Exception as e:
            st.error(f"Failed to connect to Snowflake: {e}")
            st.stop()

def execute_query_with_retry(query: str, max_retries: int = 2) -> pd.DataFrame:
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

@st.cache_data(ttl=24*3600, show_spinner=False)  # Cache for 24 hours  
def preload_all_data() -> Dict[str, pd.DataFrame]:
    """
    Preload ALL chart data with clean UI - messages only show during actual loading.
    """
    all_data = {}
    table_names = set(config["table_name"] for config in CHART_CONFIGS)
    
    try:
        session, _ = get_snowflake_session()
        
        for table_name in sorted(table_names):
            query = f"SELECT * FROM {table_name}"
            result = session.sql(query)
            all_data[table_name] = result.to_pandas()
        
        return all_data
        
    except Exception as e:
        st.error(f"Failed to preload data: {str(e)}")
        return {}
    
def get_cached_data(table_name: str, cache_hours: int = 24) -> pd.DataFrame:
    """Get data from the preloaded cache"""
    all_data = preload_all_data()
    return all_data.get(table_name, pd.DataFrame())

def calculate_insight(df: pd.DataFrame, insight_type: str, *args) -> str:
    """Calculate various insights from the dataframe"""
    if insight_type == "mean":
        column = args[0]
        value = df[column].mean()
        return f"${value:.2f}"
    
    elif insight_type == "max_group_avg":
        value_col, group_col = args[0], args[1]
        group_averages = df.groupby(group_col)[value_col].mean()
        max_group = group_averages.idxmax()
        max_value = group_averages.max()
        return f"{max_group} (${max_value:.2f})"
    
    return "N/A"

def calculate_analysis_stat(df: pd.DataFrame, stat_type: str, group_col: Optional[str] = None) -> str:
    """Calculate analysis statistics"""
    if stat_type == "count_rows":
        return f"{len(df):,}"
    elif stat_type == "count_unique_groups" and group_col:
        return f"{df[group_col].nunique()} expansion sets"
    else:
        return stat_type  # Return as-is if it's a static string

def render_chart_page(config: Dict[str, Any]):
    """Render a complete chart page based on configuration"""
    st.title(config["title"])
    st.write(config["description"])
    
    # Get data from preloaded cache (no spinner needed since data is already loaded)
    df = get_cached_data(config["table_name"], DASHBOARD_CONFIG["cache_hours"])
    
    if df.empty:
        st.error(f"❌ No data available for {config['table_name']}. Data may have failed to load during startup.")
        st.info("💡 Try refreshing the page to reload all data.")
        return
        
    # Prepare data for chart
    chart_data = df.pivot(
        index=config["x_column"], 
        columns=config["group_by_column"], 
        values=config["y_column"]
    ).sort_index()
    
    # Create the line chart
    st.line_chart(
        chart_data,
        x_label=config["x_label"],
        y_label=config["y_label"], 
        use_container_width=True
    )
    
    # Show analysis in expandable section
    with st.expander(config["expanded_title"], expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Sets Tracked:**")
            groups = df[config["group_by_column"]].unique()
            for group in sorted(groups):
                group_data = df[df[config["group_by_column"]] == group]
                avg_price = group_data[config["y_column"]].mean()
                st.write(f"- {group}: ${avg_price:.2f} avg")
        
        with col2:
            st.write("**Dataset Overview:**")
            # Dynamic analysis stats
            for label, stat_config in config["analysis_columns"].items():
                if isinstance(stat_config, str):
                    if stat_config in ["count_rows", "count_unique_groups"]:
                        value = calculate_analysis_stat(df, stat_config, config.get("group_by_column"))
                    else:
                        value = stat_config
                else:
                    value = str(stat_config)
                st.write(f"- {label}: {value}")
            
            # Dynamic insights
            st.write("**Key Insights:**")
            for label, insight_config in config["insights"].items():
                insight_value = calculate_insight(df, *insight_config)
                st.write(f"- {label}: {insight_value}")

def render_home_page():
    """Render the home/overview page"""
    st.title(DASHBOARD_CONFIG["main_title"])
    st.write(DASHBOARD_CONFIG["main_description"])
    
    # Show home page content
    st.markdown(DASHBOARD_CONFIG["home_page_content"])
    
    # Optional: Show quick stats or overview
    st.subheader("📋 Available Analysis Pages")
    
    for config in CHART_CONFIGS:
        with st.container():
            col1, col2 = st.columns([1, 10])
            with col1:
                st.write(config["page_icon"])
            with col2:
                st.write(f"**{config['page_name']}** - {config['description']}")

# =============================================================================
# MAIN APPLICATION WITH PAGE ROUTING
# =============================================================================

def main():
    # Page configuration
    st.set_page_config(
        page_title=DASHBOARD_CONFIG["page_title"],
        page_icon=DASHBOARD_CONFIG["page_icon"],
        layout="wide"
    )
    
    # Sidebar for navigation
    st.sidebar.title("📊 Navigation")
    
    # Create page options - just the chart pages
    page_options = [f"{config['page_icon']} {config['page_name']}" for config in CHART_CONFIGS]
    
    # Page selection using radio buttons
    selected_page = st.sidebar.radio(
        "Choose a page:",
        page_options,
        index=0
    )
    
    # Add some sidebar info
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ Info")
    cache_hours = DASHBOARD_CONFIG["cache_hours"]
    st.sidebar.caption(f"Data cached for {cache_hours} hours")
    st.sidebar.caption("All data preloaded for optimal Snowflake billing")
    
    # Add cache status to sidebar
    all_data = preload_all_data()  # This triggers the preload on first visit
    loaded_tables = sum(1 for df in all_data.values() if not df.empty)
    total_tables = len(set(config["table_name"] for config in CHART_CONFIGS))
    
    if loaded_tables > 0:
        st.sidebar.success(f"✅ {loaded_tables}/{total_tables} datasets loaded")
        total_rows = sum(len(df) for df in all_data.values())
        st.sidebar.caption(f"📊 {total_rows:,} total rows cached")
    else:
        st.sidebar.warning("⚠️ No data loaded yet")
    
    # Route to appropriate chart page
    for config in CHART_CONFIGS:
        if selected_page == f"{config['page_icon']} {config['page_name']}":
            render_chart_page(config)
            break
    
    # Footer (appears on all pages)
    st.markdown("---")
    st.caption(f"💡 All data loads together at startup to optimize Snowflake billing (60-second minimum charge).")
    st.caption(f"📅 Data updates once daily and is cached for {DASHBOARD_CONFIG['cache_hours']} hours.")

if __name__ == "__main__":
    main()