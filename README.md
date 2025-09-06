# MTG Price Dashboard

*Interactive Streamlit analytics dashboard for Magic: The Gathering card price trends*

**Live Dashboard**: <a href="https://mtg-price-dashboard.streamlit.app/" target="_blank" rel="noopener noreferrer">mtg-price-dashboard.streamlit.app</a>

## Overview

This Streamlit application visualizes Magic: The Gathering card price trends across different sets, normalized to release dates. The dashboard connects directly to a Snowflake data warehouse and displays price patterns for regular and foil cards over time.

**Key Features:**
- Set-normalized price analysis (45 days before to 300 days after release)
- Interactive line charts comparing multiple card sets
- Cost-optimized design with 24-hour data caching
- Real-time connection to Snowflake via Snowpark

## Full Project Architecture

This dashboard is the frontend component of a comprehensive data engineering platform. For complete details on the AWS data pipeline, cost optimization strategies, and backend architecture:

### <a href="https://github.com/applewjr/mtg-prices" target="_blank" rel="noopener noreferrer">Complete MTG Price Analytics Platform</a>

The main repository contains the full system: AWS Lambda/EMR pipeline, Snowflake integration, Apache Iceberg tables, and multi-platform distribution serving 90K+ daily records at <$1/month operational cost.