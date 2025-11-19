"""
IoT Sensor Dashboard
Real-time visualization of sensor data using Streamlit
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import time
import os
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Streamlit page
st.set_page_config(
    page_title="IoT Sensor Dashboard",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling
st.markdown(
    """
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ff6b6b;
    }
    .success-metric {
        border-left-color: #51cf66;
    }
    .warning-metric {
        border-left-color: #ffd43b;
    }
    .danger-metric {
        border-left-color: #ff6b6b;
    }
    .stMetric > div > div > div > div {
        font-size: 1.2rem;
    }
</style>
""",
    unsafe_allow_html=True,
)


class IoTDashboard:
    def __init__(self):
        self.engine = None
        self.connect_to_database()

    def connect_to_database(self):
        """Establish connection to PostgreSQL"""
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "streaming_db")
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "postgres")

        connection_string = (
            f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )

        try:
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            st.error(f"Failed to connect to database: {e}")
            logger.error(f"Database connection failed: {e}")
            return None

    def get_recent_data(self, hours=24):
        """Get recent sensor data from the database"""
        if not self.engine:
            return pd.DataFrame()

        query = """
        SELECT 
            sr.sensor_id,
            sr.location_name,
            sr.timestamp,
            sr.temperature,
            sr.humidity,
            sr.air_quality_index,
            sr.pressure,
            sr.light_level,
            sl.building_type,
            sl.latitude,
            sl.longitude
        FROM sensor_readings sr
        LEFT JOIN sensor_locations sl ON sr.location_name = sl.location_name
        WHERE sr.timestamp >= NOW() - INTERVAL '%s hours'
        ORDER BY sr.timestamp DESC
        """

        try:
            df = pd.read_sql_query(query, self.engine, params=(hours,))
            if not df.empty:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            logger.error(f"Data fetch error: {e}")
            return pd.DataFrame()

    def get_summary_stats(self, df):
        """Calculate summary statistics"""
        if df.empty:
            return {}

        latest_timestamp = df["timestamp"].max()
        total_sensors = df["sensor_id"].nunique()
        total_locations = df["location_name"].nunique()
        total_readings = len(df)

        # Recent data (last hour)
        recent_df = df[df["timestamp"] >= (latest_timestamp - timedelta(hours=1))]
        recent_readings = len(recent_df)

        # Calculate averages
        avg_temp = df["temperature"].mean()
        avg_humidity = df["humidity"].mean()
        avg_aqi = df["air_quality_index"].mean()

        # Detect anomalies (simple threshold-based)
        temp_anomalies = len(df[(df["temperature"] < 0) | (df["temperature"] > 100)])
        humidity_anomalies = len(df[(df["humidity"] < 0) | (df["humidity"] > 100)])
        aqi_anomalies = len(df[df["air_quality_index"] > 150])

        return {
            "latest_timestamp": latest_timestamp,
            "total_sensors": total_sensors,
            "total_locations": total_locations,
            "total_readings": total_readings,
            "recent_readings": recent_readings,
            "avg_temp": avg_temp,
            "avg_humidity": avg_humidity,
            "avg_aqi": avg_aqi,
            "temp_anomalies": temp_anomalies,
            "humidity_anomalies": humidity_anomalies,
            "aqi_anomalies": aqi_anomalies,
        }

    def create_time_series_chart(self, df, metric="temperature"):
        """Create time series chart for a specific metric"""
        if df.empty:
            return go.Figure()

        fig = go.Figure()

        # Color map for locations
        colors = px.colors.qualitative.Set3
        color_map = {
            loc: colors[i % len(colors)]
            for i, loc in enumerate(df["location_name"].unique())
        }

        for location in df["location_name"].unique():
            location_data = df[df["location_name"] == location]

            # Resample to reduce data points for better performance
            if len(location_data) > 1000:
                location_data = (
                    location_data.set_index("timestamp")
                    .resample("5T")
                    .mean()
                    .reset_index()
                )

            fig.add_trace(
                go.Scatter(
                    x=location_data["timestamp"],
                    y=location_data[metric],
                    mode="lines+markers",
                    name=location,
                    line=dict(color=color_map[location], width=2),
                    marker=dict(size=4),
                    hovertemplate=f"<b>{location}</b><br>"
                    + f"Time: %{{x}}<br>"
                    + f"{metric.title()}: %{{y}}<br>"
                    + "<extra></extra>",
                )
            )

        fig.update_layout(
            title=f'{metric.replace("_", " ").title()} Over Time',
            xaxis_title="Time",
            yaxis_title=metric.replace("_", " ").title(),
            hovermode="x unified",
            template="plotly_white",
            height=400,
        )

        return fig

    def create_location_comparison(self, df):
        """Create comparison chart across locations"""
        if df.empty:
            return go.Figure()

        # Calculate latest averages per location
        latest_data = (
            df.groupby("location_name")
            .agg(
                {
                    "temperature": "mean",
                    "humidity": "mean",
                    "air_quality_index": "mean",
                    "pressure": "mean",
                }
            )
            .round(2)
        )

        fig = make_subplots(
            rows=2,
            cols=2,
            subplot_titles=[
                "Temperature (°C)",
                "Humidity (%)",
                "Air Quality Index",
                "Pressure (hPa)",
            ],
            specs=[
                [{"secondary_y": False}, {"secondary_y": False}],
                [{"secondary_y": False}, {"secondary_y": False}],
            ],
        )

        locations = latest_data.index
        colors = px.colors.qualitative.Set3

        # Temperature
        fig.add_trace(
            go.Bar(
                x=locations,
                y=latest_data["temperature"],
                marker_color=colors[0],
                name="Temperature",
                showlegend=False,
            ),
            row=1,
            col=1,
        )

        # Humidity
        fig.add_trace(
            go.Bar(
                x=locations,
                y=latest_data["humidity"],
                marker_color=colors[1],
                name="Humidity",
                showlegend=False,
            ),
            row=1,
            col=2,
        )

        # AQI
        fig.add_trace(
            go.Bar(
                x=locations,
                y=latest_data["air_quality_index"],
                marker_color=colors[2],
                name="AQI",
                showlegend=False,
            ),
            row=2,
            col=1,
        )

        # Pressure
        fig.add_trace(
            go.Bar(
                x=locations,
                y=latest_data["pressure"],
                marker_color=colors[3],
                name="Pressure",
                showlegend=False,
            ),
            row=2,
            col=2,
        )

        fig.update_layout(
            title_text="Current Average Values by Location",
            height=500,
            template="plotly_white",
        )

        # Rotate x-axis labels
        fig.update_xaxes(tickangle=45)

        return fig

    def create_distribution_chart(self, df, metric="temperature"):
        """Create distribution chart for a metric"""
        if df.empty:
            return go.Figure()

        fig = go.Figure()

        for location in df["location_name"].unique():
            location_data = df[df["location_name"] == location]

            fig.add_trace(
                go.Histogram(
                    x=location_data[metric], name=location, opacity=0.7, nbinsx=30
                )
            )

        fig.update_layout(
            title=f'{metric.replace("_", " ").title()} Distribution by Location',
            xaxis_title=metric.replace("_", " ").title(),
            yaxis_title="Frequency",
            barmode="overlay",
            template="plotly_white",
            height=400,
        )

        return fig

    def render_dashboard(self):
        """Render the main dashboard"""
        st.title("🌡️ IoT Sensor Monitoring Dashboard")
        st.markdown(
            "Real-time monitoring of environmental sensors across multiple locations"
        )

        # Sidebar controls
        st.sidebar.header("Dashboard Controls")

        # Time range selector
        time_ranges = {
            "Last Hour": 1,
            "Last 6 Hours": 6,
            "Last 24 Hours": 24,
            "Last 3 Days": 72,
            "Last Week": 168,
        }

        selected_range = st.sidebar.selectbox(
            "Select Time Range",
            options=list(time_ranges.keys()),
            index=2,  # Default to 24 hours
        )

        hours = time_ranges[selected_range]

        # Auto-refresh toggle
        auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)", value=True)

        # Manual refresh button
        if st.sidebar.button("Refresh Now"):
            st.rerun()

        # Load data
        with st.spinner("Loading sensor data..."):
            df = self.get_recent_data(hours=hours)

        if df.empty:
            st.warning("No sensor data available for the selected time range.")
            st.info(
                "Make sure the producer and consumer are running and generating data."
            )
            return

        # Calculate statistics
        stats = self.get_summary_stats(df)

        # Display key metrics
        st.subheader("📊 System Overview")

        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(label="Active Sensors", value=stats["total_sensors"], delta=None)

        with col2:
            st.metric(label="Locations", value=stats["total_locations"], delta=None)

        with col3:
            st.metric(
                label="Total Readings",
                value=f"{stats['total_readings']:,}",
                delta=f"+{stats['recent_readings']} (1h)",
            )

        with col4:
            st.metric(
                label="Avg Temperature", value=f"{stats['avg_temp']:.1f}°C", delta=None
            )

        with col5:
            st.metric(
                label="Avg Air Quality", value=f"{stats['avg_aqi']:.0f}", delta=None
            )

        # Anomaly alerts
        if (
            stats["temp_anomalies"] > 0
            or stats["humidity_anomalies"] > 0
            or stats["aqi_anomalies"] > 0
        ):
            st.subheader("⚠️ Anomaly Alerts")

            alert_col1, alert_col2, alert_col3 = st.columns(3)

            with alert_col1:
                if stats["temp_anomalies"] > 0:
                    st.error(
                        f"🌡️ {stats['temp_anomalies']} temperature anomalies detected"
                    )

            with alert_col2:
                if stats["humidity_anomalies"] > 0:
                    st.error(
                        f"💧 {stats['humidity_anomalies']} humidity anomalies detected"
                    )

            with alert_col3:
                if stats["aqi_anomalies"] > 0:
                    st.error(f"🌬️ {stats['aqi_anomalies']} poor air quality readings")

        # Time series charts
        st.subheader("📈 Real-time Sensor Readings")

        # Metric selector
        metric_options = {
            "Temperature": "temperature",
            "Humidity": "humidity",
            "Air Quality Index": "air_quality_index",
            "Pressure": "pressure",
            "Light Level": "light_level",
        }

        selected_metric = st.selectbox(
            "Select Metric to Display", options=list(metric_options.keys()), index=0
        )

        metric_key = metric_options[selected_metric]

        # Create and display time series chart
        time_series_chart = self.create_time_series_chart(df, metric_key)
        st.plotly_chart(time_series_chart, use_container_width=True)

        # Location comparison and distribution
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🏢 Location Comparison")
            location_chart = self.create_location_comparison(df)
            st.plotly_chart(location_chart, use_container_width=True)

        with col2:
            st.subheader("📊 Data Distribution")
            distribution_chart = self.create_distribution_chart(df, metric_key)
            st.plotly_chart(distribution_chart, use_container_width=True)

        # Recent data table
        st.subheader("📋 Recent Sensor Readings")

        # Show latest 100 readings
        recent_data = df.head(100).copy()
        recent_data["timestamp"] = recent_data["timestamp"].dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # Format numerical columns
        for col in ["temperature", "humidity", "pressure", "light_level"]:
            if col in recent_data.columns:
                recent_data[col] = recent_data[col].round(2)

        st.dataframe(
            recent_data[
                [
                    "timestamp",
                    "location_name",
                    "sensor_id",
                    "temperature",
                    "humidity",
                    "air_quality_index",
                    "pressure",
                    "light_level",
                ]
            ],
            use_container_width=True,
        )

        # Footer with last update time
        st.markdown("---")
        if stats["latest_timestamp"]:
            st.caption(
                f"Last updated: {stats['latest_timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )

        # Auto-refresh implementation
        if auto_refresh:
            time.sleep(30)
            st.rerun()


def main():
    """Main function to run the dashboard"""
    try:
        dashboard = IoTDashboard()
        dashboard.render_dashboard()
    except Exception as e:
        st.error(f"Dashboard error: {e}")
        logger.error(f"Dashboard error: {e}")


if __name__ == "__main__":
    main()
