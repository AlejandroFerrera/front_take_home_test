import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.handler import DBHandler
from config.config import config
from sqlalchemy import text
from config.logger import setup_logger

logger = setup_logger('INSIGHTS')


def get_station_avg_last_week_temperature(
    station_id: str, db_client: DBHandler
) -> None:
    """Get last week's average temperature for a specific station."""
    query = """
        WITH date_interval AS (
            SELECT
                DATE_TRUNC('week', NOW()::timestamp) - INTERVAL '1 week' AS start_date, 
                DATE_TRUNC('week', NOW()::timestamp) AS end_date
        )
        SELECT
            st.station_id,
            st.station_name,
            ROUND(AVG(obs.temperature), 2) AS avg_temperature
        FROM
            fact_observation AS obs
        INNER JOIN
            dim_station AS st ON obs.station_sk = st.station_sk
        INNER JOIN
            date_interval AS di ON obs.observation_timestamp BETWEEN di.start_date AND di.end_date
        WHERE
            st.station_id = :station_id
        GROUP BY
            st.station_id,
            st.station_name;
    """

    result = db_client.conn.execute(text(query), {"station_id": station_id})
    row = result.fetchone()

    if row:
        logger.info(
            f"Station ID: {row.station_id}, Station Name: {row.station_name}, Average Temperature: {row.avg_temperature}"
        )
    else:
        logger.warning(f"No insights found for station ID: {station_id}")


def get_station_max_wind_speed_change(station_id: str, db_client: DBHandler) -> None:
    """Get the maximum wind speed change for a specific station in the last 7 days."""
    query = """
        WITH wind_speed_change AS (
            SELECT
                obs.station_sk,
                obs.observation_timestamp,
                st.station_id,
                st.station_name,
                obs.wind_speed,
                LAG(obs.wind_speed) OVER (
                    PARTITION BY obs.station_sk
                    ORDER BY obs.observation_timestamp
                ) AS previous_wind_speed
            FROM fact_observation AS obs
            JOIN dim_station AS st ON obs.station_sk = st.station_sk
            WHERE obs.wind_speed IS NOT NULL
              AND obs.observation_timestamp >= NOW() - INTERVAL '7 days'
        )
        SELECT
            station_id,
            station_name,
            MAX(wind_speed - previous_wind_speed) AS max_wind_speed_change
        FROM wind_speed_change
        WHERE previous_wind_speed IS NOT NULL
          AND station_id = :station_id
        GROUP BY station_id, station_name;
    """

    result = db_client.conn.execute(text(query), {"station_id": station_id})
    row = result.fetchone()

    if row:
        logger.info(
            f"Station ID: {row.station_id}, Station Name: {row.station_name}, Max Wind Speed Change: {row.max_wind_speed_change}"
        )
    else:
        logger.warning(f"No insights found for station ID: {station_id}")


if __name__ == "__main__":
    db_client = DBHandler(config)
    # Example usage
    get_station_avg_last_week_temperature("0128W", db_client)
    get_station_max_wind_speed_change("0128W", db_client)
