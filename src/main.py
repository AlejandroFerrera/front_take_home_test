import argparse
from etl.process_station import StationProcessor
from etl.process_observations import ObservationProcessor
from db.handler import DBHandler
from utils.api_client import APIClient
from config.config import config
from config.logger import setup_logger
from insights.get_insights import (
    get_station_avg_last_week_temperature,
    get_station_max_wind_speed_change,
)

# Create logger for this module
logger = setup_logger(__name__)


def main():
    """Main function to run the weather data pipeline."""
    db_handler = None
    api_client = None

    try:
        parser = argparse.ArgumentParser(description="Weather data pipeline")
        parser.add_argument("--station_id", help="Station ID to process", required=True)
        args = parser.parse_args()

        station_id = args.station_id.strip()

        # Initialize components
        db_handler = DBHandler(config)
        api_client = APIClient(config.WEATHER_API_BASE_URL, timeout=config.API_TIMEOUT)
        logger.info("Initialized API client and database handler")

        # Create processors
        station_processor = StationProcessor(db_handler, api_client)
        observation_processor = ObservationProcessor(db_handler, api_client)

        # Process station and observations
        logger.info(f"Processing station: {station_id}")

        station_sk, last_observation_timestamp = station_processor.process_station(
            station_id
        )
        logger.info(
            f"Station processed. SK: {station_sk}, Last observation: {last_observation_timestamp}"
        )

        observation_processor.process_observations(
            station_id,
            station_sk,
            last_observation_timestamp,
        )

        logger.info("Weather data pipeline completed successfully")

        # Get insights
        get_station_avg_last_week_temperature(station_id, db_handler)
        get_station_max_wind_speed_change(station_id, db_handler)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise  # RETRY OR ALERT IN THE MACRO SYSTEM
    finally:
        if api_client:
            api_client.close()
        if db_handler:
            db_handler.close()


if __name__ == "__main__":
    main()
