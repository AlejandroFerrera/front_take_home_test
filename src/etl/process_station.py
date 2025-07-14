from utils.api_client import APIClient
from db.handler import DBHandler
from datetime import datetime
from config.logger import setup_logger

logger = setup_logger(__name__)


class StationProcessor:
    """
    Processes station data from the weather API and manages station information in the database.
    """
    
    def __init__(self, db_client: DBHandler, api_client: APIClient):
        """
        Initialize the StationProcessor.

        Args:
            db_client: Database handler for database operations.
            api_client: API client for making HTTP requests.
        """
        self.db_client = db_client
        self.api_client = api_client

    def process_station(self, station_id: str) -> tuple[int, datetime | None]:
        """
        Fetches, processes, and upserts a station, returning its surrogate key and last observation timestamp.

        Args:
            station_id: The ID of the station to process.

        Returns:
            tuple[int, datetime | None]: A tuple containing the station's surrogate key and last observation timestamp.
        """
        station_raw_data = self._get_station_raw_data(station_id)
        station_data = self._extract_station_fields(station_raw_data)
        return self._create_or_update_station(station_data)

    def _extract_station_fields(self, station: dict) -> dict:
        """
        Extracts station fields from raw API data.

        Args:
            station: A dictionary representing raw station data from the API.

        Returns:
            dict: A dictionary containing processed station fields including station_id, 
                  station_name, station_timezone, longitude, and latitude.
        """
        properties: dict = station.get("properties", {})
        station_id = properties.get("stationIdentifier")
        if not station_id:
            logger.error("Mandatory 'stationIdentifier' is missing.")
            raise ValueError("Mandatory 'stationIdentifier' is missing.")

        station_name = properties.get("name")
        station_timezone = properties.get("timeZone")

        if not all((station_name, station_timezone)):
            logger.warning(
                "Optional field 'name' or 'timeZone' is missing for station %s.",
                station_id,
            )

        coordinates = station.get("geometry", {}).get("coordinates", [])
        longitude = latitude = None
        if len(coordinates) >= 2:
            longitude, latitude = coordinates[0], coordinates[1]
        else:
            logger.warning(
                "Coordinates are missing or incomplete for station %s.", station_id
            )

        return {
            "station_id": station_id,
            "station_name": station_name,
            "station_timezone": station_timezone,
            "longitude": longitude,
            "latitude": latitude,
        }

    def _get_station_raw_data(self, station_id: str) -> dict:
        """
        Fetches raw station data from the API.

        Args:
            station_id: The ID of the station to fetch data for.

        Returns:
            dict: A dictionary containing raw station data from the API.
        """
        response = self.api_client.get(f"/stations/{station_id}")
        return response.json()

    def _create_or_update_station(
        self, station_data: dict
    ) -> tuple[int, datetime | None]:
        """
        Creates or updates a station in the database.

        Args:
            station_data: A dictionary containing station data to insert or update.

        Returns:
            tuple[int, datetime | None]: A tuple containing the station's surrogate key 
                                        and last observation timestamp.
        """
        dim_station_table = self.db_client.metadata.tables["dim_station"]

        result = self.db_client.upsert(
            table=dim_station_table,
            values=station_data,
            conflict_columns=["station_id"],
            update_columns=[
                "station_name",
                "station_timezone",
                "longitude",
                "latitude",
            ],
            returning_columns=["station_sk", "last_observation_at"],
        )

        row = result.fetchone()

        if row is None:
            raise ValueError("No row returned from station insert/update operation.")

        station_sk, last_observation_at = row

        return station_sk, last_observation_at
