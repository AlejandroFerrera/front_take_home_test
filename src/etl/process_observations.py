from datetime import datetime, timezone, timedelta
from typing import List
from utils.api_client import APIClient
from db.handler import DBHandler
from config.logger import setup_logger

logger = setup_logger(__name__)

class ObservationProcessor:
    """
    Processes observation data from the weather API and manages observation information in the database.
    """
    
    def __init__(self, db_client: DBHandler, api_client: APIClient):
        """
        Initialize the ObservationProcessor.

        Args:
            db_client: Database handler for database operations.
            api_client: API client for making HTTP requests.
        """
        self.db_client = db_client
        self.api_client = api_client

    def process_observations(
        self,
        station_id: str,
        station_sk: int,
        last_observation_timestamp: datetime | None = None,
    ) -> bool:
        """
        Fetches, processes, and loads observations for a station.

        Args:
            station_id: The ID of the station to fetch observations for.
            station_sk: The surrogate key of the station.
            last_observation_timestamp: The last observation timestamp to filter new observations.

        Returns:
            bool: True if observations were loaded successfully, False otherwise.
        """
        observations_raw = self._get_observations_raw_data(
            station_id, last_observation_timestamp
        )

        if not observations_raw:
            logger.info("No observations found for station %s", station_id)
            return False

        observations_data = self._extract_observations_fields(
            observations_raw, station_sk
        )
        return self._load_observations(observations_data, station_sk)

    def _get_observations_raw_data(
        self, station_id: str, last_observation_timestamp: datetime | None = None
    ) -> List[dict]:
        """
        Fetches raw observation data from the API.

        Args:
            station_id: The ID of the station to fetch observations for.
            last_observation_timestamp: The last observation timestamp to filter new observations.

        Returns:
            List[dict]: A list of dictionaries representing observations.
        """
        now_utc = datetime.now(timezone.utc)
        if last_observation_timestamp is None:
            logger.info(
                "No last observation timestamp provided, fetching observations from the last 7 days."
            )
            start_date = (now_utc - timedelta(days=7)).isoformat()
        else:
            start_date = (last_observation_timestamp + timedelta(seconds=1)).isoformat()
        end_date = now_utc.isoformat()

        response = self.api_client.get(
            f"/stations/{station_id}/observations/",
            params={"start": start_date, "end": end_date},
        )
        return response.json().get("features", [])

    def _extract_observations_fields(
        self, observations_raw: List[dict], station_sk: int
    ) -> List[dict]:
        """
        Extracts observation fields from raw API data.

        Args:
            observations_raw: List of raw observation dictionaries from API.
            station_sk: The surrogate key of the station.

        Returns:
            List[dict]: List of processed observation dictionaries.
        """
        return [
            self._extract_observation_fields(obs, station_sk)
            for obs in observations_raw
        ]

    def _extract_observation_fields(self, observation: dict, station_sk: int) -> dict:
        """
        Extracts temperature, wind speed, and humidity from an observation.

        Fields mapping:
        - 'station_sk': Surrogate key of the station
        - 'observation_timestamp': 'properties.timestamp'
        - 'temperature': 'properties.temperature.value'
        - 'wind_speed': 'properties.windSpeed.value'
        - 'humidity': 'properties.relativeHumidity.value'

        Args:
            observation: A dictionary representing a single observation.
            station_sk: The surrogate key of the station.

        Returns:
            dict: A dictionary containing processed observation fields.
        """
        properties = observation.get("properties")
        if not properties:
            logger.error("Mandatory 'properties' field is missing.")
            raise ValueError("Mandatory 'properties' field is missing.")

        timestamp = properties.get("timestamp")
        if not timestamp:
            logger.error("Mandatory 'timestamp' field is missing in observation.")
            raise ValueError("Mandatory 'timestamp' field is missing in observation.")

        temperature = self._get_rounded_value(properties, "temperature", station_sk)
        wind_speed = self._get_rounded_value(properties, "windSpeed", station_sk)
        humidity = self._get_rounded_value(properties, "relativeHumidity", station_sk)

        return {
            "station_sk": station_sk,
            "observation_timestamp": timestamp,
            "temperature": temperature,
            "wind_speed": wind_speed,
            "humidity": humidity,
        }

    def _get_rounded_value(
        self, properties: dict, field: str, station_sk: int
    ) -> float | None:
        """
        Extract and round a numeric value from observation properties.

        Args:
            properties: Properties dictionary from observation
            field: Field name to extract
            station_sk: Station surrogate key for logging

        Returns:
            Rounded value or None if missing
        """
        value = properties.get(field, {}).get("value")
        if value is None:
            logger.warning(
                "Optional field '%s' is missing for station %s.", field, station_sk
            )
            return None
        return round(value, 2)

    def _load_observations(
        self, observations_data: List[dict], station_sk: int
    ) -> bool:
        """
        Loads observations into the database and updates station's last observation timestamp.

        Args:
            observations_data: List of processed observation dictionaries.
            station_sk: The surrogate key of the station.

        Returns:
            bool: True if observations were loaded successfully.
        """
        fact_observation_table = self.db_client.metadata.tables["fact_observation"]

        result = self.db_client.insert_many(
            fact_observation_table,
            observations_data,
            returning_cols=["observation_timestamp"],
        )

        inserted_timestamps = result.fetchall()
        if not inserted_timestamps:
            logger.debug("No observations were inserted for station %s", station_sk)
            return False

        # Update the station's last observation timestamp
        last_timestamp = max([ts[0] for ts in inserted_timestamps])
        self._update_station_last_observation(station_sk, last_timestamp)

        logger.info(
            "Loaded %d observations for station %s. Last timestamp: %s",
            len(inserted_timestamps),
            station_sk,
            last_timestamp,
        )
        return True

    def _update_station_last_observation(
        self, station_sk: int, last_timestamp: datetime
    ):
        """
        Updates the station's last observation timestamp.

        Args:
            station_sk: The surrogate key of the station.
            last_timestamp: The last observation timestamp to update.
        """
        dim_station_table = self.db_client.metadata.tables["dim_station"]

        self.db_client.update(
            dim_station_table,
            values={"station_sk": station_sk, "last_observation_at": last_timestamp},
            matching_columns=["station_sk"],
            fields_to_update=["last_observation_at"],
        )
