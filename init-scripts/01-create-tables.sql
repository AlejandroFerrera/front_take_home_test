CREATE TABLE IF NOT EXISTS dim_station (
	station_sk  BIGSERIAL PRIMARY KEY,
	station_id VARCHAR(100) UNIQUE NOT NULL,
	station_name VARCHAR(500),
	station_timezone VARCHAR(500),
	latitude NUMERIC(12, 8),
	longitude NUMERIC(12, 8),
	last_observation_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS fact_observation (
	station_sk BIGINT NOT NULL,
	observation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
	temperature NUMERIC(5, 2), -- Celsius (°C)
	wind_speed NUMERIC(5, 2),  -- km/h
 	humidity NUMERIC(5, 2),    -- Percentage (%)
	PRIMARY KEY (station_sk, observation_timestamp),
	FOREIGN KEY (station_sk) REFERENCES dim_station(station_sk)
);