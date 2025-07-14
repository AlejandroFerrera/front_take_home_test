# Weather Data Pipeline

A data pipeline that collects weather data from the National Weather Service API, stores it in a PostgreSQL database, and generates insights about temperature and wind.

## Overview

This pipeline processes weather station data by:
- Fetching station metadata and observations from the NWS API (api.weather.gov)
- Storing structured data in a PostgreSQL database
- Generating insights about average temperature and wind speed changes

## Requirements

- Docker

## Quick Start

1. Clone this repository

2. Run the pipeline with a specific weather station ID:

```bash
./run.sh 0007W
```

`0007W` can be replaced with any valid NWS station identifier.

- Other station IDs examples:
  - `000PG`
  - `0128W`
  - `001CE`

## Project Structure

```
├── Dockerfile             # Application container definition
├── compose.yml            # Docker services configuration
├── init-scripts           # Database initialization scripts
├── requirements.txt       # Python dependencies
├── run.sh                 # Main execution script
└── src                    # Python source code
    ├── config             # Configuration management
    ├── db                 # Database connection and operations
    ├── etl                # Data extraction and processing logic
    ├── insights           # SQL queries and reporting functions
    ├── utils              # API client and helper functions
    └── main.py            # Application entry point
```

## Architecture

The pipeline consists of two main components:
- **PostgreSQL database**: Stores station metadata and weather observations
- **Python application**: Handles ETL processes and insights generation

### Database Schema

- **dim_station**: Weather station dimensional data
- **fact_observation**: Weather observations fact table

### ETL Process

1. Fetch station metadata from the API
2. Update station information in the database (SCD 1)
3. Fetch latest weather observations for the station
4. Process and store the observations
5. Generate insights on temperature and wind speed

## Insights

The pipeline generates two key insights after processing the data (these are output to the console):
- Average temperature for the station over the last week
- Maximum wind speed change over the last 7 days

## Assumptions

- The Weather.gov API uses WMO format with the following units:
	- Temperature: Celsius (wmoUnit:degC)
	- Wind speed: km/h (wmoUnit:km_h-1)
	- Humidity: % (wmoUnit:percent)

- The NWS API has limited historical data availability
- Null values for temperature, wind speed, and humidity are stored as NULL in the database
- For simplicity, I hardcoded the database credentials in the `compose.yml` file. In a production environment, these should be managed securely.
- If the station ID is not found, an error is raised based on the assumption that this operation is part of a broader data pipeline where such IDs are validated beforehand. This approach ensures that appropriate actions—such as triggering alerts or rescheduling the job—can be taken.


