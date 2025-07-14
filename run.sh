#!/bin/bash

STATION_ID=$1

echo "Starting weather data pipeline for station: $STATION_ID"

# Start PostgreSQL
echo "Starting PostgreSQL..."
docker-compose up -d postgres

# Run the weather application
echo "Running weather data pipeline..."
export STATION_ID=$STATION_ID
docker-compose --profile run up weather-app

echo "Pipeline completed!"