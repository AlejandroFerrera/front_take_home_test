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
    JOIN dim_station AS st
      ON obs.station_sk = st.station_sk
    WHERE obs.wind_speed IS NOT NULL
      AND obs.observation_timestamp >= NOW() - INTERVAL '7 days'
)
SELECT
    station_id,
    station_name,
    MAX(wind_speed - previous_wind_speed) AS max_wind_speed_change
FROM wind_speed_change
WHERE previous_wind_speed IS NOT NULL
GROUP BY
    station_id,
    station_name;