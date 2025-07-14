WITH date_interval AS (
    SELECT
        DATE_TRUNC('week', NOW()::timestamp) - INTERVAL '1 week' AS start_date, 
        DATE_TRUNC('week', NOW()::timestamp) AS end_date
)
SELECT
    st.station_id,
    st.station_name,
    AVG(obs.temperature) AS avg_temperature
FROM
    fact_observation AS obs
INNER JOIN
    dim_station AS st
    ON obs.station_sk = st.station_sk
INNER JOIN
    date_interval AS di
    ON obs.observation_timestamp BETWEEN di.start_date AND di.end_date
GROUP BY
    st.station_id,
    st.station_name;