-- stg_geolocation.sql — Modelo staging de geolocalizacion

SELECT
    geolocation_zip_code_prefix,
    CAST(geolocation_lat AS DOUBLE) AS geolocation_lat,
    CAST(geolocation_lng AS DOUBLE) AS geolocation_lng,
    geolocation_city,
    geolocation_state
FROM {{ source('staging', 'stg_geolocation') }}
