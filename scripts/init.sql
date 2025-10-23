-- create database
CREATE DATABASE weatherdb;

\c weatherdb;

-- create weather_processed
CREATE TABLE IF NOT EXISTS weather_processed (
    kafka_timestamp TIMESTAMP,
    kafka_offset BIGINT,
    city TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    weather_main TEXT,
    weather_description TEXT,
    temperature DOUBLE PRECISION,
    feels_like DOUBLE PRECISION,
    humidity INTEGER,
    pressure INTEGER,
    wind_speed DOUBLE PRECISION,
    processed_at TEXT,
    temp_category TEXT,
    processing_time TIMESTAMP
);