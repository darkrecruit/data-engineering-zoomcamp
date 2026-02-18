/* @bruin

name: staging.trips
type: bq.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

depends:
  - ingestion.trips
  - ingestion.payment_lookup

columns:
  - name: pickup_datetime
    type: TIMESTAMP
    description: when the trip started
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: TIMESTAMP
    description: when the trip ended
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_location_id
    type: INTEGER
    description: location ID where the trip started
    primary_key: true
    checks:
      - name: not_null
      - name: non_negative
  - name: dropoff_location_id
    type: INTEGER
    description: location ID where the trip ended
    primary_key: true
    checks:
      - name: not_null
      - name: non_negative
  - name: fare_amount
    type: FLOAT
  - name: taxi_type
    type: STRING
  - name: payment_type_name
    type: STRING

custom_checks:
  - name: TODO_custom_check_name
    description: TODO
    value: 0
    query: |
      -- TODO: return a single scalar (COUNT(*), etc.) that should match `value`
      SELECT 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.
WITH yellow_trips AS (
  SELECT
      t.tpep_pickup_datetime AS pickup_datetime,
      t.tpep_dropoff_datetime AS dropoff_datetime,
      t.pu_location_id AS pickup_location_id,
      t.do_location_id AS dropoff_location_id,
      t.fare_amount,
      t.taxi_type,
      p.payment_type_name
  FROM ingestion.trips t
  LEFT JOIN ingestion.payment_lookup p
      ON t.payment_type = p.payment_type_id
  WHERE t.tpep_pickup_datetime >= '{{ start_datetime }}'
    AND t.tpep_pickup_datetime < '{{ end_datetime }}'
    AND t.taxi_type = 'yellow'
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY t.tpep_pickup_datetime, t.tpep_dropoff_datetime,
                  t.pu_location_id, t.do_location_id, t.vendor_id
      ORDER BY t.tpep_pickup_datetime
  ) = 1
),
green_trips AS (
  SELECT
      t.lpep_pickup_datetime AS pickup_datetime,
      t.lpep_dropoff_datetime AS dropoff_datetime,
      t.pu_location_id AS pickup_location_id,
      t.do_location_id AS dropoff_location_id,
      t.fare_amount,
      t.taxi_type,
      p.payment_type_name
  FROM ingestion.trips t
  LEFT JOIN ingestion.payment_lookup p
      ON t.payment_type = p.payment_type_id
  WHERE t.lpep_pickup_datetime >= '{{ start_datetime }}'
    AND t.lpep_pickup_datetime < '{{ end_datetime }}'
    AND t.taxi_type = 'green'
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY t.lpep_pickup_datetime, t.lpep_dropoff_datetime,
                  t.pu_location_id, t.do_location_id, t.vendor_id
      ORDER BY t.lpep_pickup_datetime
  ) = 1
)
SELECT * FROM yellow_trips
UNION ALL
SELECT * FROM green_trips;
