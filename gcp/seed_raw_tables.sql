-- Creates the raw source tables that the Dataform declarations point to.
-- Run once in the BigQuery console against your project. The query location must match
-- defaultLocation in workflow_settings.yaml (US).

CREATE SCHEMA IF NOT EXISTS raw;

CREATE OR REPLACE TABLE raw.raw_customers AS
SELECT * FROM UNNEST([
  STRUCT(1 AS customer_id, ' Ada@Example.com ' AS email, TIMESTAMP '2026-01-01 10:00:00 UTC' AS created_at),
  STRUCT(2 AS customer_id, 'grace@example.com' AS email, TIMESTAMP '2026-01-02 11:30:00 UTC' AS created_at),
  STRUCT(3 AS customer_id, 'Linus@Example.com' AS email, TIMESTAMP '2026-01-03 09:15:00 UTC' AS created_at)
]);

CREATE OR REPLACE TABLE raw.raw_orders AS
SELECT * FROM UNNEST([
  STRUCT(101 AS order_id, 1 AS customer_id, 25.50 AS order_total, DATE '2026-01-05' AS order_date),
  STRUCT(102 AS order_id, 1 AS customer_id, 12.00 AS order_total, DATE '2026-01-07' AS order_date),
  STRUCT(103 AS order_id, 2 AS customer_id, 99.90 AS order_total, DATE '2026-01-08' AS order_date)
]);
