{{ config(materialized='table') }}

SELECT * FROM `base-datateam.dataset_test.workflow` WHERE TIMESTAMP_TRUNC(_airbyte_emitted_at, DAY) = TIMESTAMP("2023-06-26") LIMIT 1000
