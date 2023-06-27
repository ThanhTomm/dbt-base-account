{{ config(materialized='table') }}

SELECT DISTINCT
    user_id,
    username
    FROM `base-datateam.dataset_test.job`