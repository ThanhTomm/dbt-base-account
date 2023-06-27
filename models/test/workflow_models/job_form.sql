{{ config(materialized='table') }}

WITH clean_job as (
    SELECT *
        FROM {{ ref("job") }}
)

SELECT
    j.id as job_id
    , j.last_update
    , jf.id as key 
    , jf.name 
    , jf.value
    FROM clean_job j
    JOIN `base-datateam.dataset_test.job_form` jf on j._airbyte_job_hashid = jf._airbyte_job_hashid