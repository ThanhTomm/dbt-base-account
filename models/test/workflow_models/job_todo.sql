{{ config(materialized='table') }}

WITH clean_job as (
    SELECT *
        FROM {{ ref("job") }}
)

SELECT
    j.id as job_id
    , j.user_id
    , j.last_update
    , jt.id 
    , jt.name 
    , jt.stage_id
    , jt.status
    , jt.deadline
    FROM clean_job j
    JOIN `base-datateam.dataset_test.job_todos` jt on j._airbyte_job_hashid = jt._airbyte_job_hashid