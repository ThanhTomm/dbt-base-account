{{ config(materialized='table') }}

WITH clean_job as (
    SELECT *
        FROM {{ ref("job") }}
)

SELECT
    j.id as job_id
    , j.last_update
    , jm.user_id
    , jm.mover_id
    , jm.from_stage_id 
    , jm.stage_start
    , jm.stage_end
    , jm.stage_deadline
    FROM clean_job j
    JOIN `base-datateam.dataset_test.job_moves` jm on j._airbyte_job_hashid = jm._airbyte_job_hashid