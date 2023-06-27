{{ config(materialized='table') }}

WITH clean_workflow as (
    SELECT *
        FROM {{ ref("workflow") }}
),

clean_stage as (
    SELECT *
        FROM {{ ref("stage") }}
),

clean_user as (
    SELECT *
        FROM {{ ref("user") }}
)

SELECT 
    j.id
    , j._airbyte_job_hashid
    , j.state
    , j.name
    , j.stage_id
    , j.user_id
    , j.creator_id
    , j.workflow_id
    , j.last_update
    , j.since
    , j.finish_at
    , j.deadline
    , j.deadline_assign
    , js.notes as stats_notes
    , js.posts as stats_posts
    , js.likes as stats_likes
    , js.files as stats_files 
    , jof.failed_reason_id
    , jof.failed_name
    , jof.note as failed_note
    , j.status
    FROM `base-datateam.dataset_test.job` as j
    JOIN `base-datateam.dataset_test.job_stats` js on j._airbyte_job_hashid = js._airbyte_job_hashid
    JOIN `base-datateam.dataset_test.job_on_failed` jof on j._airbyte_job_hashid = jof._airbyte_job_hashid and j.stage_id = jof.stage_id
    JOIN clean_workflow as w on j.workflow_id = w.id
    JOIN clean_stage as s on j.stage_id = s.id
    JOIN clean_user as u on j.user_id = u.user_id
    