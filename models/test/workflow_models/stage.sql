{{ config(materialized='table') }}

WITH clean_workflow as (
    SELECT *
        FROM {{ ref("workflow") }}
),

clean_user as (
    SELECT *
        FROM {{ ref("user") }}
)

SELECT 
    s.id
    , s.name
    , s.user_id
    , s.real_order
    , s.last_update
    FROM `base-datateam.dataset_test.stage` as s
    JOIN clean_workflow as w on s.workflow_id = w.id
    JOIN clean_user as u on s.user_id = u.user_id