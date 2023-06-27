{{ config(materialized='table') }}

WITH clean_workflow as (
    SELECT *
        FROM {{ ref("workflow") }}
)

SELECT 
    s.id
    , s.name
    , s.user_id
    , s.real_order
    , s.last_update
    FROM `base-datateam.dataset_test.stage` as s
    JOIN clean_workflow as w on s.workflow_id = w.id