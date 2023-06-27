{{ config(materialized='table') }}

SELECT 
    w.id
    , w.name
    , w.status
    , w.last_update
    , wd.category
    FROM `base-data-analyst.dataset_test.workflow` w
    JOIN `base-data-analyst.dataset_test.workflow_data` wd on w._airbyte_workflows 