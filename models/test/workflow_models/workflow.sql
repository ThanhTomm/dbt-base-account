{{ config(materialized='table') }}

SELECT 
    w.id
    , w.name
    , w.status
    , w.last_update
    , wd.category
    FROM `base-datateam.dataset_test.workflow` w
    JOIN `base-datateam.dataset_test.workflow_data` wd on w._airbyte_workflow_hashid = wd._airbyte_workflow_hashid