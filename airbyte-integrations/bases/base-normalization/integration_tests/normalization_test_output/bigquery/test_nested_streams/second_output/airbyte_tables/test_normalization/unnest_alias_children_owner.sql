

  create or replace table `dataline-integration-testing`.test_normalization.`unnest_alias_children_owner`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
-- Final base SQL model
select
    _airbyte_children_hashid,
    owner_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_owner_hashid
from `dataline-integration-testing`._airbyte_test_normalization.`unnest_alias_children_owner_ab3`
-- owner at unnest_alias/children/owner from `dataline-integration-testing`.test_normalization.`unnest_alias_children`
where 1 = 1
  );
    