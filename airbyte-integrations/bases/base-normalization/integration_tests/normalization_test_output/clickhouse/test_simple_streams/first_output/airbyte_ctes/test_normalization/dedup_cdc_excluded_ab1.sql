

  create view _airbyte_test_normalization.dedup_cdc_excluded_ab1__dbt_tmp 
  
  as (
    
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    JSONExtractRaw(_airbyte_data, 'id') as id,
    JSONExtractRaw(_airbyte_data, 'name') as name,
    JSONExtractRaw(_airbyte_data, '_ab_cdc_lsn') as _ab_cdc_lsn,
    JSONExtractRaw(_airbyte_data, '_ab_cdc_updated_at') as _ab_cdc_updated_at,
    JSONExtractRaw(_airbyte_data, '_ab_cdc_deleted_at') as _ab_cdc_deleted_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from test_normalization._airbyte_raw_dedup_cdc_excluded as table_alias
-- dedup_cdc_excluded
where 1 = 1

  )