{{
    config(
        materialized='incremental_view',
        partition_by={
            'field': 'partition_date',
            'data_type': 'date',
        },
        incremental_strategy='insert_overwrite',
        alias='example',
    )
}}

with base as (
    select * from (
        select
            1 as id,
            date('2024-07-19') as partition_date,
        union all
        select
            1 as id,
            date('2024-07-20') as partition_date,
        union all
        select
            2 as id,
            date('2024-07-20') as partition_date,
        union all
        select
            1 as id,
            date('2024-07-21') as partition_date,
        union all
        select
            2 as id,
            date('2024-07-21') as partition_date,
        union all
        select
            3 as id,
            date('2024-07-21') as partition_date,
    )
)

select
    *
from base
{% if is_view_layer() %}
where partition_date >= date('2024-07-21')
{% else %}
where partition_date < date('2024-07-21')
{% if is_incremental() %}
and partition_date >= date('2024-07-20')
{% endif %}
{% endif %}
