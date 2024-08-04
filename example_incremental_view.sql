{{
    config(
        materialized='incremental_view',
    )
}}
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
)
{% if is_view_build() %}
where partition_date >= date('2024-07-20')
{% else %}
where partition_date < date('2024-07-20')
{% endif %}
