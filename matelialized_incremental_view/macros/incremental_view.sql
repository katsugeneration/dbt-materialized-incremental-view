{% macro is_latest_layer() %}
    {#-- do not run introspective queries in parsing #}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {{ return(model.config.is_latest_layer) }}
    {% endif %}
{% endmacro %}


{% materialization incremental_view, default %}
    {{ run_hooks(pre_hooks) }}

    -- main operation
    {% set target_relation = api.Relation.create(
        database=this.database,
        schema=this.schema,
        identifier=this.table,
        type='view',
        ) %}
    {% set target_relation_stack = api.Relation.create(
        database=this.database,
        schema=this.schema,
        identifier=this.table ~ '__stack',
        type='table',
        ) %}
    {% set target_relation_latest = api.Relation.create(
        database=this.database,
        schema=this.schema,
        identifier=this.table ~ '__latest',
        type='view',
        ) %}

    {% do model.config.update({'is_latest_layer': false}) %}
    {% do materialization_incremental_bigquery.context.update({'this': target_relation_stack}) %}
    {% do materialization_incremental_bigquery.context.update({'sql': render(model.raw_code)}) %}
    {{ materialization_incremental_bigquery() }}

    {% do model.config.update({'is_latest_layer': true}) %}
    {% call statement('latest') %}
        {{ create_view_as(target_relation_latest, render(model.raw_code)) }}
    {% endcall %}

    {% set build_sql %}
        select * from {{ target_relation_latest }}
        union all
        select * from {{ target_relation_stack }}
    {% endset %}

    {% call statement('main') %}
        {{ create_view_as(target_relation, build_sql) }}
    {% endcall %}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations':[target_relation]}) }}
{% endmaterialization %}
