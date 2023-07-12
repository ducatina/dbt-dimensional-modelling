{% set create_sequence_query %}
CREATE SCHEMA IF NOT EXISTS dbt_dmdemo;
CREATE SEQUENCE IF NOT EXISTS dbt_dmdemo.{{ this.name }}_seq;
{% endset %}
{% do run_query(create_sequence_query) %}

with stg_order_status as (
    select distinct status as order_status
    from
        {{ ref('salesorderheader') }}
)

select
    {{ increment_sequence() }} as order_status_key,
    order_status,
    case
        when order_status = 1 then 'in_process'
        when order_status = 2 then 'approved'
        when order_status = 3 then 'backordered'
        when order_status = 4 then 'rejected'
        when order_status = 5 then 'shipped'
        when order_status = 6 then 'cancelled'
        else 'no_status'
    end as order_status_name
from stg_order_status
