{% set create_sequence_query %}
CREATE SCHEMA IF NOT EXISTS dbt_dmdemo;
CREATE SEQUENCE IF NOT EXISTS dbt_dmdemo.{{ this.name }}_seq;
{% endset %}
{% do run_query(create_sequence_query) %}

with stg_date as (
    select * from {{ ref('date') }}
)

select
    {{ increment_sequence() }} as date_key,
    *
from stg_date