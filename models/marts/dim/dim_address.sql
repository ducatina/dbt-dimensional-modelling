{% set create_sequence_query %}
CREATE SCHEMA IF NOT EXISTS dbt_dmdemo;
CREATE SEQUENCE IF NOT EXISTS dbt_dmdemo.{{ this.name }}_seq;
{% endset %}
{% do run_query(create_sequence_query) %}

with stg_address as (
    select *
    from {{ ref('address') }}
),

stg_stateprovince as (
    select *
    from {{ ref('stateprovince') }}
),

stg_countryregion as (
    select *
    from {{ ref('countryregion') }}
)

select
    {{ increment_sequence() }} as address_key,
    stg_address.addressid,
    stg_address.city as city_name,
    stg_stateprovince.name as state_name,
    stg_countryregion.name as country_name
from stg_address
left join stg_stateprovince on stg_address.stateprovinceid = stg_stateprovince.stateprovinceid
left join stg_countryregion on stg_stateprovince.countryregioncode = stg_countryregion.countryregioncode
