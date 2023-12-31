{% set create_sequence_query %}
CREATE SCHEMA IF NOT EXISTS dbt_dmdemo;
CREATE SEQUENCE IF NOT EXISTS dbt_dmdemo.{{ this.name }}_seq;
{% endset %}
{% do run_query(create_sequence_query) %}

with stg_customer as (
    select
        customerid,
        personid,
        storeid
    from {{ ref('customer') }}
),

stg_person as (
    select
        businessentityid,
        concat(coalesce(firstname, ''), ' ', coalesce(middlename, ''), ' ', coalesce(lastname, '')) as fullname
    from {{ ref('person') }}
),

stg_store as (
    select
        businessentityid as storebusinessentityid,
        storename
    from {{ ref('store') }}
)

select
    {{ increment_sequence() }}  as customer_key,
    stg_customer.customerid,
    stg_person.businessentityid,
    stg_person.fullname,
    stg_store.storebusinessentityid,
    stg_store.storename
from stg_customer
left join stg_person on stg_customer.personid = stg_person.businessentityid
left join stg_store on stg_customer.storeid = stg_store.storebusinessentityid
