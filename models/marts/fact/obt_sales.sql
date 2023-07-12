{% set create_sequence_query %}
CREATE SCHEMA IF NOT EXISTS dbt_dmdemo;
CREATE SEQUENCE IF NOT EXISTS dbt_dmdemo.{{ this.name }}_seq;
{% endset %}
{% do run_query(create_sequence_query) %}

with f_sales as (
    select * from {{ ref('fct_sales') }}
),

d_customer as (
    select * from {{ ref('dim_customer') }}
),

d_credit_card as (
    select * from {{ ref('dim_credit_card') }}
),

d_address as (
    select * from {{ ref('dim_address') }}
),

d_order_status as (
    select * from {{ ref('dim_order_status') }}
),

d_product as (
    select * from {{ ref('dim_product') }}
),

d_date as (
    select * from {{ ref('dim_date') }}
)

select
    {{ dbt_utils.star(from=ref('fct_sales'), relation_alias='f_sales', except=[
        "product_key", "customer_key", "creditcard_key", "address_key", "order_status_key", "date_key"
    ]) }},
    {{ dbt_utils.star(from=ref('dim_product'), relation_alias='d_product', except=["product_key"]) }},
    {{ dbt_utils.star(from=ref('dim_customer'), relation_alias='d_customer', except=["customer_key"]) }},
    {{ dbt_utils.star(from=ref('dim_credit_card'), relation_alias='d_credit_card', except=["creditcard_key"]) }},
    {{ dbt_utils.star(from=ref('dim_address'), relation_alias='d_address', except=["address_key"]) }},
    {{ dbt_utils.star(from=ref('dim_order_status'), relation_alias='d_order_status', except=["order_status_key"]) }},
    {{ dbt_utils.star(from=ref('dim_date'), relation_alias='d_date', except=["date_key"]) }}
from f_sales
join d_product on f_sales.product_key = d_product.product_key
join d_customer on f_sales.customer_key = d_customer.customer_key
join d_credit_card on f_sales.creditcard_key = d_credit_card.creditcard_key
join d_address on f_sales.address_key = d_address.address_key
join d_order_status on f_sales.order_status_key = d_order_status.order_status_key
join d_date on f_sales.date_key = d_date.date_key
