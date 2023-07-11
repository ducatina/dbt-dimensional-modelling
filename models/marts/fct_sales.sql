with stg_salesorderheader as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        status as order_status,
        cast(orderdate as date) as orderdate
    from {{ ref('salesorderheader') }}
),

stg_salesorderdetail as (
    select
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        unitprice * orderqty as revenue
    from {{ ref('salesorderdetail') }}
)

select
    {{ increment_sequence() }} as sales_key,
    product_key,
    customer_key,
    date_key,
    creditcard_key,
    address_key,
    order_status_key,
    fct.salesorderid,
    fct.salesorderdetailid,
    fct.unitprice,
    fct.orderqty,
    fct.revenue
from stg_salesorderdetail as fct
inner join stg_salesorderheader on fct.salesorderid = stg_salesorderheader.salesorderid
inner JOIN dim_product ON fct.productid = dim_product.productid
inner JOIN dim_customer ON stg_salesorderheader.customerid = dim_customer.customerid
inner JOIN dim_date ON stg_salesorderheader.orderdate = dim_date.date_day
inner JOIN dim_credit_card ON stg_salesorderheader.creditcardid = dim_credit_card.creditcardid
inner JOIN dim_order_status ON stg_salesorderheader.order_status = dim_order_status.order_status
inner JOIN dim_address ON stg_salesorderheader.shiptoaddressid = dim_address.addressid