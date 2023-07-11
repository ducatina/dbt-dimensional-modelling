DBT_DMSELECT CURRENT_ACCOUNT();

GRANT CREATE WAREHOUSE ON ACCOUNT TO ORGADMIN;

CREATE WAREHOUSE ORG_WH;



SHOW ORGANIZATION ACCOUNTS;

CREATE DATABASE DBT_DM;


CREATE TABLE date (
  date_day date,
  prior_date_day date,
  next_date_day date,
  prior_year_date_day date,
  prior_year_over_year_date_day date,
  day_of_week integer,
  day_of_week_name varchar,
  day_of_month integer,
  day_of_year integer
);

CREATE TABLE stateprovince (
  stateprovinceid integer,
  countryregioncode varchar,
  modifieddate timestamp,
  rowguid varchar,
  name varchar,
  territoryid integer,
  isonlystateprovinceflag boolean,
  stateprovincecode varchar
);

CREATE OR REPLACE TABLE person (
  businessentityid INTEGER,
  title VARCHAR,
  firstname VARCHAR,
  middlename VARCHAR,
  lastname VARCHAR,
  persontype VARCHAR,
  namestyle BOOLEAN,
  suffix VARCHAR,
  modifieddate TIMESTAMP,
  rowguid VARCHAR,
  emailpromotion INTEGER
);

CREATE TABLE countryregion (
  countryregioncode VARCHAR,
  modifieddate TIMESTAMP,
  name VARCHAR
);


CREATE TABLE ADDRESS (
    addressid INT,
    addressline1 VARCHAR(100),
    addressline2 VARCHAR(100),
    city VARCHAR(100),
    stateprovinceid INT,
    postalcode VARCHAR(20),
    spatiallocation VARCHAR(200),
    rowguid STRING,
    modifieddate TIMESTAMP
);


CREATE TABLE productsubcategory (
  productsubcategoryid INTEGER,
  productcategoryid INTEGER,
  name VARCHAR,
  modifieddate TIMESTAMP
);

CREATE TABLE productcategory (
  productcategoryid INTEGER,
  name VARCHAR,
  modifieddate TIMESTAMP
);

CREATE TABLE product (
  productid INTEGER,
  name VARCHAR,
  safetystocklevel SMALLINT,
  finishedgoodsflag BOOLEAN,
  class VARCHAR,
  makeflag BOOLEAN,
  productnumber VARCHAR,
  reorderpoint SMALLINT,
  modifieddate TIMESTAMP,
  rowguid VARCHAR,
  productmodelid INTEGER,
  weightunitmeasurecode VARCHAR,
  standardcost NUMERIC,
  productsubcategoryid INTEGER,
  listprice NUMERIC,
  daystomanufacture INTEGER,
  productline VARCHAR,
  color VARCHAR,
  sellstartdate TIMESTAMP,
  weight NUMERIC
);

CREATE TABLE store (
  businessentityid INTEGER,
  storename VARCHAR,
  salespersonid INTEGER,
  modifieddate TIMESTAMP
);


CREATE TABLE salesreason (
  salesreasonid INTEGER,
  name VARCHAR,
  reasontype VARCHAR,
  modifieddate TIMESTAMP
);

CREATE TABLE salesorderheadersalesreason (
  salesorderid INTEGER,
  modifieddate TIMESTAMP,
  salesreasonid INTEGER
);

CREATE TABLE salesorderheader (
  salesorderid INTEGER,
  shipmethodid INTEGER,
  billtoaddressid INTEGER,
  modifieddate TIMESTAMP,
  rowguid VARCHAR,
  taxamt NUMERIC,
  shiptoaddressid INTEGER,
  onlineorderflag BOOLEAN,
  territoryid INTEGER,
  status SMALLINT,
  orderdate TIMESTAMP,
  creditcardapprovalcode VARCHAR,
  subtotal NUMERIC,
  creditcardid INTEGER,
  currencyrateid INTEGER,
  revisionnumber SMALLINT,
  freight NUMERIC,
  duedate TIMESTAMP,
  totaldue NUMERIC,
  customerid INTEGER,
  salespersonid INTEGER,
  shipdate TIMESTAMP,
  accountnumber VARCHAR
);


CREATE TABLE salesorderdetail (
  salesorderid INTEGER,
  orderqty SMALLINT,
  salesorderdetailid INTEGER,
  unitprice NUMERIC,
  specialofferid INTEGER,
  modifieddate TIMESTAMP,
  rowguid VARCHAR,
  productid INTEGER,
  unitpricediscount NUMERIC
);

CREATE TABLE customer (
  customerid INTEGER,
  personid INTEGER,
  storeid INTEGER,
  territoryid INTEGER
);

CREATE TABLE creditcard (
  creditcardid INTEGER,
  cardtype VARCHAR,
  expyear SMALLINT,
  modifieddate TIMESTAMP,
  expmonth SMALLINT,
  cardnumber VARCHAR
);



create or replace transient table DBT_DM.dbt_dpetrusic.dim_product
         as
        (

with surrogate_key as (
    select nextval('surrogate_key_seq') as surrogate_key
),

stg_product as (
    select *
    from DBT_DM.dbt_dpetrusic.product
),

stg_product_subcategory as (
    select *
    from DBT_DM.dbt_dpetrusic.productsubcategory
),

stg_product_category as (
    select *
    from DBT_DM.dbt_dpetrusic.productcategory
)

select
   surrogate_key as product_key,
    stg_product.productid,
    stg_product.name as product_name,
    stg_product.productnumber,
    stg_product.color,
    stg_product.class,
    stg_product_subcategory.name as product_subcategory_name,
    stg_product_category.name as product_category_name
from stg_product
left join stg_product_subcategory on stg_product.productsubcategoryid = stg_product_subcategory.productsubcategoryid
left join stg_product_category on stg_product_subcategory.productcategoryid = stg_product_category.productcategoryid
        );

        select surrogate_key_seq.nextval as surrogate_key


  CREATE SEQUENCE dim_product_seq 
  START = 1
  INCREMENT = 1;

  
CREATE SEQUENCE dim_date_seq 
  START = 1
  INCREMENT = 1;

CREATE SEQUENCE fct_sales_seq 
  START = 1
  INCREMENT = 1;

  CREATE SEQUENCE dim_credit_card_seq 
  START = 1
  INCREMENT = 1;

   CREATE SEQUENCE dim_address_seq 
  START = 1
  INCREMENT = 1;

  CREATE SEQUENCE DIM_CUSTOMER_SEQ
  START = 1
  INCREMENT = 1;

  CREATE SEQUENCE dim_order_status_seq
  START = 1
  INCREMENT = 1;


with stg_salesorderheader as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        status as order_status,
        cast(orderdate as date) as orderdate
    from salesorderheader
),

stg_salesorderdetail as (
    select
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        unitprice * orderqty as revenue
    from salesorderdetail
)
  select
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
inner JOIN dim_address ON stg_salesorderheader.shiptoaddressid = dim_address.addressid;




        (with f_sales as (
    select * from DBT_DM.dbt_dpetrusic.fct_sales
),

d_customer as (
    select * from DBT_DM.dbt_dpetrusic.dim_customer
),

d_credit_card as (
    select * from DBT_DM.dbt_dpetrusic.dim_credit_card
),

d_address as (
    select * from DBT_DM.dbt_dpetrusic.dim_address
),

d_order_status as (
    select * from DBT_DM.dbt_dpetrusic.dim_order_status
),

d_product as (
    select * from DBT_DM.dbt_dpetrusic.dim_product
),

d_date as (
    select * from DBT_DM.dbt_dpetrusic.dim_date
)

select
    f_sales."SALES_KEY",
  f_sales."SALESORDERID",
  f_sales."SALESORDERDETAILID",
  f_sales."UNITPRICE",
  f_sales."ORDERQTY",
  f_sales."REVENUE",
    d_product."PRODUCTID",
  d_product."PRODUCT_NAME",
  d_product."PRODUCTNUMBER",
  d_product."COLOR",
  d_product."CLASS",
  d_product."PRODUCT_SUBCATEGORY_NAME",
  d_product."PRODUCT_CATEGORY_NAME",
    d_customer."CUSTOMERID",
  d_customer."BUSINESSENTITYID",
  d_customer."FULLNAME",
  d_customer."STOREBUSINESSENTITYID",
  d_customer."STORENAME",
    d_credit_card."CREDITCARDID",
  d_credit_card."CARDTYPE",
    d_address."ADDRESSID",
  d_address."CITY_NAME",
  d_address."STATE_NAME",
  d_address."COUNTRY_NAME",
    d_order_status."ORDER_STATUS",
  d_order_status."ORDER_STATUS_NAME",
    d_date."DATE_DAY",
  d_date."PRIOR_DATE_DAY",
  d_date."NEXT_DATE_DAY",
  d_date."PRIOR_YEAR_DATE_DAY",
  d_date."PRIOR_YEAR_OVER_YEAR_DATE_DAY",
  d_date."DAY_OF_WEEK",
  d_date."DAY_OF_WEEK_NAME",
  d_date."DAY_OF_MONTH",
  d_date."DAY_OF_YEAR"
from f_sales
left join d_product on f_sales.product_key = d_product.product_key
left join d_customer on f_sales.customer_key = d_customer.customer_key
left join d_credit_card on f_sales.creditcard_key = d_credit_card.creditcard_key
left join d_address on f_sales.address_key = d_address.address_key
left join d_order_status on f_sales.order_status_key = d_order_status.order_status_key
left join d_date on f_sales.date_key = d_date.date_key
        );
