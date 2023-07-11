with stg_date as (
    select * from {{ ref('date') }}
)

select
    {{ increment_sequence() }} as date_key,
    *
from stg_date