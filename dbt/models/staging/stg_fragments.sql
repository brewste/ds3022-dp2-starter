select distinct
    cast(order_no as integer) as order_no,
    trim(word) as word
from {{ source('raw', 'fragments') }}