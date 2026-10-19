select *
from {{ ref('stg_fragments') }}
having
    count(*) != 21
    or min(order_no) != 1
    or max(order_no) != 21