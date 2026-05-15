{{ config(materialized='view') }}

with ranked as (
    select
        v:id::string                 as transaction_id,
        v:account_id::string         as account_id,
        v:amount::float              as amount,
        v:txn_type::string           as transaction_type,
        v:related_account_id::string as related_account_id,
        v:status::string             as status,
        v:created_at::timestamp      as transaction_time,
        current_timestamp            as load_timestamp,
        row_number() over (
            partition by v:id::string
            order by v:created_at::timestamp desc
        ) as rn
    from {{ source('raw', 'transactions') }}
)

select
    transaction_id,
    account_id,
    amount,
    transaction_type,
    related_account_id,
    status,
    transaction_time,
    load_timestamp
from ranked
where rn = 1
