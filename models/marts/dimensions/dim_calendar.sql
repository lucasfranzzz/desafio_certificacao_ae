with base_calendar as
(
    select
        *
    from {{ ref('int_calendar') }}
)

select 
    date_day
    , month_start_date
    , year_start_date
from base_calendar