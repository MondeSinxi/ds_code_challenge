with services as (
select
    *
from 'data/sr.parquet'
),
joined_indexes as (
select 
    s.*, 
    coalesce(df.index, 0) as h3_level8_index,
    abs(df.latitude - s.latitude) as diff
from services s
left join df on
    abs(df.latitude - s.latitude) < {threshold}
    and abs(df.longitude - s.longitude) < {threshold}
)
select distinct on (notification_number) 
    * exclude(diff, column00)
from joined_indexes
order by notification_number, diff
