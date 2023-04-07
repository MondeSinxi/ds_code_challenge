with c as (select count(*) as counts from dbq group by notification_number
)

select count(*) as multiplate_joins
from c
where counts > 1
