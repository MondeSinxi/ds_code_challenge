select count(*) as missed_joins
from dbq
where latitude is not null and longitude is not null and h3_level8_index = '0'
