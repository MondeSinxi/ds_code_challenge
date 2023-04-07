select
    sr.*,
    df.longitude as df_long,
    df.latitude as df_lat,
    coalesce(df.index, 0) as h3_level8_index
from 's3://cct-ds-code-challenge-input-data/sr.csv.gz' as sr
left join df on
    abs(sr.longitude - df.longitude) < {threshold}
    and (sr.latitude - df.latitude) < {threshold}
limit {limit}
