select
    to_char(d, 'YYYYMMDD')::int          as date_key,
    d::date                              as date_day,
    extract(year from d)::int            as year,
    extract(quarter from d)::int         as quarter,
    extract(month from d)::int           as month,
    extract(day from d)::int             as day,
    extract(isodow from d)::int          as iso_weekday,
    extract(isodow from d) in (6, 7)     as is_weekend
from generate_series(date '1900-01-01', date '2035-12-31', interval '1 day') as d
