-- Removing extra UNNEST, expanding the row:
SELECT
   j1.id, j2.id AS position_id, j2.employee_id, j2.valid_at
FROM
(select
   e.id, 
   e.valid_at AS employee_valid_at,
   array_agg(row(p.id,
      coalesce(p.valid_at * e.valid_at, e.valid_at),
      p.name, p.employee_id
   )::positions) as ps,
   range_agg(p.valid_at) as valid_at
from employees e
   left join positions p
   on e.id = p.employee_id
      and e.valid_at && p.valid_at
group by e.id, e.valid_at) AS j1
join lateral
   ((select  p.id, p.employee_id, p.valid_at
      from unnest (ps) as p)
     union select NULL, NULL, unnest (multirange(j1.employee_valid_at) - j1.valid_at)) j2
  on true
order by j1.id, j2.valid_at
;

