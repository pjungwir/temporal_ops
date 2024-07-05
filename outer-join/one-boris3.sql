SELECT
   j.id, unnest (
     (select array_agg(row(j2.id, j2.valid_at, j2.name, j2.employee_id)::positions) from
   ((select  p.id, p.valid_at, p.name, p.employee_id
      from unnest (ps) as p)
     union select NULL, unnest (multirange(employee_valid_at) - j.valid_at), NULL, NULL ) AS j2
   ))
FROM
(select
   e.id,
   e.valid_at AS employee_valid_at,
   array_agg(row(p.id, coalesce(p.valid_at * e.valid_at, e.valid_at),
      p.name, p.employee_id
   )::positions) as ps,
   range_agg(p.valid_at) as valid_at
from employees e
   left join positions p
   on e.id = p.employee_id
      and e.valid_at && p.valid_at
where e.id = 10
group by e.id, e.valid_at) j
;
