-- second version from Boris:
select
   e.id,
   p.id AS position_id, p.employee_id,
   e.valid_at * coalesce(p.valid_at, e.valid_at) as valid_at
from employees e
   left join positions p
   on e.id = p.employee_id
      and e.valid_at && p.valid_at
UNION all (
select
   e.id,
   NULL as position_id, NULL as employee_id,
   unnest (
   multirange(e.valid_at) - range_agg(p.valid_at)) as valid_at
from employees e join positions p
   on e.id = p.employee_id
      and e.valid_at && p.valid_at
group by e.id, e.valid_at)
order by id, valid_at
;
