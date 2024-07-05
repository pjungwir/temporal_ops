-- This works too, probably a better plan:
select  e.id, j2.id as position_id,
        j2.employee_id, 
        coalesce(j2.valid_at, e.valid_at) AS valid_at
from    employees e
left join (
  select  p.employee_id, range_agg(p.valid_at) as valid_at, array_agg(p) AS ps
  from    positions p
  group by p.employee_id
) as j
on e.id = j.employee_id and e.valid_at && j.valid_at
left join lateral (
  select  u.employee_id, u.id, e.valid_at * u.valid_at AS valid_at
  from    unnest(j.ps) as u
  where not isempty(e.valid_at * u.valid_at)
  union all
  select  null, null, u.valid_at
  from unnest(multirange(e.valid_at) - j.valid_at) as u(valid_at)
  where not isempty(u.valid_at)
) as j2 on j.employee_id is not null
where e.id = 10
order by e.id, coalesce(j2.valid_at, e.valid_at)
;

