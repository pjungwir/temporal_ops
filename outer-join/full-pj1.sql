-- Temporal outer join is equivalent to temporal inner join plus temporal antijoin.
-- But it feels like we should be able to do this in one pass of a instead of two.
select  e.id, p.id AS position_id, p.employee_id,
        unnest(multirange(e.valid_at) * multirange(p.valid_at)) AS valid_at
from    employees e
join    positions p
on e.id = p.employee_id and e.valid_at && p.valid_at
union all
select  e.id, null, null,
        unnest(
          case when j.valid_at is null
               then multirange(e.valid_at)
               else multirange(e.valid_at) - j.valid_at end
        )
from    employees e
left join (
  select  p.employee_id, range_agg(p.valid_at) AS valid_at
  from    positions p
  group by p.employee_id
) as j
on e.id = j.employee_id and e.valid_at && j.valid_at
order by id, valid_at
;