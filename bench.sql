\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS btree_gist;

DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
  valid_at daterange NOT NULL,

  name TEXT NOT NULL,
  salary INT NOT NULL,

  -- no SQL:2011 yet, so use an exclusion constraint instead:
  -- CONSTRAINT employees_pkey PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
  CONSTRAINT employees_pkey EXCLUDE USING gist (id WITH =, valid_at WITH &&)
);

DROP TABLE IF EXISTS positions;
CREATE TABLE positions (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
  valid_at daterange NOT NULL,

  name TEXT NOT NULL,
  employee_id BIGINT NOT NULL,

  -- no SQL:2011 yet, so use an exclusion constraint instead (and no FK):
  -- CONSTRAINT positions_pkey PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
  -- CONSTRAINT positions_to_employees_fk FOREIGN KEY
    -- (employee_id, PERIOD valid_at)
    -- REFERENCES employees (id, PERIOD valid_at)
  CONSTRAINT positions_pkey EXCLUDE USING gist (id WITH =, valid_at WITH &&)
);
CREATE INDEX idx_positions_on_employee_id ON positions USING gist (employee_id, valid_at);

-- Add some employees
INSERT INTO employees (valid_at, name, salary)
  SELECT  -- employees have been with the company 1-20 years:
          daterange(current_date - 365*(random() * 20)::int, null),
          (ARRAY['Joe', 'Fred', 'Sue', 'Carol'])[1 + s.i % 4],
          -- salary is 20-200k, in round numbers:
          1000*(20 + (random() * 180)::int)
  FROM    generate_series(1, 10000) s(i);

-- Every employee gets a 2% raise every 1-3 years, up 'til today:
-- Since we don't have FOR PORTION OF,
-- for each employee:
-- loop starting from their hire date until we reach today:
-- (1) Add 1-3 years.
-- (2) If it's after today, do quit looping. Otherwise...
-- (3) Set the last record's end time.
-- (4) Add a new record starting on that date with a 2% raise and a null end time.
-- It's tempting to use a recursive CTE,
-- but we need to avoid reconsidering the same record over & over, since it may still end in NULL.
-- The easiest way is to process each employee separately, so we can stop looping when we reach today.
DO
$$
DECLARE
  emp_id BIGINT;
  n TEXT;
  s INT;
  t DATE;
BEGIN
  FOR emp_id, n, s, t IN SELECT id, name, salary, lower(valid_at) FROM employees LOOP
    LOOP
      t := t + 365*(1 + (random() * 3))::int;
      IF t >= current_date THEN
        EXIT;
      END IF;

      s = (s * 1.02)::int; -- 2% raise woohoo!

      -- close the currently-open record:
      UPDATE  employees
      SET     valid_at = daterange(lower(valid_at), t)
      WHERE   id = emp_id AND valid_at @> 'Infinity'::date;

      -- start a new one with the raise:
      INSERT INTO employees (id, valid_at, name, salary)
      VALUES (emp_id, daterange(t, null), n, s);
    END LOOP;
  END LOOP;
END
$$;

-- Now for each employee, give them some positions, spread across their tenure.
-- This has the same pattern as above:
-- For each employee,
-- Set s to their hire date.
-- Now loop:
-- (1) Set e to s + 1-3 years.
-- (2) If e is after today, set it to null instead.
-- (3) 1% chance that the employee has no position during this period, so that antijoin has something to find.
-- (4) Add a position from s to e.
-- (5) If e is null, break.
-- (6) s := e.
DO
$$
DECLARE
  emp_id BIGINT;
  duty TEXT;
  rank INT;
  s DATE;
  e DATE;
BEGIN
  FOR emp_id, s IN SELECT DISTINCT ON (id) id, lower(valid_at) FROM employees ORDER BY id, valid_at LOOP
    duty = (ARRAY['Janitor', 'Dishwasher', 'Peon', 'Gopher'])[1 + (4*random())::int];
    rank := 1;

    LOOP
      e := s + 365*(1 + (random() * 3))::int;
      IF e >= current_date THEN
        e := NULL;
      END IF;

      -- 1% chance the employee had no position for a while:
      IF random() > 0.01 THEN
        INSERT INTO positions (valid_at, name, employee_id)
        VALUES (daterange(s, e), concat(duty, ' ', to_char(rank, 'RN')), emp_id);
      END IF;

      IF e IS NULL THEN
        EXIT;
      END IF;

      rank := rank + 1;
      s := e;
    END LOOP;
  END LOOP;
END
$$;

ANALYZE employees;
ANALYZE positions;

/*

-- RESULTS:


--
-- SEMIJOIN
--

-- With LATERAL:

paul=# explain analyze select e.id, j.valid_at
from employees e
join lateral (
  select unnest(multirange(e.valid_at) * range_agg(p.valid_at)) as valid_at
  from positions p
  where e.id = p.employee_id
  and e.valid_at && p.valid_at
  group by p.employee_id
) as j on true;
                                                                              QUERY PLAN                                                                              
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Nested Loop  (cost=0.28..302608.82 rows=4418900 width=40) (actual time=2.519..274.167 rows=43964 loops=1)
   ->  Seq Scan on employees e  (cost=0.00..1018.89 rows=44189 width=21) (actual time=0.007..2.485 rows=44189 loops=1)
   ->  ProjectSet  (cost=0.28..4.83 rows=100 width=40) (actual time=0.006..0.006 rows=1 loops=44189)
         ->  GroupAggregate  (cost=0.28..4.31 rows=1 width=40) (actual time=0.005..0.005 rows=1 loops=44189)
               ->  Index Only Scan using idx_positions_on_employee_id on positions p  (cost=0.28..4.30 rows=1 width=21) (actual time=0.005..0.005 rows=1 loops=44189)
                     Index Cond: ((employee_id = e.id) AND (valid_at && e.valid_at))
                     Heap Fetches: 0
 Planning Time: 0.126 ms
 JIT:
   Functions: 9
   Options: Inlining false, Optimization false, Expressions true, Deforming true
   Timing: Generation 0.366 ms, Inlining 0.000 ms, Optimization 0.149 ms, Emission 2.331 ms, Total 2.845 ms
 Execution Time: 275.630 ms
(13 rows)

-- With GROUP BY (predicate is pushed down and result is memoized):

paul=# explain analyze select e.id, unnest(multirange(e.valid_at) * j.valid_at) AS valid_at
from employees e
join (
  select p.employee_id, range_agg(p.valid_at) as valid_at
  from positions p
  group by p.employee_id
) as j on e.id = j.employee_id and e.valid_at && j.valid_at;
                                                                  QUERY PLAN                                                                  
----------------------------------------------------------------------------------------------------------------------------------------------
 ProjectSet  (cost=4878.37..6127.03 rows=22200 width=40) (actual time=22.842..52.430 rows=43964 loops=1)
   ->  Hash Join  (cost=4878.37..6013.26 rows=222 width=53) (actual time=22.838..33.757 rows=43947 loops=1)
         Hash Cond: (e.id = j.employee_id)
         Join Filter: (e.valid_at && j.valid_at)
         Rows Removed by Join Filter: 226
         ->  Seq Scan on employees e  (cost=0.00..1018.89 rows=44189 width=21) (actual time=0.008..2.177 rows=44189 loops=1)
         ->  Hash  (cost=4758.85..4758.85 rows=9562 width=40) (actual time=22.818..22.818 rows=9990 loops=1)
               Buckets: 16384  Batches: 1  Memory Usage: 755kB
               ->  Subquery Scan on j  (cost=4031.51..4758.85 rows=9562 width=40) (actual time=11.783..22.022 rows=9990 loops=1)
                     ->  HashAggregate  (cost=4031.51..4663.23 rows=9562 width=40) (actual time=11.783..21.519 rows=9990 loops=1)
                           Group Key: p.employee_id
                           Planned Partitions: 16  Batches: 17  Memory Usage: 705kB  Disk Usage: 2984kB
                           ->  Seq Scan on positions p  (cost=0.00..890.07 rows=43707 width=21) (actual time=0.003..2.106 rows=43707 loops=1)
 Planning Time: 0.133 ms
 Execution Time: 54.127 ms
(15 rows)


--
-- ANTIJOIN
--

-- With LATERAL:

paul=# explain analyze SELECT  e.id, e.name, e.salary, e.valid_at AS emp_valid_at,
        UNNEST(CASE WHEN j.valid_at IS NULL THEN multirange(e.valid_at)
                    ELSE multirange(e.valid_at) - j.valid_at END) AS valid_at
FROM    employees e
LEFT JOIN LATERAL (
  SELECT  p.employee_id, range_agg(p.valid_at) AS valid_at
  FROM    positions p
  WHERE e.id = p.employee_id AND e.valid_at && p.valid_at
  GROUP BY p.employee_id
) AS j ON true
WHERE   NOT isempty(e.valid_at);
                                                                              QUERY PLAN                                                                              
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
 ProjectSet  (cost=0.28..143931.86 rows=2945900 width=61) (actual time=4.536..270.218 rows=606 loops=1)
   ->  Nested Loop Left Join  (cost=0.28..128760.48 rows=29459 width=61) (actual time=3.539..253.304 rows=44189 loops=1)
         ->  Seq Scan on employees e  (cost=0.00..1129.36 rows=29459 width=29) (actual time=3.504..8.295 rows=44189 loops=1)
               Filter: (NOT isempty(valid_at))
         ->  GroupAggregate  (cost=0.28..4.31 rows=1 width=40) (actual time=0.005..0.005 rows=1 loops=44189)
               ->  Index Only Scan using idx_positions_on_employee_id on positions p  (cost=0.28..4.30 rows=1 width=21) (actual time=0.005..0.005 rows=1 loops=44189)
                     Index Cond: ((employee_id = e.id) AND (valid_at && e.valid_at))
                     Heap Fetches: 0
 Planning Time: 0.130 ms
 JIT:
   Functions: 14
   Options: Inlining false, Optimization false, Expressions true, Deforming true
   Timing: Generation 0.523 ms, Inlining 0.000 ms, Optimization 0.169 ms, Emission 3.338 ms, Total 4.030 ms
 Execution Time: 270.826 ms
(14 rows)


-- With GROUP BY (predicate is pushed down and result is memoized):

paul=# explain analyze SELECT  e.id, e.name, e.salary, e.valid_at AS emp_valid_at,
        UNNEST(CASE WHEN j.valid_at IS NULL THEN multirange(e.valid_at)
                    ELSE multirange(e.valid_at) - j.valid_at END) AS valid_at
FROM    employees e
LEFT JOIN (
  SELECT  p.employee_id, range_agg(p.valid_at) AS valid_at
  FROM    positions p
  GROUP BY p.employee_id
) AS j ON e.id = j.employee_id AND e.valid_at && j.valid_at
WHERE   NOT isempty(e.valid_at);
                                                                  QUERY PLAN                                                                  
----------------------------------------------------------------------------------------------------------------------------------------------
 ProjectSet  (cost=4878.37..21256.45 rows=2945900 width=61) (actual time=22.419..48.705 rows=606 loops=1)
   ->  Hash Left Join  (cost=4878.37..6085.06 rows=29459 width=61) (actual time=22.300..34.292 rows=44189 loops=1)
         Hash Cond: (e.id = j.employee_id)
         Join Filter: (e.valid_at && j.valid_at)
         Rows Removed by Join Filter: 226
         ->  Seq Scan on employees e  (cost=0.00..1129.36 rows=29459 width=29) (actual time=0.011..3.797 rows=44189 loops=1)
               Filter: (NOT isempty(valid_at))
         ->  Hash  (cost=4758.85..4758.85 rows=9562 width=40) (actual time=22.275..22.275 rows=9990 loops=1)
               Buckets: 16384  Batches: 1  Memory Usage: 755kB
               ->  Subquery Scan on j  (cost=4031.51..4758.85 rows=9562 width=40) (actual time=11.444..21.515 rows=9990 loops=1)
                     ->  HashAggregate  (cost=4031.51..4663.23 rows=9562 width=40) (actual time=11.443..21.010 rows=9990 loops=1)
                           Group Key: p.employee_id
                           Planned Partitions: 16  Batches: 17  Memory Usage: 705kB  Disk Usage: 2984kB
                           ->  Seq Scan on positions p  (cost=0.00..890.07 rows=43707 width=21) (actual time=0.003..2.093 rows=43707 loops=1)
 Planning Time: 0.160 ms
 Execution Time: 49.321 ms
(16 rows)

-- So LATERAL is much worse for both operations.


-- Now here are the same queries for just one employee (not bothering with lateral joins).
-- This shows that Postgres can push the predicate down into the join and avoid GROUPing the whole table:
-- But one scary thing is that if you say `e.id = 10` instead of `e.id = 10::bigint`, you get a seqscan!
-- (Technically for semijoin you still get an index scan on one of the tables, but it's only using valid_at, not the id, so read carefully!)
-- Are they not in the same opfamily? Is the implicit cast hiding that it's a constant expression? Seems like something Postgres ought to fix, so I should investigate.

-- semijoin of employee 10:

paul=# explain analyze select e.id, unnest(multirange(e.valid_at) * j.valid_at) AS valid_at
from employees e
join (
  select p.employee_id, range_agg(p.valid_at) as valid_at
  from positions p
  group by p.employee_id
) as j on e.id = j.employee_id and e.valid_at && j.valid_at
where e.id = 10::bigint;
                                                                            QUERY PLAN                                                                            
------------------------------------------------------------------------------------------------------------------------------------------------------------------
 ProjectSet  (cost=0.56..9.20 rows=100 width=40) (actual time=0.068..0.071 rows=2 loops=1)
   ->  Nested Loop  (cost=0.56..8.69 rows=1 width=53) (actual time=0.064..0.065 rows=2 loops=1)
         ->  GroupAggregate  (cost=0.28..4.37 rows=1 width=40) (actual time=0.031..0.031 rows=1 loops=1)
               ->  Index Only Scan using idx_positions_on_employee_id on positions p  (cost=0.28..4.35 rows=4 width=21) (actual time=0.024..0.024 rows=2 loops=1)
                     Index Cond: (employee_id = '10'::bigint)
                     Heap Fetches: 0
         ->  Index Only Scan using employees_pkey on employees e  (cost=0.28..4.30 rows=1 width=21) (actual time=0.031..0.032 rows=2 loops=1)
               Index Cond: ((id = '10'::bigint) AND (valid_at && (range_agg(p.valid_at))))
               Heap Fetches: 0
 Planning Time: 0.133 ms
 Execution Time: 0.096 ms
(11 rows)


-- anitjoin of employee 10:
paul=# explain analyze SELECT  e.id, e.name, e.salary, e.valid_at AS emp_valid_at,
        UNNEST(CASE WHEN j.valid_at IS NULL THEN multirange(e.valid_at)
                    ELSE multirange(e.valid_at) - j.valid_at END) AS valid_at
FROM    employees e
LEFT JOIN (
  SELECT  p.employee_id, range_agg(p.valid_at) AS valid_at
  FROM    positions p
  GROUP BY p.employee_id
) AS j ON e.id = j.employee_id AND e.valid_at && j.valid_at
WHERE   NOT isempty(e.valid_at)
and e.id = 10::bigint;
                                                                                  QUERY PLAN                                                                                  
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 ProjectSet  (cost=4.59..28.97 rows=300 width=61) (actual time=0.063..0.064 rows=0 loops=1)
   ->  Nested Loop Left Join  (cost=4.59..27.42 rows=3 width=61) (actual time=0.055..0.058 rows=2 loops=1)
         Join Filter: (e.valid_at && j.valid_at)
         ->  Bitmap Heap Scan on employees e  (cost=4.32..22.99 rows=3 width=29) (actual time=0.022..0.024 rows=2 loops=1)
               Recheck Cond: (id = '10'::bigint)
               Filter: (NOT isempty(valid_at))
               Heap Blocks: exact=1
               ->  Bitmap Index Scan on employees_pkey  (cost=0.00..4.31 rows=5 width=0) (actual time=0.016..0.017 rows=2 loops=1)
                     Index Cond: (id = '10'::bigint)
         ->  Materialize  (cost=0.28..4.38 rows=1 width=40) (actual time=0.015..0.015 rows=1 loops=2)
               ->  Subquery Scan on j  (cost=0.28..4.38 rows=1 width=40) (actual time=0.028..0.028 rows=1 loops=1)
                     ->  GroupAggregate  (cost=0.28..4.37 rows=1 width=40) (actual time=0.027..0.028 rows=1 loops=1)
                           ->  Index Only Scan using idx_positions_on_employee_id on positions p  (cost=0.28..4.35 rows=4 width=21) (actual time=0.020..0.020 rows=2 loops=1)
                                 Index Cond: (employee_id = '10'::bigint)
                                 Heap Fetches: 0
 Planning Time: 0.176 ms
 Execution Time: 0.137 ms
(17 rows)


*/
