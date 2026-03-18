# Temporal Ops

The `temporal_ops` Postgres extension implements these temporal relational operators:

- semijoin
- antijoin
- outer join
- union (TODO)
- except (TODO)
- intersects (TODO)
- aggregate (TODO)

## Usage

We provide a [set-returning function](TODO) for each operator,
which takes the table and column name(s) for the inputs
and returns a result relation.
You can call this function inside the `FROM` clause of a query and treat its result like another table.

For example, suppose you have two tables named `employees` and `positions`,
where each position references an employee record via `employee_id`.
Then you could get a semijoin like this:

```sql
SELECT  (emp).id, (emp).name, valid_at
FROM    temporal_semijoin('employee', 'id', 'valid_at',
                          'position', 'employee_id', 'valid_at')
                          AS t(emp employee, valid_at tstzrange)
```

The function gives rows with two attributes: the first the rowtype of the left-hand input table,
the second matching that table's valid time column.
In your outer query, you can destructure the rowtype column as you like
(here, to project on `id` and `name`).
Note that because the function returns a `record` type, you must give an explicit column definition list, as above.
The types must be correct, but you can name the columns whatever you like. This can be useful if you want both `valid_at` from the input table and the result valid time.

Although the table names look like `text` parameters, they are actually [`regclass`](TODO).
This means (1) the function also accepts oids (2) Postgres resolves the table name with its normal `search_path` rules.

The functions include [support functions](TODO) so that at plan time,
Postgres will replace the function call with an equivalent subquery,
which it can inline into the outer query.
This means that quals from the outer query (e.g. `WHERE id = 5`) get pushed down into the subquery.
Otherwise the function would join *every row* of its inputs when called.

### Semijoin

There are several variations:

`temporal_semijoin(left_table regclass, left_key text, right_table regclass, right_key text)` -
Takes a single column name from each table to compare for equality, and assumes valid time is stored in columns named `valid_at`.

`temporal_semijoin(left_table regclass, left_key text, left_valid_at text, right_table regclass, right_key text, right_valid_at text)`
Takes a single column name from each table to compare for equality, and takes the names of your valid time columns.

`temporal_semijoin(left_table regclass, left_keys text[], right_table regclass, right_keys text[])`
Takes an array of column names from each table to compare for equality, and assumes valid time is stored in columns named `valid_at`. The two arrays must have the same number of elements. Each column in the left array is compared with the column in the corresponding position of the right array.

`temporal_semijoin(left_table regclass, left_keys text[], left_valid_at text, right_table regclass, right_keys text[], right_valid_at text)`
Takes an array of column names from each table to compare for equality, and takes the names of your valid time columns.

### Antijoin

There are several variations:

`temporal_antijoin(left_table regclass, left_key text, right_table regclass, right_key text)` -
Takes a single column name from each table to compare for equality, and assumes valid time is stored in columns named `valid_at`.

`temporal_antijoin(left_table regclass, left_key text, left_valid_at text, right_table regclass, right_key text, right_valid_at text)`
Takes a single column name from each table to compare for equality, and takes the names of your valid time columns.

`temporal_antijoin(left_table regclass, left_keys text[], right_table regclass, right_keys text[])`
Takes an array of column names from each table to compare for equality, and assumes valid time is stored in columns named `valid_at`. The two arrays must have the same number of elements. Each column in the left array is compared with the column in the corresponding position of the right array.

`temporal_antijoin(left_table regclass, left_keys text[], left_valid_at text, right_table regclass, right_keys text[], right_valid_at text)`
Takes an array of column names from each table to compare for equality, and takes the names of your valid time columns.

### Outer Join

There are several variations:

`temporal_outer_join(left_table regclass, left_key text, right_table regclass, right_key text)` -
Takes a single column name from each table to compare for equality, and assumes valid time is stored in columns named `valid_at`.

`temporal_outer_join(left_table regclass, left_key text, left_valid_at text, right_table regclass, right_key text, right_valid_at text)`
Takes a single column name from each table to compare for equality, and takes the names of your valid time columns.

`temporal_outer_join(left_table regclass, left_keys text[], right_table regclass, right_keys text[])`
Takes an array of column names from each table to compare for equality, and assumes valid time is stored in columns named `valid_at`. The two arrays must have the same number of elements. Each column in the left array is compared with the column in the corresponding position of the right array.

`temporal_outer_join(left_table regclass, left_keys text[], left_valid_at text, right_table regclass, right_keys text[], right_valid_at text)`
Takes an array of column names from each table to compare for equality, and takes the names of your valid time columns.

## Installation

TODO

## Theory

The gist is that while rows in an ordinary database table represent true statements,
without being very precise about *when* they are true,
a temporal table gives start & end times to say exactly when the row became true and stopped being true.
A missing bound means something like "indefinitely" or "forever".
In Postgres we can store the start & end time in a rangetype (or even multirange).
Normally this temporal information is called "application time" or "valid time".
See my [bibliography](https://illuminatedcomputing.com/TODO) for more information.

The original relational theory gave various *operators* to manipulate relations (i.e. tables).
The most important are the joins: inner join, outer join, semijoin, and antijoin.
There are also setops---union, except, intersect---and aggregates.
Then there is project (`SELECT`), select (`WHERE`), and the oft-forgotten division.

In temporal theory, these operators can be adapted to account for their inputs' valid time.
For some this is very simple: the projection of a temporal record is true for the same duration as its input.
Likewise for select.
(Techically it might be desirable to coalesce identical records if their valid times are adjacent (or if we store valid time in a multirange). But that is not the same as the operator.)
But for most operators, things are more complicated.

Take inner join: suppose that table `A` and table `B` store their valid times in `tstzrange` columns named `valid_at`.
Then the result of joining row `a` to row `b` has a valid time of `a.valid_at * b.valid_at` (where `*` stands for intersection). The join is true when *both* `a` and `b` are true.
That's still pretty easy, so we don't implement it here. (Maybe I will someday just for completeness.)

Conceptually, semijoin and antijoin are straightforward.
In `a semijoin b`, we want the result to be `a.valid_at * range_agg(b.valid_at)`, because we want `a` whenever any `b` gives a match. If we dealing with rangetypes, not multiranges, this may mean several isolated rows come from one `a` row. And if the other join conditions match (e.g. an equijoin on ids), but the valid time is empty, that `a` should not appear at all.

In `a antijoin b`, we want the result to be `a.valid_at - range_agg(b.valid_at)`: all the times from `a` with no match. Again that might give more than one result row for each `a`, or no row if the valid time is empty.

But in SQL these are harder to implement, because SQL implements semijoin and antijoin as correlated subqueries. For example:

```sql
-- semijoin in SQL, broken:
SELECT  *, a.valid_at * b.valid_at
FROM    a
WHERE EXISTS (
    SELECT  1
    FROM    b
    WHERE   a.id = b.id
    AND     a.valid_at && b.valid_at
);
```

The problem is that `b` is not in scope outside the subquery, so we can't say `a.valid_at * b.valid_at`.
Likewise for antijoin.

Outer join is also complicated.

In a temporal database, inner joins are easy (even without SQL:2011), but what about outer joins?
What about semijoins and antijoins?
What about aggregates?
What about setops (`UNION`, `INTERSECT`, and `EXCEPT`)?
This repo gives SQL to implement these temporal operations in a Postgres database.
(It is a work in progress, so not all operations are implemented yet. See below for progress.)

What makes these operations harder for temporal tables?
Each operation needs to compute the correct new application time.

Following [Date's model](https://illuminatedcomputing.com/posts/2017/12/temporal-databases-bibliography/), a temporal table could be replaced by a traditional table that just has a separate row for every second/millisecond/whatever. The application-time interval is just an optimization to avoid having a silly number of rows.
All operations should give an interval which matches the rows you'd get from that idealized table.
Nearly all of these operations are *binary*: they combine two relations.
So the result's application-time may not exactly match either input, but should be a function of them.

And the result set should include the original application-times of the joined tables, too.
That raises the question of what to call all the result set columns, since the inputs might all be named `valid_at`.

[Research by Dignös, Böhlen, and Gamper](https://www.zora.uzh.ch/id/eprint/62963/1/p433-dignos.pdf) gives a way to implement a temporal version of every relational operator. But their approach depends on new syntax as well as changes to the planner and executor. I would love to have native support for this in Postgres someday, but what does a working programmer do now? We need a SQL implementation for these operations.

As far as I know no one has published SQL for these things before.
It's not covered in Snodgass or either of Johnston's books. Date has some queries like this (for example "Q6" on page 297 is a semijoin), but his implementations are not in SQL and are hard to apply to practical databases. Boris Novikov recently published [an article showing temporal aggregates](https://www.red-gate.com/simple-talk/databases/postgresql/making-temporal-databases-work-part-2-computing-aggregates-across-temporal-versions/), which is a great step forward. I hope to incorporate his ideas here and add on to them.

This repo provides a Postgres extension to provide temporal relational operators.
Those operators are implemented as set-returning functions, e.g. `temporal_semijoin`.
You tell the function the table and column names to use, and they general a subquery to do the temporal operation.

By default Postgres cannot inline a non-SQL function, but [as of v19](https://commitfest.postgresql.org/48/5083/) it is possible to give a [support function](TODO: link to the docs) that converts the previously-opaque function call into an equivalent query plan. Then the planner can inline the subquery, enabling optimizations like qual pushdown. This extension provides such support functions wherever possible. The tests also cover hand-written SQL implementations, although calling the functions better expresses intent and is just as fast.

## Inner Joins

Inner joins are easy. You only join rows whose application time *overlaps*, and the result's application time is the input's *intersection*. For example:

```
SELECT  a.*, b.*, a.valid_at * b.valid_at AS valid_at
FROM    a
JOIN    b
ON      a.id = b.id AND a.valid_at && b.valid_at;
```


## Semijoins

A semijoin of `a` and `b` gives you every row from `a` that has a match in `b`.
This is a basic operator from the relational algrebra, but we don't have it in SQL.
We achieve them with a contortion: a predicate (`EXISTS`) containing a [correlated subquery](https://www.geeksforgeeks.org/sql-correlated-subqueries/).

A temporal semijoin is the same idea, except the resulting application-time should be only the part of `a` that matched `b`.
In other words it is `a.valid_time * range_agg(b.valid_time)`.

We can't write this as an `EXISTS`, because there is a syntax problem: what is the resulting range called? How do you `SELECT` it?
We only have access to `b.valid_at` inside the subquery. Outside it is not in scope.
So we need to do something else.

Another tricky thing is that the result's cardinality differs from regular semijoins.
In a semijoin, each left tuple produces only zero or one result tuples.
(This is one of the main reasons to use it over an inner join: to avoid producing duplicates you don't want.)
But in temporal it's not like that. For instance given:

| `a.id` | `a.valid_at` | `b.id` | `b.valid_at` |
| -----: | :----------- | -----: | :----------- |
|      1 | [1,20)       |      1 | [5,10)       |
|        |              |      1 | [15,30)      |

Then `a semijoin b` gives (using `j` for the result):

| `j.id` | `j.valid_at` |
| -----: | :----------- |
|      1 | [5,10)       |
|      1 | [15,20)      |

Here is SQL that works for *some* semijoins:

```
SELECT  a.id, a.valid_at * b.valid_at
FROM    a
JOIN    b
ON      a.id = b.id
AND     a.valid_at && b.valid_at
AND     NOT isempty(a.valid_at && b.valid_at);
```

If the left-hand side is a temporal FK referencing the right-hand side,
then that works, because we know the right-side records are (temporally) unique for each left-side record,
so they can't produce overlapping duplicates.

But if the FK points the other way, or if we're not doing an equijoin (e.g. if we're joining by `employee1.hired_at > employee2.hired_at`),
then we need some extra work to avoid duplicates:

```
SELECT  a.id, UNNEST(multirange(a.valid_at) * j.valid_at) AS valid_at
FROM    a
JOIN (
  SELECT  b.id, range_agg(b.valid_at) AS valid_at
  FROM    b
  GROUP BY b.id
) AS j
ON a.id = j.id AND a.valid_at && j.valid_at;
```

In other words, combine all the `b` ranges for each id into a multirange, find its intersection with `a`, then unnest to get back to ranges.
(This would be a lot harder without multiranges!)


## Antijoins

An antijoin of `a` and `b` gives you all rows of `a` that *don't* have a match in `b`.
(Therefore `(a join b) UNION ALL (a antijoin b)` is the same as a left outer join, if you extended the antijoin result with `NULL` columns.)
Like semijoins, antijoins have no SQL syntax.
We use a correlated subquery again, except instead of `EXISTS` we say `NOT EXISTS`.

In a temporal antijoin we want to keep parts of `a` that don't match, and throw away parts that do.
We keep the timeframe that is not "covered".
Starting from these values again:

| `a.id` | `a.valid_at` | `b.id` | `b.valid_at` |
| -----: | :----------- | -----: | :----------- |
|      1 | [1,20)       |      1 | [5,10)       |
|        |              |      1 | [15,30)      |

`a antijoin b` gives:

| `j.id` | `j.valid_at` |
| -----: | :----------- |
|      1 | [1,5)        |
|      1 | [10,15)      |

Our antijoin implementation is similar to semijoins.
We need to see all the matching `b` records before emitting a result,
so again this is a job for `range_agg`.
We need an outer join of course, because the point is to find records with no match.
Then instead of intersection we want `a.valid_at - range_agg(b.valid_at)`:

```
SELECT  a.id,
        UNNEST(CASE WHEN j.valid_at IS NULL THEN multirange(a.valid_at)
                    ELSE multirange(a.valid_at) - j.valid_at END) AS valid_at
FROM    a
LEFT JOIN (
  SELECT  b.id, range_agg(b.valid_at) AS valid_at
  FROM    b
  GROUP BY b.id
) AS j
ON a.id = j.id AND a.valid_at && j.valid_at
WHERE   NOT isempty(a.valid_at);
```

If no `b` matches were found, we subtract nothing, so we just keep `a.valid_at`.
Here I originally wanted to say `multirange(a.valid_at) - COALESCE(j.valid_at, 'empty')`,
but Postgres needs a specific type for `empty`, so using `CASE` keeps our SQL type-agnostic.

And if `a.valid_at` started out empty, we should just throw it away.
If there is a temporal PK on that column, empty should be forbidden anyway.

## Left Outer Joins

A left outer join gives you everything from `a` joined to `b`, except when there is no match it keeps the row from `a` and fills the `b` side with nulls.
(A right outer join is the same but from the other side. It's almost always easy to convert right joins to left, so I won't cover it here.)

In a temporal left join, we need to slice up the `a` side's application-time so it matches what we find in the matches from `b`.
Then we can fill in the gaps with `NULL`s.
There is a great example of this in the Dignös paper above.

Here are lots of left join implementations:

This first version uses the equivalence above, that a left join is an inner join plus an antijoin:

```
SELECT  a.*, b.*,
        UNNEST(multirange(a.valid_at) * multirange(b.valid_at)) AS valid_at
FROM    a
JOIN    b
ON      a.id = b.id AND a.valid_at && b.valid_at
UNION ALL
SELECT  a.*, (NULL::b).*,
        UNNEST(
          CASE WHEN j.valid_at IS NULL
               THEN multirange(a.valid_at)
               ELSE multirange(a.valid_at) - j.valid_at END
        )
FROM    a
LEFT JOIN (
  SELECT  b.id, range_agg(b.valid_at) AS valid_at
  FROM    b
  GROUP BY b.id
) AS j
ON      a.id = j.id AND a.valid_at && j.valid_at;
```

I like that version because it is simple to understand, but notice it has to scan `a` twice.
We can probably find something that performs better.

The tricky part for me was avoiding duplicates in the unmatched portion if there are several rows in `b` that match.
In many of my attempts, for every match in `b` I emitted the unmatched portion too, which is wrong.
We need to combine all the matches in `b` before emitting an unmatched portion,
but *not* combine them for emitting the matching parts.

I finally got a correct result with this:

```
SELECT  a.*, (j2.u).*,
        COALESCE(j2.valid_at, a.valid_at) AS valid_at
FROM    a
LEFT JOIN (
  SELECT  b.id, range_agg(b.valid_at) AS valid_at, array_agg(b) AS bs
  FROM    b
  GROUP BY b.id
) AS j
ON      a.id = j.id AND a.valid_at && j.valid_at
LEFT JOIN LATERAL (
  SELECT  u, a.valid_at * u.valid_at AS valid_at
  FROM    UNNEST(j.bs) AS u
  WHERE   NOT isempty(a.valid_at * u.valid_at)
  UNION ALL
  SELECT  NULL, u.valid_at
  FROM    UNNEST(multirange(a.valid_at) - j.valid_at) AS u(valid_at)
  WHERE   NOT isempty(u.valid_at)
) AS j2
ON      true
WHERE   NOT isempty(a.valid_at);
```

Here are two more implementations, written by Boris Novikov.
The first scans `a` twice:

```
SELECT  a.*, b.*, a.valid_at * COALESCE(b.valid_at, a.valid_at) AS valid_at
FROM    a
LEFT JOIN b
ON      a.id = b.id AND a.valid_at && b.valid_at
WHERE   NOT isempty(a.valid_at)
UNION ALL (
SELECT  a.*, (NULL::b).*, UNNEST(multirange(a.valid_at) - range_agg(b.valid_at)) AS valid_at
FROM    a
JOIN    b
ON      a.id = b.id AND a.valid_at && b.valid_at
WHERE   NOT isempty(a.valid_at)
GROUP BY a.id, a.valid_at
);
```

But this one does it in one scan. I expect it is the fastest:

```
SELECT  (j1.a).*, (j2.b).*, j2.valid_at
FROM    (
  SELECT  a, array_agg(ROW(b, COALESCE(a.valid_at * b.valid_at, a.valid_at))) AS bs,
          range_agg(b.valid_at) AS bs_valid_at
  FROM    a
  LEFT JOIN b
  ON      a.id = b.id AND a.valid_at && b.valid_at
  GROUP BY a
) AS j1
JOIN LATERAL (
  SELECT bs.b, valid_at FROM UNNEST(j1.bs) AS bs(b b, valid_at int4range)
  UNION ALL
  SELECT NULL, UNNEST(multirange((j1.a).valid_at) - j1.bs_valid_at)
) AS j2 ON true
WHERE   NOT isempty((j1.a).valid_at);
```

All these queries have some empty checks like `WHERE NOT isempty(a.valid_at)`, but if you have a temporal primary key you can omit those, since empty is already forbidden.


## Aggregates

TODO

## UNION and UNION ALL

TODO

## INTERSECT

TODO

## EXCEPT

TODO

# Performance

See [bench.sql](bench.sql) for some performance experiments.
Highlights here:

My first version of semijoin/antijoin used `LATERAL` joins, which was a lot slower.
I was worried that doing `GROUP BY` in a subquery with no filters would make Postgres process the whole table,
even when the top-level is doing something like `WHERE a.id = 10`.
Turns out Postgres is smart enough to push that condition into the subquery.
I should have trusted the planner!

Now that we have support functions to inline dynamically-constructed queries,
I should make some new benchmarks. Stay tuned. . . .

# Acknowledgements

Many thanks to Boris Novikov and Hettie Dombrovskaya for inspiring this work,
and for all their contributions to temporal data over the years.
In our correspondence they suggested or improved many of these ideas.
You should check out Hettie's [`pg_bitemporal` extension](https://github.com/hettie-d/pg_bitemporal)
and Boris's [writing on temporal aggregates](https://www.red-gate.com/simple-talk/databases/postgresql/making-temporal-databases-work-part-2-computing-aggregates-across-temporal-versions/).
