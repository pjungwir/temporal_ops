# Temporal Ops

In a temporal database, inner joins are easy (even without SQL:2011), but what about outer joins?
What about semijoins and antijoins?
What about aggregates?
What about `UNION`, `INTERSECT`, `EXCEPT`?
This repo gives SQL to implement these temporal operations in a Postgres database.
(It is a work in progress, so not all operations are implemented yet. See below for progress.)

What makes these operations harder for temporal tables?
Each operation needs to compute the correct new application time.
Following [Date's model](https://illuminatedcomputing.com/posts/2017/12/temporal-databases-bibliography/), a temporal table could be replaced by a traditional table that just has a separate row for every second/millisecond/whatever. The application-time interval is just an optimization to avoid having a silly number of rows.
All operations should give an interval which matches the rows you'd get from that idealized table.
Nearly all of these operations are *binary*: they combine two relations. So the result's application-time may not exactly match either input, but should be a function of them.

[Research by Dignös, Böhlen, and Gamper](https://www.zora.uzh.ch/id/eprint/62963/1/p433-dignos.pdf) gives a way to implement a temporal version of every relational operator. But their approach depends on new syntax as well as changes to the planner and executor. I would love to have native support for this in Postgres someday, but what does a working programmer do now? We need a SQL implementation for these operations.
As far as I know no one has published SQL for these things before.
It's not covered in Snodgass or either of Johnston's books. Date has some queries like this (for example "Q6" on page 297 is a semijoin), but his implementations are not in SQL and are hard to apply to practical databases. Boris Novikov recently published [an article showing temporal aggregates](https://www.red-gate.com/simple-talk/databases/postgresql/making-temporal-databases-work-part-2-computing-aggregates-across-temporal-versions/), which is a great step forward. I hope to incorporate his ideas here and add on to them.

This repo is structured like a Postgres extension you could install, but there is nothing in it.
I'm just documenting the shape of queries you'd write yourself.
Having the extension infrastructure is convenient for writing tests,
but really I'm hoping I can eventually wrap these queries in functions to encapsulate things.
Today Postgres isn't able to inline plpgsql functions, so you'll get worse performance when combining them with the rest of your query.
For example a semijoin function would compute its result for your entire table, even when you want just one record.
I'm working on a [patch](https://commitfest.postgresql.org/48/5083/) to allow inlining plpgsql functions, but it's pretty new.
The [inlined branch here](https://github.com/pjungwir/temporal_ops/tree/inlined) shows how I'd use that proposed functionality.

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

I also looked at encapsulating these queries inside a Set-Returning Function (SRF).
My worry was that functions are opaque to the planner,
so that Postgres would no longer push down conditions like that.
If it's PLPGSQL, that that's true.

But if the function is SQL (thus with hardcoded table & column names),
then Postgres can [inline it](https://wiki.postgresql.org/wiki/Inlining_of_SQL_functions),
essentially turning it into a subquery,
and then the same optimizations happen.

It'd be cool if we could get the same inlining from a function that dynamically generates the SQL.
When you define a function, you can attach a [`SUPPORT` function](https://www.postgresql.org/docs/current/xfunc-optimization.html)
and one thing it can do is replace the function Node with a different Node tree (`SupportRequestSimplify`).
That means you can write functions that act like macros!
I [tried](https://github.com/pjungwir/temporal_ops/tree/inlined) using that to return a `Query`, but I got `ERROR:  unrecognized node type: 58`.
This feature is really intended for constant-substitution,
so I don't think anyone has considered using it in place of a SRF.
It doesn't seem hard to patch Postgres to allow that though.
(Probably I would need to implement this in `inline_set_returning_function`,
not in `simplify_function`, because `RangeTblEntry.functions` needs to be a `List` of `RangeTblFunction`s.
And then maybe it should be a new kind of Support Request, e.g. `SupportRequestInlineSRF`,
so that we call it in the right place.)

# Acknowledgements

Many thanks to Boris Novikov and Hettie Dombrovskaya for inspiring this work,
and for all their contributions to temporal data over the years.
In our correspondence they suggested or improved many of these ideas.
You should check out Hettie's [`pg_bitemporal` extension](https://github.com/hettie-d/pg_bitemporal)
and Boris's [writing on temporal aggregates](https://www.red-gate.com/simple-talk/databases/postgresql/making-temporal-databases-work-part-2-computing-aggregates-across-temporal-versions/).
