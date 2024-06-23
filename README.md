# Temporal Ops

In a temporal database, inner joins are easy (even without SQL:2011), but what about outer joins?
What about semijoins and antijoins?
What about aggregates?
What about `UNION`, `INTERSECT`, `EXCEPT`?

First let's deal with just semijoins and antijoins.

[Research by Dignös, Böhlen, and Gamper](https://www.zora.uzh.ch/id/eprint/62963/1/p433-dignos.pdf) gives a way to implement those (and in fact all of the above),
but when I talked with [Hettie Dombrovskaya](https://github.com/hettie-d/) and Boris Novikov, they were concerned about the performance.
If you have to run `ALIGN` on the full tables before applying the join predicates, it will never perform well.
Ideally you would do the extra processing as part of the same semijoin/antijoin/whatever executor node,
so that the planner does everything it would normally do to filter irrelevant rows early.
I would have to look at those researchers' patch submission again to see if their concern is correct.
But in any case, implementing things in the existing executor nodes does seem like the right idea.

There is a syntax problem though: what is the resulting range called? How do you `SELECT` it?
In semi/anti joins, you want not the range from table `a` and not the range from table `b`, but a result of combining them somehow.
For semijoin you want `a.valid_at * range_agg(b.valid_at)`; for antijoin, `a.valid_at - range_agg(b.valid_at)`.
But in SQL, those joins have no syntax of their own.
You achieve them with a contortion: a predicate (`EXISTS` or `NOT EXISTS`) containing a ["correlated sub-query"](https://www.geeksforgeeks.org/sql-correlated-subqueries/).
So the new resulting range is never given a name.

Another tricky thing is that the result's cardinality differs from regular semi/anti joins.
In a semijoin, each left tuple produces only zero or one result tuples.
(This is one of the main reasons to use it over an inner join: to avoid producing duplicates you don't want.)
Same thing in an antijoin.
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

And `a antijoin b` gives:

| `j.id` | `j.valid_at` |
| -----: | :----------- |
|      1 | [1,5)        |
|      1 | [10,15)      |

How do you `SELECT j.valid_at`? The correlated sub-query doesn't have a name, and all you produce from `EXISTS` is true or false.

But can we get what we want with a regular inner/outer join plus some extra work?
This works for *some* semijoins:

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

Our antijoin approach is similar.
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

Neither of these operations are easy to wrap up in a function.
Perhaps you could take the table names and column names and generate SQL.
It could introspect for FKs and use the simpler query above when possible.
But a function is opaque to the optimizer,
so you wouldn't get filtering from other predicates in the query.
For now I'm happy with just documenting how to do it yourself.

Many thanks to Boris and Hettie for inspiring this work!
Most of the ideas are their own; I just wrote the semijoin+antijoin SQL.

# Performance

See [bench.sql](bench.sql) for some performance experiments.
Highlights here:

My first version of semijoin/antijoin used `LATERAL` joins, which was a lot slower.
I was worried that doing `GROUP BY` in a sub-query with no filters would make Postgres process the whole table,
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


# TODO

- Temporal outer joins.
- Temporal aggregates.
- Temporal `UNION`.
- Temporal `INTERSECT`.
- Temporal `EXCEPT`.

