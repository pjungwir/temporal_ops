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
For semijoin you want `a.valid_at * b.valid_at`; for antijoin, `a.valid_at - range_agg(b.valid_at)`.
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

But can we get what we want with a regular inner/outer join plus a set-returning function (SRF)?
This works for semijoins:

```
SELECT  a.id, b.id, j.valid_at
FROM    a
JOIN    b
ON      a.id = b.id
AND     a.valid_at && b.valid_at
JOIN LATERAL temporal_semijoin(a.valid_at, b.valid_at) AS j(valid_at)
ON      true
```

In that query, `temporal_semijoin` just returns `a * b`.

For antijoins things are trickier, because we need to see all the matching `b` records
before emitting a result. This feels like a job for `range_agg`.
And I guess an outer join will help here, because we want to see everything from `a` then drop some of it.
And then if the original inputs are ranges we need to `UNNEST` the resulting multirange.
(We could skip this last part if the inputs were multiranges.)
If there was no match from `b` at all, then we should just preserve `a`'s range.

```
SELECT  a.id, j.id, COALESCE(j.valid_at, a.valid_at) AS valid_at
FROM    a
LEFT JOIN LATERAL (
  SELECT  b.id, UNNEST(multirange(a.valid_at) - range_agg(b.valid_at)) AS valid_at
  FROM    b
  WHERE   a.id = b.id
  AND     a.valid_at && b.valid_at
  GROUP BY b.id
) AS j ON true;
```

I don't see a nicer way to wrap this up in a function yet.

Many thanks to Boris and Hettie for inspiring this work!
Most of the ideas are their own; I just wrote the semijoin+antijoin SQL.

# TODO

- Make sure these give sane query plans for large tables.
- Temporal outer joins.

