SELECT  a.id, j.id, j.valid_at
FROM    a
JOIN LATERAL (
  SELECT  b.id, UNNEST(multirange(a.valid_at) * range_agg(b.valid_at)) AS valid_at
  FROM    b
  WHERE   a.id = b.id
  AND     a.valid_at && b.valid_at
  GROUP BY b.id
) AS j ON true;

-- If it is an equijoin and the FK is on the left side
-- (thus the right side is unique),
-- we can simplify to this:
SELECT  a.id, b.id, a.valid_at * b.valid_at
FROM    a
JOIN    b
ON      a.id = b.id
AND     a.valid_at && b.valid_at
AND     NOT isempty(a.valid_at * b.valid_at)
WHERE   a.id IS DISTINCT FROM 6;
