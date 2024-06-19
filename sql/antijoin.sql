SELECT  a.id, j.id, COALESCE(j.valid_at, a.valid_at) AS valid_at
FROM    a
LEFT JOIN LATERAL (
  SELECT  b.id, UNNEST(multirange(a.valid_at) - range_agg(b.valid_at)) AS valid_at
  FROM    b
  WHERE   a.id = b.id
  AND     a.valid_at && b.valid_at
  GROUP BY b.id
) AS j ON true;
