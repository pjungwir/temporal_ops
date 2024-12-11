SELECT  a.id, UNNEST(multirange(a.valid_at) * COALESCE(b2.valid_at, '{}')) AS valid_at
FROM    a
JOIN (
  SELECT  b.id, range_agg(b.valid_at) as valid_at
  FROM    b
  GROUP BY b.id
) as b2 ON a.id = b2.id AND a.valid_at && b2.valid_at
WHERE   multirange(a.valid_at) * COALESCE(b2.valid_at, '{}') IS DISTINCT FROM '{}'
ORDER BY a.id, valid_at
;
