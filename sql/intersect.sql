SELECT  a.id, UNNEST(COALESCE(a.valid_at, '{}') * COALESCE(b.valid_at, '{}')) AS valid_at
FROM (
  SELECT  a.id, range_agg(a.valid_at) as valid_at
  FROM    a
  GROUP BY a.id
) as a
JOIN (
  SELECT  b.id, range_agg(b.valid_at) as valid_at
  FROM    b
  GROUP BY b.id
) as b ON a.id = b.id AND a.valid_at && b.valid_at
WHERE   COALESCE(a.valid_at, '{}') * COALESCE(b.valid_at, '{}') IS DISTINCT FROM '{}'
ORDER BY a.id, valid_at
;

SELECT  a.id, UNNEST(COALESCE(a.valid_at, '{}') * COALESCE(b.valid_at, '{}')) AS valid_at
FROM (
  SELECT  a.id, range_agg(a.valid_at) as valid_at
  FROM    a2 as a
  GROUP BY a.id
) as a
JOIN (
  SELECT  b.id, range_agg(b.valid_at) as valid_at
  FROM    b2 as b
  GROUP BY b.id
) as b ON a.id = b.id AND a.valid_at && b.valid_at
WHERE   COALESCE(a.valid_at, '{}') * COALESCE(b.valid_at, '{}') IS DISTINCT FROM '{}'
ORDER BY a.id, valid_at
;
