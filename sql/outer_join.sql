-- Temporal outer join is equivalent to temporal inner join plus temporal antijoin.
-- But it feels like we should be able to do this in one pass of a instead of two.
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
ON      a.id = j.id AND a.valid_at && j.valid_at
ORDER BY 1, 5
;

-- This works too, probably a better plan:
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
WHERE   NOT isempty(a.valid_at)
ORDER BY a.id, COALESCE(j2.valid_at, a.valid_at);

-- A version from Boris.
-- This one also scans a twice.
SELECT  (x.a).*, (x.b).*, valid_at
FROM    (
  SELECT  a, b, a.valid_at * COALESCE(b.valid_at, a.valid_at) AS valid_at
  FROM    a
  LEFT JOIN b
  ON      a.id = b.id AND a.valid_at && b.valid_at
  WHERE   NOT isempty(a.valid_at)
  UNION ALL (
  SELECT  a, NULL, UNNEST(multirange(a.valid_at) - range_agg(b.valid_at)) AS valid_at
  FROM    a
  JOIN    b
  ON      a.id = b.id AND a.valid_at && b.valid_at
  WHERE   NOT isempty(a.valid_at)
  GROUP BY a, a.valid_at
  )
) x
ORDER BY 1, 5;

-- Another version from Boris, scanning a just once:
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
WHERE   NOT isempty((j1.a).valid_at)
ORDER BY 1, 5;
