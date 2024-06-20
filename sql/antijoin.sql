SELECT  a.id, j.id,
        UNNEST(CASE WHEN j.valid_at IS NULL THEN multirange(a.valid_at)
                    ELSE multirange(a.valid_at) - j.valid_at END) AS valid_at
FROM    a
LEFT JOIN LATERAL (
  SELECT  b.id, range_agg(b.valid_at) AS valid_at
  FROM    b
  WHERE   a.id = b.id
  AND     a.valid_at && b.valid_at
  GROUP BY b.id
) AS j ON true
WHERE   NOT isempty(a.valid_at);
