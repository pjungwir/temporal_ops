SELECT  id, UNNEST(range_agg(valid_at)) AS valid_at
FROM  (
  SELECT * FROM a
  UNION
  SELECT * FROM b
) x
GROUP BY id
ORDER BY id, valid_at;
