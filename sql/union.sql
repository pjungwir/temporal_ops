SELECT  id, UNNEST(range_agg(valid_at)) AS valid_at
FROM  (
  SELECT * FROM a
  UNION
  SELECT * FROM b
) x
GROUP BY id
ORDER BY id, valid_at;

SELECT  id, UNNEST(range_agg(valid_at)) AS valid_at
FROM  (
  SELECT * FROM a2 as a
  UNION
  SELECT * FROM b2 as b
) x
GROUP BY id
ORDER BY id, valid_at;
