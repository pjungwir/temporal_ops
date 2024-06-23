SELECT  a.id, UNNEST(multirange(a.valid_at) * j.valid_at) AS valid_at
FROM    a
JOIN (
  SELECT  b.id, range_agg(b.valid_at) AS valid_at
  FROM    b
  GROUP BY b.id
) AS j
ON a.id = j.id AND a.valid_at && j.valid_at;

-- If it is an equijoin and the FK is on the left side
-- (thus the right side is unique),
-- we can simplify to this:
SELECT  a.id, a.valid_at * b.valid_at
FROM    a
JOIN    b
ON      a.id = b.id
AND     a.valid_at && b.valid_at
AND     NOT isempty(a.valid_at * b.valid_at)
WHERE   a.id IS DISTINCT FROM 6;

-- Test with our function:
SELECT	*
FROM		temporal_semijoin('a', 'id', 'valid_at', 'b', 'id', 'valid_at') AS t(id int, valid_at int4range);

-- Qual is pushed down:
INSERT INTO a SELECT 10, int4range(i, i+1) FROM generate_series(1,1000) s(i);
CREATE INDEX idx_a_id ON a (id);
CREATE INDEX idx_b_id ON b (id);
ANALYZE a, b;

EXPLAIN SELECT *
FROM		temporal_semijoin('a', 'id', 'valid_at', 'b', 'id', 'valid_at') AS t(id int, valid_at int4range)
WHERE   id = 1;

DROP INDEX idx_a_id;
DROP INDEX idx_b_id;
DELETE FROM a WHERE id = 10;
