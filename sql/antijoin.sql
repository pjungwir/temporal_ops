SELECT  a.id,
        UNNEST(CASE WHEN j.valid_at IS NULL THEN multirange(a.valid_at)
                    ELSE multirange(a.valid_at) - j.valid_at END) AS valid_at
FROM    a
LEFT JOIN (
  SELECT  b.id, range_agg(b.valid_at) AS valid_at
  FROM    b
  GROUP BY b.id
) AS j
ON a.id = j.id AND a.valid_at && j.valid_at
WHERE   NOT isempty(a.valid_at);

-- Test with our function:
SELECT	*
FROM		temporal_antijoin('a', 'id', 'valid_at', 'b', 'id', 'valid_at') AS t(id int, valid_at int4range);

-- Qual is pushed down:
INSERT INTO a SELECT 10, int4range(i, i+1) FROM generate_series(1,1000) s(i);
CREATE INDEX idx_a_id ON a (id);
CREATE INDEX idx_b_id ON b (id);
ANALYZE a, b;

EXPLAIN SELECT *
FROM		temporal_antijoin('a', 'id', 'valid_at', 'b', 'id', 'valid_at') AS t(id int, valid_at int4range)
WHERE   id = 1;

DROP INDEX idx_a_id;
DROP INDEX idx_b_id;
DELETE FROM a WHERE id = 10;

-- Without the support function:
CREATE OR REPLACE FUNCTION temporal_antijoin_support(INTERNAL)
RETURNS INTERNAL
AS 'temporal_ops', 'noop_support'
LANGUAGE C;
SELECT	*
FROM		temporal_antijoin('a', 'id', 'valid_at', 'b', 'id', 'valid_at') AS t(id int, valid_at int4range);
