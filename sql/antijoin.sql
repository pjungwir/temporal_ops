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
SELECT	(t.a).*, valid_at
FROM		temporal_antijoin('a', 'id', 'valid_at', 'b', 'id', 'valid_at') AS t(a a, valid_at int4range);

-- Test with our text[] function:
SELECT	(t.a).*, valid_at
FROM		temporal_antijoin('a', array['id'], 'valid_at', 'b', array['id'], 'valid_at') AS t(a a, valid_at int4range);

-- Test with single-key implicit valid_at function:
SELECT	(t.a).*, valid_at
FROM		temporal_antijoin('a', 'id', 'b', 'id') AS t(a a, valid_at int4range);

-- Test with multi-key implicit valid_at function:
SELECT	(t.a).*, valid_at
FROM		temporal_antijoin('a', array['id'], 'b', array['id']) AS t(a a, valid_at int4range);

-- Qual is pushed down:
INSERT INTO a SELECT 10, int4range(i, i+1) FROM generate_series(1,1000) s(i);
CREATE INDEX idx_a_id ON a (id);
CREATE INDEX idx_b_id ON b (id);
ANALYZE a, b;

EXPLAIN SELECT (t.a).*, valid_at
FROM		temporal_antijoin('a', 'id', 'valid_at', 'b', 'id', 'valid_at') AS t(a a, valid_at int4range)
WHERE   (t.a).id = 1;

DROP INDEX idx_a_id;
DROP INDEX idx_b_id;
DELETE FROM a WHERE id = 10;

-- Without the support function:
CREATE OR REPLACE FUNCTION temporal_antijoin_support(INTERNAL)
RETURNS INTERNAL
AS 'temporal_ops', 'noop_support'
LANGUAGE C;
SELECT	(t.a).*, valid_at
FROM		temporal_antijoin('a', 'id', 'valid_at', 'b', 'id', 'valid_at') AS t(a a, valid_at int4range);
