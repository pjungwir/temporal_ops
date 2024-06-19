SELECT  a.id, b.id, j.valid_at
FROM    a
JOIN    b
ON      a.id = b.id
AND     a.valid_at && b.valid_at
JOIN LATERAL  temporal_semijoin(a.valid_at, b.valid_at) AS j(valid_at)
ON      true;
