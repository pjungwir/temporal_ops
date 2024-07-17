-- Just a basic inner join

SELECT  a.id, b.id, a.valid_at * b.valid_at AS valid_at
FROM    a
JOIN    b
ON      a.id = b.id AND a.valid_at && b.valid_at
ORDER BY a.id, b.id, valid_at
