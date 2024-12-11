CREATE EXTENSION temporal_ops;

CREATE TABLE a (
  id int,
  valid_at int4range
);
CREATE TABLE b (
  id int,
  valid_at int4range
);

INSERT INTO a VALUES
  (1, '[1,20)'),
  (2, '[1,20)'),
  (4, '[1,20)'),
  (6, '[1,20)'),
  (7, '[5,20)'),
  (8, 'empty'),
  (9, '[1,20)');

INSERT INTO b VALUES
  (1, '[5,10)'),
  (1, '[15,30)'),
  (3, '[5,10)'),
  (4, '[500,600)'),
  (6, '[5,10)'),
  (6, '[5,12)'),
  (7, 'empty'),
  (8, '[5,10)'),
  (9, '[1,20)');

-- Test with duplicate inputs:

CREATE VIEW a2 AS
  SELECT * FROM a
  UNION ALL
  SELECT * FROM a;

CREATE VIEW b2 AS
  SELECT * FROM b
  UNION ALL
  SELECT * FROM b;
