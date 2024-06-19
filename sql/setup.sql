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
  (4, '[1,20)');

INSERT INTO b VALUES
  (1, '[5,10)'),
  (1, '[15,30)'),
  (3, '[5,10)'),
  (4, '[500,600)');
