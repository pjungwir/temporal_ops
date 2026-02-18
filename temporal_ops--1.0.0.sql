/* temporal_ops--1.0.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION temporal_ops" to load this file \quit

CREATE OR REPLACE FUNCTION temporal_semijoin_support(INTERNAL)
RETURNS INTERNAL
AS 'temporal_ops', 'temporal_semijoin_support'
LANGUAGE C;

CREATE OR REPLACE FUNCTION temporal_antijoin_support(INTERNAL)
RETURNS INTERNAL
AS 'temporal_ops', 'temporal_antijoin_support'
LANGUAGE C;

/*
 * temporal_semijoin - semijoins left table+columns to right table+columns
 *
 * Assumes an equijoin on a single key column plus application-time columns.
 *
 * Returns records with the left-hand id and intersecting application-time.
 *
 * Since this query returns SETOF RECORD,
 * the caller must declare the names+types of the result.
 * For example:
 *
 * SELECT *
 * FROM temporal_semijoin(
 *        'a', 'id', 'valid_at',
 *        'b', 'a_id', valid_at')
 *      AS j(id int, valid_at daterange)
 *
 * TODO: Implement SupportRequestRows to give better selectivity estimates.
 * (Is that even necessary if we are replacing ourself with a Node tree?)
 *
 * TODO: Write a version with multiple key columns.
 *
 * TODO: Write a version with extra left-hand columns to SELECT.
 *
 * TODO: If left_col is an FK to right_col,
 * use a simpler SQL statement,
 * since we know we'll only get at most one right record
 * for each left.
 */
CREATE OR REPLACE FUNCTION temporal_semijoin(
  left_table regclass,
  left_id_col text,
  left_valid_col text,
  right_table regclass,
  right_id_col text,
  right_valid_col text
)
RETURNS SETOF RECORD AS $$
DECLARE
  subquery TEXT := 'j';
BEGIN
  IF left_table::text = 'j' OR right_table::text = 'j' THEN
    subquery := 'j1';
    IF left_table::text = 'j1' OR right_table::text = 'j1' THEN
      subquery := 'j2';
    END IF;
  END IF;
  RETURN QUERY EXECUTE format($j$
    SELECT  %1$I.%2$I, UNNEST(multirange(%1$I.%3$I) * %7$I.%6$I) AS %3$I
    FROM    %1$I
    JOIN (
      SELECT  %4$I.%5$I, range_agg(%4$I.%6$I) AS %6$I
      FROM    %4$I
      GROUP BY %4$I.%5$I
    ) AS %7$I
    ON %1$I.%2$I = %7$I.%5$I AND %1$I.%3$I && %7$I.%6$I;
  $j$, left_table, left_id_col, left_valid_col, right_table, right_id_col, right_valid_col, subquery);
END;
$$ STABLE LEAKPROOF PARALLEL SAFE SUPPORT temporal_semijoin_support LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION temporal_antijoin(
  left_table regclass,
  left_id_col text,
  left_valid_col text,
  right_table regclass,
  right_id_col text,
  right_valid_col text
)
RETURNS SETOF RECORD AS $$
DECLARE
  subquery TEXT := 'j';
BEGIN
  IF left_table::text = 'j' OR right_table::text = 'j' THEN
    subquery := 'j1';
    IF left_table::text = 'j1' OR right_table::text = 'j1' THEN
      subquery := 'j2';
    END IF;
  END IF;

  RETURN QUERY EXECUTE format($j$
    SELECT  %1$I.%2$I,
            UNNEST(CASE WHEN %7$I.%6$I IS NULL THEN multirange(%1$I.%3$I)
                        ELSE multirange(%1$I.%3$I) - %7$I.%3$I END) AS %3$I
    FROM    %1$I
    LEFT JOIN (
      SELECT  %4$I.%5$I, range_agg(%4$I.%6$I) AS %6$I
      FROM    %4$I
      GROUP BY %4$I.%5$I
    ) AS %7$I
    ON %1$I.%2$I = %7$I.%5$I AND %1$I.%3$I && %7$I.%6$I;
  $j$, left_table, left_id_col, left_valid_col, right_table, right_id_col, right_valid_col, subquery);
END;
$$ STABLE LEAKPROOF PARALLEL SAFE SUPPORT temporal_antijoin_support LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION temporal_outer_join(
  left_table regclass,
  left_id_col text,
  left_valid_col text,
  right_table regclass,
  right_id_col text,
  right_valid_col text
)
RETURNS SETOF RECORD AS $$
DECLARE
  subquery1 TEXT := 'ja';
  subquery2 TEXT := 'jb';
  result_valid_col TEXT := left_valid_col;
  result_valid_col_type TEXT := (
    SELECT  atttypid::regtype::text
    FROM    pg_attribute
    WHERE   attrelid = left_table::regclass
    AND     attname = left_valid_col
  );
BEGIN
  IF left_table::text = 'ja' OR right_table::text = 'ja' THEN
    subquery1 := 'ja1';
    IF left_table::text = 'ja1' OR right_tabl::text = 'ja1' THEN
      subquery1 := 'ja2';
    END IF;
  END IF;
  IF left_table::text = 'jb' OR right_table::text = 'jb' THEN
    subquery2 := 'jb1';
    IF left_table::text = 'jb1' OR right_table::text = 'jb1' THEN
      subquery2 := 'jb2';
    END IF;
  END IF;

  RETURN QUERY EXECUTE format($j$
	  SELECT  %7$I.%1$I, %8$I.%4$I, %8$I.%9$I
	  FROM    (
	    SELECT  %1$I,
              array_agg(ROW(%4$I, COALESCE(%1$I.%3$I * %4$I.%6$I, %1$I.%3$I))) AS %4$I,
	            range_agg(%4$I.%6$I) AS %6$I
	    FROM    %1$I
	    LEFT JOIN %4$I
	    ON      %1$I.%2$I = %4$I.%5$I AND %1$I.%3$I && %4$I.%6$I
	    GROUP BY %1$I
	  ) AS %7$I
	  JOIN LATERAL (
	    SELECT %4$I.%4$I, %4$I.%9$I FROM UNNEST(%7$I.%4$I) AS %4$I(%4$I %4$I, %9$I %10$I)
	    UNION ALL
	    SELECT NULL, UNNEST(multirange((%7$I.%1$I).%3$I) - %7$I.%6$I)
	  ) AS %8$I ON true
	  WHERE   NOT isempty((%7$I.%1$I).%3$I);
  $j$,
  left_table, left_id_col, left_valid_col,
  right_table, right_id_col, right_valid_col,
  subquery1, subquery2, result_valid_col, result_valid_col_type);
END;
$$ STABLE LEAKPROOF PARALLEL SAFE /* SUPPORT temporal_outer_join_support */ LANGUAGE plpgsql;
