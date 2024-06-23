/* temporal_ops--1.0.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION temporal_ops" to load this file \quit

CREATE OR REPLACE FUNCTION temporal_semijoin_support(INTERNAL)
RETURNS INTERNAL
AS 'temporal_ops', 'temporal_semijoin_support'
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
 * TODO: Try to use SupportRequestSimplify to return a Node tree instead,
 * so that the planner can push down predicates.
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
  left_table text,
  left_id_col text,
  left_valid_col text,
  right_table text,
  right_id_col text,
  right_valid_col text
)
RETURNS SETOF RECORD AS $$
DECLARE
  subquery TEXT := 'j';
BEGIN
  IF left_table = 'j' OR right_table = 'j' THEN
    subquery := 'j1';
    IF left_table = 'j1' OR right_table = 'j1' THEN
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
