/* temporal_ops--1.0.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION temporal_ops" to load this file \quit

/*
 * ********
 * semijoin
 * ********
 */

CREATE OR REPLACE FUNCTION temporal_semijoin_sql(
  left_table regclass,
  left_keys text[],
  left_valid_at text,
  right_table regclass,
  right_keys text[],
  right_valid_at text)
RETURNS TEXT
AS 'temporal_ops', 'temporal_semijoin_keys_sql'
LANGUAGE C STRICT STABLE;

CREATE OR REPLACE FUNCTION temporal_semijoin_sql(
  left_table regclass,
  left_key text,
  left_valid_at text,
  right_table regclass,
  right_key text,
  right_valid_at text)
RETURNS TEXT
AS 'temporal_ops', 'temporal_semijoin_key_sql'
LANGUAGE C STRICT STABLE;

CREATE OR REPLACE FUNCTION temporal_semijoin_support(INTERNAL)
RETURNS INTERNAL
AS 'temporal_ops', 'temporal_semijoin_support'
LANGUAGE C STRICT STABLE;

/*
 * ********
 * antijoin
 * ********
 */

CREATE OR REPLACE FUNCTION temporal_antijoin_sql(
  left_table regclass,
  left_keys text[],
  left_valid_at text,
  right_table regclass,
  right_keys text[],
  right_valid_at text)
RETURNS TEXT
AS 'temporal_ops', 'temporal_antijoin_keys_sql'
LANGUAGE C STRICT STABLE;

CREATE OR REPLACE FUNCTION temporal_antijoin_sql(
  left_table regclass,
  left_key text,
  left_valid_at text,
  right_table regclass,
  right_key text,
  right_valid_at text)
RETURNS TEXT
AS 'temporal_ops', 'temporal_antijoin_key_sql'
LANGUAGE C STRICT STABLE;

CREATE OR REPLACE FUNCTION temporal_antijoin_support(INTERNAL)
RETURNS INTERNAL
AS 'temporal_ops', 'temporal_antijoin_support'
LANGUAGE C STRICT STABLE;

/*
 * **********
 * outer join
 * **********
 */

CREATE OR REPLACE FUNCTION temporal_outer_join_sql(
  left_table regclass,
  left_keys text[],
  left_valid_at text,
  right_table regclass,
  right_keys text[],
  right_valid_at text)
RETURNS TEXT
AS 'temporal_ops', 'temporal_outer_join_keys_sql'
LANGUAGE C STRICT STABLE;

CREATE OR REPLACE FUNCTION temporal_outer_join_sql(
  left_table regclass,
  left_key text,
  left_valid_at text,
  right_table regclass,
  right_key text,
  right_valid_at text)
RETURNS TEXT
AS 'temporal_ops', 'temporal_outer_join_key_sql'
LANGUAGE C STRICT STABLE;

CREATE OR REPLACE FUNCTION temporal_outer_join_support(INTERNAL)
RETURNS INTERNAL
AS 'temporal_ops', 'temporal_outer_join_support'
LANGUAGE C STRICT STABLE;

/*
 * temporal_semijoin - semijoins left table+columns to right table+columns
 *
 * Assumes an equijoin on a single key column plus application-time columns.
 *
 * Returns records with the left-hand tuple and intersecting application-time.
 *
 * Since this query returns SETOF RECORD,
 * the caller must declare the names+types of the result.
 * For example:
 *
 * SELECT (j.a).*, valid_at
 * FROM temporal_semijoin(
 *        'a', 'id', 'valid_at',
 *        'b', 'a_id', valid_at')
 *      AS j(a a, valid_at daterange)
 *
 * TODO: Implement SupportRequestRows to give better selectivity estimates.
 * (Is that even necessary if we are replacing ourself with a Node tree?)
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
  q TEXT := temporal_semijoin_sql(left_table, left_id_col, left_valid_col,
                                  right_table, right_id_col, right_valid_col);
BEGIN
  RETURN QUERY EXECUTE q;
END;
$$ STABLE LEAKPROOF PARALLEL SAFE SUPPORT temporal_semijoin_support LANGUAGE plpgsql;



/*
 * Like temporal_semijoin above, but takes text[] instead of text
 * for the scalar key columns.
 */
CREATE OR REPLACE FUNCTION temporal_semijoin(
  left_table regclass,
  left_id_cols text[],
  left_valid_col text,
  right_table regclass,
  right_id_cols text[],
  right_valid_col text
)
RETURNS SETOF RECORD AS $$
DECLARE
  q TEXT := temporal_semijoin_sql(left_table, left_id_cols, left_valid_col,
                                  right_table, right_id_cols, right_valid_col);
BEGIN
  RETURN QUERY EXECUTE q;
END;
$$ STABLE LEAKPROOF PARALLEL SAFE SUPPORT temporal_semijoin_support LANGUAGE plpgsql;



/*
 * temporal_antijoin - antijoins left table+columns to right table+columns
 *
 * Assumes an equijoin on a single key column plus application-time columns.
 *
 * Returns records with the left-hand tuple and intersection application-time.
 *
 * Since this query returns SETOF RECORD,
 * the caller must declare the names+types of the result.
 * For example:
 *
 * SELECT (j.a).*, valid_at
 * FROM temporal_antijoin(
 *        'a', 'id', 'valid_at',
 *        'b', 'a_id', 'valid_at')
 *      AS j(a a, valid_at daterange)
 */
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
  q TEXT := temporal_antijoin_sql(left_table, left_id_col, left_valid_col,
                                  right_table, right_id_col, right_valid_col);
BEGIN
  RETURN QUERY EXECUTE q;
END;
$$ STABLE LEAKPROOF PARALLEL SAFE SUPPORT temporal_antijoin_support LANGUAGE plpgsql;

/*
 * Like temporal_antijoin above, but takes text[] instead of text
 * for the scalar key columns.
 */
CREATE OR REPLACE FUNCTION temporal_antijoin(
  left_table regclass,
  left_id_cols text[],
  left_valid_col text,
  right_table regclass,
  right_id_cols text[],
  right_valid_col text
)
RETURNS SETOF RECORD AS $$
DECLARE
  q TEXT := temporal_antijoin_sql(left_table, left_id_cols, left_valid_col,
                                  right_table, right_id_cols, right_valid_col);
BEGIN
  RETURN QUERY EXECUTE q;
END;
$$ STABLE LEAKPROOF PARALLEL SAFE SUPPORT temporal_antijoin_support LANGUAGE plpgsql;



/*
 * temporal_outer_join - outer joins left table+columns to right table+columns
 *
 * Assumes an equijoin on a single key column plus application-time columns.
 *
 * Returns records with the left-hand tuple, right-hand tuple (or all NULLs),
 * and intersection application-time.
 *
 * Since this query returns SETOF RECORD,
 * the caller must declare the names+types of the result.
 * For example:
 *
 * SELECT (j.a).*, (j.b).*, valid_at
 * FROM temporal_outer_join(
 *        'a', 'id', 'valid_at',
 *        'b', 'a_id', 'valid_at')
 *      AS j(a a, b b, valid_at daterange)
 */
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
  q TEXT := temporal_outer_join_sql(left_table, left_id_col, left_valid_col,
                                    right_table, right_id_col, right_valid_col);
BEGIN
  RETURN QUERY EXECUTE q;
END;
$$ STABLE LEAKPROOF PARALLEL SAFE SUPPORT temporal_outer_join_support LANGUAGE plpgsql;

/*
 * Like temporal_outer_join above, but takes text[] instead of text
 * for the scalar key columns.
 */
CREATE OR REPLACE FUNCTION temporal_outer_join(
  left_table regclass,
  left_id_cols text[],
  left_valid_col text,
  right_table regclass,
  right_id_cols text[],
  right_valid_col text
)
RETURNS SETOF RECORD AS $$
DECLARE
  q TEXT := temporal_outer_join_sql(left_table, left_id_cols, left_valid_col,
                                    right_table, right_id_cols, right_valid_col);
BEGIN
  RETURN QUERY EXECUTE q;
END;
$$ STABLE LEAKPROOF PARALLEL SAFE SUPPORT temporal_outer_join_support LANGUAGE plpgsql;
