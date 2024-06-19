/* temporal_ops--1.0.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION temporal_ops" to load this file \quit

CREATE OR REPLACE FUNCTION temporal_semijoin(a anyrange, b anyrange)
RETURNS SETOF anyrange AS $$
  SELECT  a * b;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;
