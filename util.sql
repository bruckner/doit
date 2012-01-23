/* Copyright (c) 2011 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */


-- Safe text-to-numeric extractor; returns null if s is nonnumeric
CREATE OR REPLACE FUNCTION to_num(TEXT) RETURNS NUMERIC AS
$$
DECLARE
  s ALIAS FOR $1;
BEGIN
  RETURN s::NUMERIC;
EXCEPTION
  WHEN DATA_EXCEPTION THEN
   RETURN NULL;
END
$$ LANGUAGE plpgsql;


-- Generate a list of random source_ids
-- First arg in # sources, second it max source size (in entities)
CREATE OR REPLACE FUNCTION random_source_list (integer, integer) RETURNS SETOF INTEGER
AS '
SELECT source_id
  FROM (
    SELECT source_id, random() x
      FROM public.doit_sources
     WHERE n_values <= $2
  ORDER BY x DESC
     LIMIT $1
  ) t;
' LANGUAGE sql;


-- Aggregate function for string concatenation
CREATE OR REPLACE FUNCTION agg_concat (agg_str TEXT, new_str TEXT, delim TEXT)
       RETURNS TEXT
AS $$
BEGIN
  IF new_str IS NULL THEN
    RETURN agg_str;
  END IF;

  IF agg_str = '' THEN
    RETURN new_str;
  ELSE
    RETURN agg_str || delim || new_str;
  END IF;
END
$$ LANGUAGE plpgsql;

DROP AGGREGATE IF EXISTS group_concat (TEXT, TEXT) CASCADE;
CREATE AGGREGATE group_concat (TEXT, TEXT) (
       stype = text,
       sfunc = agg_concat,
       initcond = ''
);


-- Overload round function to handle floats
CREATE OR REPLACE FUNCTION round(float, integer) RETURNS numeric AS
$$
BEGIN
  RETURN round($1::numeric, $2);
END
$$ LANGUAGE plpgsql;



-- Exists functions (NOT USED)
create or replace function table_exists (tname text, sname text default 'public')
       returns boolean
as '
   select exists
   	  (select tablename from pg_tables
   	    where tablename = $1
	      and schemaname = $2);
' language sql;

create or replace function schema_exists (text)
       returns boolean
as '
   select exists
   	  (select nspname from pg_namespace
   	    where nspname = $1);
' language sql;

create or replace function view_exists (vname text, sname text default 'public')
       returns boolean
as '
   select exists
   	  (select viewname from pg_views
   	    where viewname = $1
	      and schemaname = $2);
' language sql;

create or replace function index_exists (iname text)
       returns boolean
as '
   select exists
   	  (select indexname
	     from pg_indexes
	    where indexname = $1);
' language sql;

