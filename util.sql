
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
CREATE OR REPLACE FUNCTION agg_concat (agg_str text, delim text, new_str text)
       RETURNS TEXT
AS $$
BEGIN
	IF new_str IS NULL THEN
	   RETURN agg_str;
	ELSE
	   RETURN agg_str || delim || new_str;
	END IF;
END
$$ LANGUAGE plpgsql;

DROP AGGREGATE IF EXISTS group_concat (text, text);
CREATE AGGREGATE group_concat (text, text) (
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

