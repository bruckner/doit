
-- Safe text-to-numeric extractor; returns null if s is nonnumeric
create or replace function to_num (s text) returns numeric as
$$
import math

if (s is None):
   return s

try:
    n = float(s)
    if (math.isinf(n) or math.isnan(n)):
        return None
    else:
        return n
except ValueError:
    return None

$$ language plpythonu;


-- Generate a list of random source_ids
-- First arg in # sources, second it max source size (in entities)
create or replace function random_source_list (integer, integer) returns setof integer
as '
select source_id
  from (
    select source_id, random() x
      from public.doit_sources
     where n_entities <= $2
  order by x desc
     limit $1
  ) t;
' language sql;


-- Exists functions
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


-- Aggregate function for string concatenation
create or replace function agg_concat (agg_str text, delim text, new_str text)
       returns text
as $$
begin
	if new_str is null then
	   return agg_str;
	else
	   return agg_str || delim || new_str;
	end if;
end
$$ language plpgsql;

drop aggregate if exists group_concat (text, text);

create aggregate group_concat (text, text) (
       stype = text,
       sfunc = agg_concat,
       initcond = ''
);

