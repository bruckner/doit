
-- Generate a list of random source_ids
-- First arg in # sources, second it max source size (in entities)
create or replace function random_source_list (integer, integer) returns setof integer
as '
select source_id
  from (
    select source_id, random() x
      from doit_sources
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
