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
