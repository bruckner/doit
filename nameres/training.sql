
-- Initialize the training schema
create or replace function init_training () returns void as 
$$
begin

-- reload training schema
if schema_exists('training') then
   drop schema training cascade;
end if;
create schema training;


-- training tables and views
create table training.sources (source_id integer);

create view training.fields as
select source_id, name, tag_code
  from doit_fields
 where source_id
    in (select source_id from training.sources);

create view training.data as
     select d.source_id,f.tag_code,d.local_entity_id,d.name,d.value
       from doit_data d
 inner join training.fields f
         on d.source_id = f.source_id
        and d.name = f.name;

end
$$ language plpgsql;


-- Add a new source to the training set
create or replace function add_training_source (integer) returns integer
as
$$
begin
   insert into training.sources (source_id) values ($1);
   return $1;
end
$$ language plpgsql;



