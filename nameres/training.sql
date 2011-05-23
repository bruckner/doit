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

create table training.fields (
       source_id integer,
       name text,
       tag_code text
);

create index idx_training_fields_source_id_name on training.fields (source_id,name);

create table training.data (
       source_id integer,
       tag_code text,
       local_entity_id integer,
       name text,
       value text
);


-- synonym tf-idf views
create view training.raw_syn_grams as
select tag_code, qgrams2(name,3) gram
  from training.fields;

create view training.syn_grams as
  select tag_code, gram, count(gram) c
    from training.raw_syn_grams
group by tag_code, gram;

create view training.syn_tf as
select tag_code, gram, log(1+c) score
  from training.syn_grams;


-- dictionary tf-idf views
create view training.raw_val_grams as
select tag_code, qgrams2(value,3) gram
  from training.data
 where value is not null;

create view training.val_grams as
  select tag_code, gram, count(gram) c
    from training.raw_val_grams
group by tag_code, gram;

create view training.val_count as
select count(*) c
  from training.data
 where value is not null;

create view training.val_tf as
select tag_code, gram, log(1+c) score
  from training.val_grams;


-- functions
create or replace function add_training_source (integer) returns void
as '
insert into training.sources (source_id) values ($1);

insert into training.fields (source_id,name,tag_code)
select source_id,name,tag_code
  from doit_fields
 where source_id = $1;

insert into training.data (source_id,tag_code,local_entity_id,name,value)
     select d.source_id,f.tag_code,d.local_entity_id,d.name,d.value
       from doit_data d
 inner join training.fields f
         on d.source_id = f.source_id
        and d.name = f.name;
' language sql;

end
$$ language plpgsql;
