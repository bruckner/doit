
create or replace function init_val_test () returns void as
$$
begin

-- reload testing schema
if schema_exists('val_test') then
	drop schema val_test cascade;
end if;
create schema val_test;

-- testing tables, views, and indexes
create table val_test.tf as
select *
  from training.val_tf;

create index idx_val_tf_gram on val_test.tf (gram);
create index idx_val_tf_tag_code on val_test.tf (tag_code);

create view val_test.dict_count as
select count(distinct tag_code) c
  from training.fields;

create table val_test.dict_matches (
	name text,
	match text,
	uncertainty float
);

insert into val_test.dict_matches (name,match,uncertainty)
select distinct tag_code, '', 0.0
  from training.fields;

update val_test.dict_matches
   set match = name;

create table val_test.idf as
  select g.gram, sqrt(log(c.c::float / count(distinct g.tag_code)::float)) score
    from training.val_grams g, val_test.dict_count c
group by g.gram, c.c;

create index idx_val_idf_gram on val_test.idf (gram);

create table val_test.dict_lengths as
  select tag_code, sum(c) c
    from training.val_grams
group by tag_code;

create table val_test.input (
	name text,
	gram text
);

end
$$ language plpgsql;


create or replace function val_test_source (integer) returns void
as $$
begin
delete from val_test.input;

insert into val_test.input (name, gram)
select name, qgrams2(value,3)
  from doit_data
 where source_id = $1
   and value is not null;

create view val_test.results as
   select i.name, tf.tag_code, sum(tf.score*idf.score) score
     from val_test.input i, val_test.tf, val_test.idf
    where i.gram = tf.gram
      and tf.gram = idf.gram
 group by i.name, tf.tag_code
 order by i.name asc, score desc;

end
$$ language plpgsql;

