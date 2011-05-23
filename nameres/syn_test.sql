create or replace function init_syn_test () returns void as
$$
begin

-- reload testing schema
if schema_exists('syn_test') then
	drop schema syn_test cascade;
end if;
create schema syn_test;

-- testing tables, views, and indexes
create table syn_test.tf as
select *
  from training.syn_tf;

create index idx_syn_tf_gram on syn_test.tf (gram);
create index idx_syn_tf_tag_code on syn_test.tf (tag_code);

create view syn_test.name_count as
select count(distinct name || source_id) c
  from training.fields;

create table syn_test.idf as
  select g.gram, sqrt(log(c.c::float / count(distinct g.tag_code)::float)) score
    from training.syn_grams g, syn_test.name_count c
group by g.gram, c.c;

create index idx_syn_idf_gram on syn_test.idf (gram);

create table syn_test.input (
       name text,
       gram text
);

end
$$ language plpgsql;


create or replace function syn_test_source (integer) returns void
as $$
begin
delete from syn_test.input;

insert into syn_test.input (name, gram)
select name, qgrams2(name,3)
  from doit_fields
 where source_id = $1;

create view syn_test.results as
  select i.name, tf.tag_code, sum(tf.score*idf.score) score
    from syn_test.input i, syn_test.tf, syn_test.idf
   where i.gram = tf.gram
     and tf.gram = idf.gram
group by i.name, tf.tag_code
order by i.name asc, score desc;

end
$$ language plpgsql;
