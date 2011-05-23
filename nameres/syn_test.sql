create or replace function syn_test_init () returns void as
$$
begin
  -- reload testing schema
  if schema_exists('syn_test') then
	drop schema syn_test cascade;
  end if;
  create schema syn_test;

  -- Auxillary tables/views for testing
  create table syn_test.sources (source_id integer);

  create view syn_test.input as
       select name, qgrams2(name, 3) gram
         from doit_fields
        where source_id
           in (select source_id from syn_test.sources);

  create table syn_test.results (
       name text,
       match text,
       score float
  );

  create view syn_test.name_count as
       select count(distinct name || source_id) c
         from training.fields;


  -- Tables/views for qgrams tf-idf
  create view syn_test.raw_qgrams as
       select tag_code, qgrams2(name,3) gram
         from training.fields;

  create view syn_test.qgrams as
       select tag_code, gram, count(gram) c
         from syn_test.raw_qgrams
     group by tag_code, gram;

  create table syn_test.qgrams_tf as
  select tag_code, gram, log(1+c) score
    from syn_test.qgrams;

  create index idx_syn_qgrams_tf_gram on syn_test.qgrams_tf (gram);
  create index idx_syn_qgrams_tf_tag_code on syn_test.qgrams_tf (tag_code);

  create table syn_test.qgrams_idf as
        select g.gram, sqrt(log(c.c::float / count(distinct g.tag_code)::float)) score
	  from syn_test.qgrams g, syn_test.name_count c
      group by g.gram, c.c;

  create index idx_syn_qgrams_idf_gram on syn_test.qgrams_idf (gram);

end
$$ language plpgsql;


-- Adds a source to the test list syn_test.sources
create or replace function syn_test_add_source (integer) returns integer
as $$
begin
	insert into syn_test.sources (source_id) values ($1);
	return $1;
end
$$ language plpgsql;


create or replace function syn_test_load_results () returns void
as $$
begin
	delete from syn_test.results;

	insert into syn_test.results (name, match, score)
	     select i.name, tf.tag_code, sum(tf.score*idf.score)
	       from syn_test.input i, syn_test.qgrams_tf tf, syn_test.qgrams_idf idf
	      where i.gram = tf.gram
		and tf.gram = idf.gram
	   group by i.name, tf.tag_code;
end
$$ language plpgsql;
