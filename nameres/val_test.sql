-- UDFs for doit value comparisons

-- Initialize the value test schema
create or replace function val_test_init () returns void as
$$
begin
  -- reload testing schema
  if schema_exists('val_test') then
	drop schema val_test cascade;
  end if;
  create schema val_test;

  -- Holds sources to use for testing
  create table val_test.sources (source_id integer);

  -- Auxillary tables/views for testing
  create view val_test.qgrams_input as
       select name, qgrams2(value,3) gram
         from doit_data
        where source_id
           in (select source_id from val_test.sources)
          and value is not null;

  create table val_test.qgrams_results (
       name text,
       match text,
       score float
  );

  create view val_test.ngrams_input as
  select null;

  create table val_test.ngrams_results (
       name text,
       match text,
       score float
  );


  -- For idf calculations
  create view val_test.dict_count as
       select count(distinct tag_code) c
         from training.fields;


  -- Tables/views for qgrams tf-idf (created but not populated)
  create view val_test.raw_qgrams as
       select tag_code, qgrams2(value,3) gram
         from training.data
        where value is not null;

  create view val_test.qgrams as
       select tag_code, gram, count(gram) c
         from val_test.raw_qgrams
     group by tag_code, gram;

  create table val_test.qgrams_tf (
       tag_code text,
       gram text,
       score float
  );

  create table val_test.qgrams_idf (
       gram text,
       score float
  );


-- Tables/views for ngrams tf-idf (created but not populated)
-- coming soon...

end
$$ language plpgsql;


-- Loads data from training schema for use with future testing
create or replace function val_test_load_qgrams_training () returns void
as $$
begin
  if index_exists('index idx_val_tf_gram') then
     drop index idx_val_tf_gram;
  end if;

   if index_exists('index idx_val_tf_tag_code') then
     drop index idx_val_tf_tag_code;
   end if;

  if index_exists('index idx_val_idf_gram') then
     drop index idx_val_idf_gram;
  end if;

  delete from val_test.qgrams_tf;
  delete from val_test.qgrams_idf;

  insert into val_test.qgrams_tf (tag_code, gram, score)
       select tag_code, gram, log(1+c)
         from val_test.qgrams;

  create index idx_val_tf_gram on val_test.qgrams_tf (gram);
  create index idx_val_tf_tag_code on val_test.qgrams_tf (tag_code);

  insert into val_test.qgrams_idf (gram, score)
       select g.gram, sqrt(log(c.c::float / count(distinct g.tag_code)::float))
         from val_test.qgrams g, val_test.dict_count c
     group by g.gram, c.c;

  create index idx_val_idf_gram on val_test.qgrams_idf (gram);

end
$$ language plpgsql;


-- Adds a source to the test list val_test.sources
create or replace function val_test_add_source (integer) returns integer
as $$
begin
	insert into val_test.sources (source_id) values ($1);
	return $1;
end
$$ language plpgsql;


-- Loads results for qgram test into val_test.qgrams_results
-- Uses any loaded training data, and test source list in val_test.sources
create or replace function val_test_load_qgrams_results () returns void
as $$
begin
	delete from val_test.qgrams_results;

	insert into val_test.qgrams_results (name, match, score)
   	     select i.name, tf.tag_code, sum(tf.score*idf.score)
     	       from val_test.qgrams_input i, val_test.qgrams_tf tf, val_test.qgrams_idf idf
    	      where i.gram = tf.gram
      	        and tf.gram = idf.gram
 	   group by i.name, tf.tag_code;
end
$$ language plpgsql;

