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
  create table val_test.qgrams_input (
  	 source_id integer,
	 name text,
	 gram text
  );

  create table val_test.qgrams_results (
       source_id integer,
       name text,
       match text,
       score float
  );

  create table val_test.tokens_input (
  	 source_id integer,
	 name text,
	 token text
  );

  create table val_test.tokens_results (
       source_id integer,
       name text,
       match text,
       score float
  );


  -- For idf calculations
  create view val_test.dict_count as
       select count(distinct tag_code) c
         from training.fields;

  -- Tables/views for token tf-idf
  create view val_test.raw_tokens as
       select tag_code, tokenize(value) token
         from training.data
        where value is not null;

  create view val_test.tokens as
       select tag_code, token, count(token) c
         from val_test.raw_tokens
     group by tag_code, token;

  create view val_test.token_freqs as
       select tag_code, sum(c) freq
         from val_test.tokens
     group by tag_code;

  create table val_test.tokens_tf (
  	 tag_code text,
	 token text,
  	 score float
  );

  create table val_test.tokens_idf (
  	 token text,
	 score float
  );

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

end
$$ language plpgsql;


-- Loads data from training schema for use with future testing
create or replace function val_test_load_qgrams_training () returns void
as $$
begin
  if index_exists('index idx_val_qgrams_tf_gram') then
     drop index idx_val_qgrams_tf_gram;
  end if;

   if index_exists('index idx_val_qgrams_tf_tag_code') then
     drop index idx_val_qgrams_tf_tag_code;
   end if;

  if index_exists('index idx_val_qgrams_idf_gram') then
     drop index idx_val_qgrams_idf_gram;
  end if;

  delete from val_test.qgrams_tf;
  delete from val_test.qgrams_idf;

  insert into val_test.qgrams_tf (tag_code, gram, score)
       select tag_code, gram, log(1+c)
         from val_test.qgrams;

  create index idx_val_qgrams_tf_gram on val_test.qgrams_tf (gram);
  create index idx_val_qgrams_tf_tag_code on val_test.qgrams_tf (tag_code);

  insert into val_test.qgrams_idf (gram, score)
       select g.gram, sqrt(log(c.c::float / count(distinct g.tag_code)::float))
         from val_test.qgrams g, val_test.dict_count c
     group by g.gram, c.c;

  create index idx_val_qgrams_idf_gram on val_test.qgrams_idf (gram);

end
$$ language plpgsql;


-- Loads data from training schema for use with future tokens testing
create or replace function val_test_load_tokens_training () returns void
as $$
begin
  if index_exists('index idx_val_tokens_tf_token') then
     drop index idx_val_tokens_tf_token;
  end if;

   if index_exists('index idx_val_tokens_tf_tag_code') then
     drop index idx_val_tokens_tf_tag_code;
   end if;

  if index_exists('index idx_val_tokens_idf_token') then
     drop index idx_val_tokens_idf_token;
  end if;

  delete from val_test.tokens_tf;
  delete from val_test.tokens_idf;

  insert into val_test.tokens_tf (tag_code, token, score)
       select t.tag_code, t.token, t.c::float / f.freq::float
         from val_test.tokens t, val_test.token_freqs f
	where t.tag_code = f.tag_code;

  create index idx_val_tokens_tf_token on val_test.tokens_tf (token);
  create index idx_val_token_tf_tag_code on val_test.tokens_tf (tag_code);

  insert into val_test.tokens_idf (token, score)
       select t.token, sqrt(log(c.c::float / count(distinct t.tag_code)::float))
         from val_test.tokens t, val_test.dict_count c
     group by t.token, c.c;

  create index idx_val_tokens_idf_token on val_test.tokens_idf (token);

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


-- Load test data into input table for qgrams test
create or replace function val_test_load_qgrams_test () returns void
as $$
begin
	if index_exists('idx_val_qgrams_input_gram') then
	   drop index idx_val_qgrams_input_gram;
	end if;

	insert into val_test.qgrams_input (source_id, name, gram)
        select source_id, name, qgrams2(value,3) gram
          from doit_data
         where source_id
	    in (select source_id from val_test.sources)
	   and value is not null;

	create index idx_val_qgrams_input_gram on val_test.qgrams_input (gram);
end
$$ language plpgsql;


-- Load test data into input table for tokens test
create or replace function val_test_load_tokens_test () returns void
as $$
begin
	if index_exists('idx_val_tokens_input_token') then
	   drop index idx_val_tokens_input_token;
	end if;

	insert into val_test.tokens_input (source_id, name, token)
        select source_id, name, tokenize(value) token
          from doit_data
         where source_id
	    in (select source_id from val_test.sources)
	   and value is not null;

	create index idx_val_tokens_input_token on val_test.tokens_input (token);
end
$$ language plpgsql;




-- Loads results for qgram test into val_test.qgrams_results
-- Uses any loaded training data, and test source list in val_test.sources
create or replace function val_test_load_qgrams_results () returns void
as $$
begin
	delete from val_test.qgrams_results;

	insert into val_test.qgrams_results (source_id, name, match, score)
   	     select i.source_id, i.name, tf.tag_code, sum(tf.score*idf.score)
     	       from val_test.qgrams_input i, val_test.qgrams_tf tf, val_test.qgrams_idf idf
    	      where i.gram = tf.gram
      	        and tf.gram = idf.gram
 	   group by i.source_id, i.name, tf.tag_code;
end
$$ language plpgsql;


-- Loads results for token test into val_test.qgrams_results
-- Uses any loaded training data, and test source list in val_test.sources
create or replace function val_test_load_tokens_results () returns void
as $$
begin
	delete from val_test.tokens_results;

	insert into val_test.tokens_results (source_id, name, match, score)
   	     select i.source_id, i.name, tf.tag_code, sum(tf.score*idf.score)
     	       from val_test.tokens_input i, val_test.tokens_tf tf, val_test.tokens_idf idf
    	      where i.token = tf.token
      	        and tf.token = idf.token
 	   group by i.source_id, i.name, tf.tag_code;
end
$$ language plpgsql;

