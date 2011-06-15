
-- Staging area for incoming data
DROP TABLE IF EXISTS in_sources CASCADE;
DROP TABLE IF EXISTS in_fields CASCADE;
DROP TABLE IF EXISTS in_data CASCADE;

CREATE TABLE in_sources (
       source_id integer
);

CREATE TABLE in_fields (
       source_id integer,
       name text
);

CREATE TABLE in_data (
       source_id integer,
       entity_id integer,
       name text,
       value text NOT NULL
);

-- UDF to fill staging area with random sources
CREATE OR REPLACE FUNCTION staging_load (integer, integer DEFAULT 10000000) RETURNS void AS
$$
BEGIN

  INSERT INTO in_sources (source_id)
       SELECT * FROM random_source_list($1,$2);

  INSERT INTO in_fields (source_id, name)
       SELECT source_id, name
         FROM public.doit_fields
        WHERE source_id
           IN (SELECT source_id FROM in_sources);

  INSERT INTO in_data (source_id, entity_id, name, value)
       SELECT source_id, local_entity_id, name, value
         FROM public.doit_data
        WHERE source_id
           IN (SELECT source_id FROM in_sources)
	  AND value IS NOT NULL;

END
$$ LANGUAGE 'plpgsql';

-- UDF to clear out the staging area
CREATE OR REPLACE FUNCTION staging_flush () RETURNS void AS
$$
BEGIN
  DELETE FROM in_sources;
  DELETE FROM in_fields;
  DELETE FROM in_data;

  PERFORM att_qgrams_flush();
  PERFORM mdl_flush();
  PERFORM val_qgrams_flush();
  PERFORM val_ngrams_flush();
END
$$ LANGUAGE plpgsql;

-- UDF to clean out the entire schema and start from scratch
CREATE OR REPLACE FUNCTION clean_house () RETURNS void AS
$$
BEGIN
  PERFORM staging_flush();

  DELETE FROM global_attributes;
  DELETE FROM attribute_clusters;

  DELETE FROM training_sources;

  PERFORM att_qgrams_clean();
  PERFORM mdl_clean();
  PERFORM val_qgrams_flush();
  PERFORM val_ngrams_flush();
END
$$ LANGUAGE plpgsql;



-- Core tables for name resolution
DROP TABLE IF EXISTS global_attributes CASCADE;
CREATE TABLE global_attributes (
       id serial,
       source_id integer,
       name text
);

DROP TABLE IF EXISTS attribute_clusters CASCADE;
CREATE TABLE attribute_clusters (
       global_id integer,
       global_name text,
       local_source_id integer,
       local_name text,
       uncertainty float,
       authority float
);


-- Integration methods options
DROP TABLE IF EXISTS integration_methods CASCADE;
CREATE TABLE integration_methods (
	id serial,
	method_name text,
	active boolean
);

INSERT INTO integration_methods (method_name, active)
     VALUES ('att_qgrams', 't'),
	    ('mdl', 't'),
	    ('val_ngrams', 't'),
	    ('val_qgrams', 'f'),
	    ('dist', 't');

