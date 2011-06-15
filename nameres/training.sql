-- Create some shared resources for loading training data
-- This is Goby specific for the moment...

-- Housekeeping
DROP TABLE IF EXISTS training_sources CASCADE;
DROP VIEW IF EXISTS training_fields CASCADE;
DROP VIEW IF EXISTS training_data CASCADE;

-- Sources to use for training kept here
CREATE TABLE training_sources (source_id integer);

-- Useful views
CREATE VIEW training_fields AS
SELECT source_id, name, tag_code
  FROM public.doit_fields
 WHERE source_id
    IN (SELECT source_id FROM training_sources);

CREATE VIEW training_data AS
     SELECT source_id, local_entity_id, name, value
       FROM public.doit_data
      WHERE source_id
         IN (SELECT source_id FROM training_sources)
	AND value IS NOT NULL;

-- UDF to load training data into DoIt
CREATE OR REPLACE FUNCTION training_stage (integer, integer) RETURNS void AS
$$
BEGIN

-- Get some random training sources
INSERT INTO training_sources
     SELECT * FROM random_source_list($1,$2);

-- Load training data into staging area
INSERT INTO in_sources (source_id)
     SELECT * FROM training_sources;

INSERT INTO in_fields (source_id, name)
     SELECT source_id, name
       FROM training_fields;

INSERT INTO in_data (source_id, entity_id, name, value)
     SELECT source_id, local_entity_id, name, value
       FROM training_data;

-- Load training tags as global atts
INSERT INTO global_attributes (source_id, name)
     SELECT MIN(source_id), tag_code
       FROM training_fields
   GROUP BY tag_code;

-- Create training attribute clusters
INSERT INTO attribute_clusters (global_id, global_name, local_source_id,
                                local_name, uncertainty, authority)
     SELECT g.id, g.name, t.source_id, t.name, 0.0, 1.0
       FROM global_attributes g, training_fields t
      WHERE g.name = t.tag_code;

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION training_load (integer, integer) RETURNS void AS
$$
BEGIN
RAISE INFO 'Staging some random training data...';
PERFORM training_stage($1, $2);
RAISE INFO '  done.';

-- Load method-specific training data:
IF EXISTS (SELECT 1 FROM integration_methods WHERE method_name = 'att_qgrams' AND active = 't') THEN
  RAISE INFO 'Loading qgrams for attribute synonym matching...';
  PERFORM syn_load_qgrams();
  RAISE INFO '  done.';
END IF;

IF EXISTS (SELECT 1 FROM integration_methods WHERE method_name = 'mdl' AND active = 't') THEN
  RAISE INFO 'Loading MDL dictionary data...';
  PERFORM mdl_load_dictionaries();
  RAISE INFO '  done.';
END IF;

IF EXISTS (SELECT 1 FROM integration_methods WHERE method_name = 'val_qgrams' AND active = 't') THEN
  RAISE INFO 'Loading qgrams for value-set matching...';
  PERFORM val_qgrams_stage();
  PERFORM val_qgrams_load();
  RAISE INFO '  done.';
END IF;

IF EXISTS (SELECT 1 FROM integration_methods WHERE method_name = 'val_ngrams' AND active = 't') THEN
  RAISE INFO 'Loading ngrams for value-set matching...';
  PERFORM val_ngrams_stage();
  PERFORM val_ngrams_load();
  RAISE INFO '  done.';
END IF;

IF EXISTS (SELECT 1 FROM integration_methods WHERE method_name = 'dist' AND active = 't') THEN
  RAISE INFO 'Loading value-set distribution data...';
  PERFORM dist_load();
  RAISE INFO '  done.';
END IF;

-- Flush the staging area
PERFORM staging_flush();

END
$$ LANGUAGE plpgsql;



