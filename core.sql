/* Copyright (c) 2011 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */


-- Tables for local data and metadata
CREATE TABLE local_sources (
	id serial,
	local_id text
);

CREATE TABLE local_source_meta (
	source_id int,
	meta_name text,
	value text
);

CREATE TABLE local_fields (
	id serial,
	source_id int,
	local_id text,
	local_name text,
	local_desc text,
	display_order int
);

CREATE TABLE local_field_meta (
	field_id int,
	meta_name text,
	value text
);

CREATE TABLE local_entities (
	id serial,
	source_id int,
	local_id text
);

CREATE TABLE local_data (
	field_id int,
	entity_id int,
	value text
);
CREATE INDEX idx_local_data_field_id ON local_data (field_id);
--CREATE INDEX idx_local_data_entity_id ON local_data (entity_id);


-- Default adapter to load from data source
-- To override default adapter, replace this function
CREATE OR REPLACE FUNCTION import_source (INTEGER) RETURNS INT AS
$$
DECLARE
  local_source_id ALIAS FOR $1;
  new_source_id INT;
BEGIN

  INSERT INTO local_sources (local_id) VALUES (local_source_id);

  new_source_id := id FROM local_sources WHERE local_id::int = local_source_id;

  --INSERT INTO local_source_meta;

  INSERT INTO local_fields (source_id, local_name)
       SELECT s.id, f.name
         FROM public.doit_fields f, local_sources s
        WHERE f.source_id = local_source_id
	  AND s.id = new_source_id;

  --INSERT INTO local_field_meta;

  INSERT INTO local_entities (source_id, local_id)
  SELECT s.id, d.entity_id
    FROM public.doit_data d, local_sources s
   WHERE d.source_id = local_source_id
     AND s.id = new_source_id
GROUP BY s.id, d.entity_id;

  INSERT INTO local_data (field_id, entity_id, value)
       SELECT f.id, e.id, d.value
         FROM public.doit_data d, local_sources s, local_fields f, local_entities e
        WHERE d.source_id = local_source_id
	  AND s.id = new_source_id
	  AND f.source_id = s.id
	  AND f.local_name = d.name
	  AND e.source_id = s.id
	  AND e.local_id::int = d.entity_id
          AND length(d.value) < 1300
	  AND d.value IS NOT NULL;

  --PERFORM preprocess_source(new_source_id);

  RETURN new_source_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION import_random (INTEGER, INTEGER DEFAULT 10000000) RETURNS SETOF INT AS
$$
DECLARE
  n_sources  ALIAS FOR $1;
  max_values ALIAS FOR $2;
BEGIN
  -- Uses temp table because otherwise the query mysteriously crawls...
  CREATE TEMP TABLE in_sources_tmp AS
  SELECT source_id
    FROM public.doit_sources
   WHERE n_values <= max_values
     AND source_id NOT IN (SELECT local_id::INTEGER FROM local_sources)
ORDER BY random()
   LIMIT n_sources;

  RETURN QUERY
  SELECT import_source(source_id)
    FROM in_sources_tmp;

  DROP TABLE in_sources_tmp;
END
$$ LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION preprocess_source (integer) RETURNS VOID AS
$$
BEGIN
    PERFORM qgrams_preprocess_source($1);
    PERFORM dist_preprocess_source($1);
    PERFORM mdl_preprocess_source($1);
    PERFORM ngrams_preprocess_source($1);
END
$$ LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION preprocess_global () RETURNS VOID AS
$$
BEGIN
  RAISE INFO 'Preparing qgrams global index...';
  PERFORM qgrams_preprocess_global();
  RAISE INFO '  done.';
  RAISE INFO 'Preparing global distributions index...';
  PERFORM dist_preprocess_global();
  RAISE INFO '  done.';
  RAISE INFO 'Preparing MDL global dictionaries...';
  PERFORM mdl_preprocess_global();
  RAISE INFO '  done.';
  RAISE INFO 'Preparing ngrams global index...';
  PERFORM ngrams_preprocess_global();
  RAISE INFO '  done.';
END
$$ LANGUAGE plpgsql;


-- UDF to unload all local data
CREATE OR REPLACE FUNCTION unload_local () RETURNS void AS
$$
BEGIN
  DELETE FROM local_sources;
  DELETE FROM local_source_meta;
  DELETE FROM local_fields;
  DELETE FROM local_field_meta;
  DELETE FROM local_entities;
  DELETE FROM local_data;

  PERFORM qgrams_clean();
  PERFORM mdl_clean();
  PERFORM ngrams_clean();
  PERFORM dist_clean();
END
$$ LANGUAGE plpgsql;


-- UDF to clean out the entire schema and start from scratch
CREATE OR REPLACE FUNCTION clean_house () RETURNS void AS
$$
BEGIN
  PERFORM unload_local();
  DELETE FROM global_attributes;
  DELETE FROM attribute_mappings;
  PERFORM nr_clean();
END
$$ LANGUAGE plpgsql;



-- Core tables for name resolution
DROP TABLE IF EXISTS global_attributes CASCADE;
DROP TABLE IF EXISTS attribute_mappings CASCADE;
DROP TABLE IF EXISTS integration_methods CASCADE;

CREATE TABLE global_attributes (
       id serial,
       name text,
       external_id text,
       derived_from text
);

CREATE TABLE attribute_mappings (
       local_id integer,
       global_id integer,
       confidence float,
       authority float,
       who_created text,
       when_created timestamp,
       why_created text
);

CREATE VIEW attribute_affinities AS
     SELECT local_id, global_id, LEAST(GREATEST(0.0, SUM(authority * confidence)), 1.0) affinity
       FROM attribute_mappings
   GROUP BY local_id, global_id;

CREATE VIEW attribute_max_affinities AS
     SELECT a.*
       FROM attribute_affinities a
 INNER JOIN (SELECT local_id, MAX(affinity) AS "affinity" FROM attribute_affinities GROUP BY local_id) b
         ON a.local_id = b.local_id AND a.affinity = b.affinity;


-- Integration methods options
CREATE TABLE integration_methods (
	id serial,
	method_name text,
	active boolean,
	weight float
);

INSERT INTO integration_methods (method_name, active, weight)
     VALUES ('qgrams', 't', 1.0),
	    ('mdl', 't', 1.0),
	    ('ngrams', 't', 1.0),
	    ('val_qgrams', 'f',1.0),
	    ('dist', 't',1.0);

