/* entity.sql
 *
 * Goal:  Implement an entity similarity function for testing.
 * Desired characteristics:  Want to be able to identify a subset of
 * entities, and quickly compute all pairwise similarities within
 * that group.
 *
 * HOW TO USE AS OF 12 APR 2012:
 *   Insert ids of entities to compare into entity_test_group.  Run
 *   entities_preprocess_test_group(), then entities_results_for_test_group().
 *   entity-wise similarities are in entity_similarities, and attribute-wise
 *   ones are in entity_field_cosine_similarities.
 *
 *   Attribute weights are kept in entity_field_weights.
 *
 * Daniel Bruckner, April 2012
 */

CREATE OR REPLACE FUNCTION entities_clean () RETURNS VOID AS
$$
BEGIN
  DROP TABLE entity_tokens CASCADE;
  DROP TABLE entity_tokens_stats CASCADE;
  DROP TABLE entity_field_norms;
END
$$ LANGUAGE plpgsql;


/* Attribute-wise cosine similarity tables, views, and functions */
CREATE VIEW entity_tokens_raw AS
     SELECT entity_id, field_id, qgrams2(value, 3) token
       FROM local_data
      WHERE value IS NOT NULL AND value != '';

CREATE TABLE entity_tokens (
        entity_id INTEGER,
        field_id INTEGER,
        token TEXT,
        count NUMERIC,
        tf FLOAT
);

CREATE TABLE entity_field_norms (
        entity_id INTEGER,
        field_id INTEGER,
        norm FLOAT
);

CREATE TABLE entity_field_cosine_similarity (
        entity_a INTEGER,
        entity_b INTEGER,
        field_id INTEGER,
        similarity FLOAT
);

CREATE TABLE entity_similarities (
        entity_a INTEGER,
        entity_b INTEGER,
        raw_similarity FLOAT,
        similarity FLOAT,
        human_label TEXT
);

CREATE TABLE entity_field_weights (
        field_id INTEGER,
        weight FLOAT
);

CREATE TABLE entity_test_group (
        entity_id INTEGER PRIMARY KEY
);



CREATE OR REPLACE FUNCTION entities_preprocess_all () RETURNS VOID AS
$$
BEGIN
  INSERT INTO entity_tokens
       SELECT entity_id, field_id, token, COUNT(*), LOG(COUNT(*) + 1) tf
         FROM entity_tokens_raw
     GROUP BY entity_id, field_id, token;

  INSERT INTO entity_field_norms
       SELECT entity_id, field_id, SQRT(SUM(tf^2)) norm
         FROM entity_tokens
     GROUP BY entity_id, field_id;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION entities_preprocess_category () RETURNS VOID AS
$$
BEGIN
  NULL; /* To be implemented... */
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION entities_preprocess_test_group (BOOLEAN) RETURNS VOID AS
$$
DECLARE
  do_clean ALIAS FOR $1;
BEGIN
  IF do_clean THEN
    DELETE FROM entity_tokens WHERE entity_id IN (SELECT * FROM entity_test_group);
    DELETE FROM entity_field_norms WHERE entity_id IN (SELECT * FROM entity_test_group);
  END IF;

  ANALYZE entity_test_group; /* Make sure planner gets good statistics */

  CREATE TEMP TABLE entity_test_data AS
       SELECT entity_id, field_id, value
         FROM local_data
        WHERE entity_id IN (SELECT * FROM entity_test_group)
          AND value IS NOT NULL AND value != '';
RAISE INFO 'Got data.';
  CREATE TEMP TABLE entity_test_tokens_raw AS
       SELECT entity_id, field_id, qgrams2(value, 3) token
         FROM entity_test_data;
RAISE INFO 'Got tokens.';
  INSERT INTO entity_tokens
       SELECT entity_id, field_id, token, COUNT(*), LOG(COUNT(*) + 1) tf
         FROM entity_test_tokens_raw
     GROUP BY entity_id, field_id, token;
RAISE INFO 'Got tf.';
  INSERT INTO entity_field_norms
       SELECT entity_id, field_id, SQRT(SUM(tf^2)) norm
         FROM entity_tokens
        WHERE entity_id IN (SELECT * FROM entity_test_group)
     GROUP BY entity_id, field_id;

  DROP TABLE entity_test_data;
  DROP TABLE entity_test_tokens_raw;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION entities_results_for_test_group () RETURNS VOID AS
$$
DECLARE
  entity_size FLOAT;
BEGIN
  /* 2 steps: i. compute attribute-wise similarities;
   *          ii. compute over similarity
   * Assumes preprocessing already done on test group, i.e., tokenization
   * is done and norms computed.
   */

  /* Attribute-wise cosine similarity */

  /* Do some caching to make cos-sim query smaller */
  CREATE TEMP TABLE test_tokens AS
       SELECT * FROM entity_tokens
        WHERE entity_id IN (SELECT * FROM entity_test_group);
RAISE INFO 'Got tokens.';
  CREATE TEMP TABLE test_norms AS
       SELECT * FROM entity_field_norms
        WHERE entity_id IN (SELECT * FROM entity_test_group);
RAISE INFO 'Got norms.';
  /* Clean out any old calculations */
  /* This might be crazy slow... */
  DELETE FROM entity_field_cosine_similarity
        WHERE entity_a IN (SELECT * FROM entity_test_group)
           OR entity_b IN (SELECT * FROM entity_test_group);
RAISE INFO 'Got clean.';
  INSERT INTO entity_field_cosine_similarity
       SELECT a.entity_id, b.entity_id, a.field_id,
              SUM(a.tf * b.tf) / (na.norm * nb.norm) sim
         FROM test_tokens a, test_tokens b, test_norms na, test_norms nb
        WHERE a.entity_id != b.entity_id
          AND a.field_id = b.field_id
          AND a.token = b.token
          AND a.entity_id = na.entity_id
          AND b.entity_id = nb.entity_id
          AND a.field_id = na.field_id
          AND a.field_id = nb.field_id
     GROUP BY a.entity_id, b.entity_id, a.field_id, na.norm, nb.norm;
RAISE INFO 'Got cosine similarity.';
  DROP TABLE test_tokens;
  DROP TABLE test_norms;

  /* Overall entity-wise similarity */

  entity_size := SUM(weight) FROM entity_field_weights;

  /* Clean out any old calculations */
  DELETE FROM entity_similarity
        WHERE entity_a IN (SELECT * FROM entity_test_group)
           OR entity_b IN (SELECT * FROM entity_test_group);

  INSERT INTO entity_similarity
       SELECT cs.entity_a, cs.entity_b, SUM(fw.weight * cs.similarity),
              SUM(fw.weight * cs.similarity) / entity_size
         FROM entity_field_cosine_similarity cs, entity_field_weights fw
        WHERE cs.field_id = fw.field_id
     GROUP BY cs.entity_a, cs.entity_b;
END
$$ LANGUAGE plpgsql;
