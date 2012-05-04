/* entity.sql
 *
 * Goal:  Implement an entity similarity function for testing.
 * Desired characteristics:  Want to be able to identify a subset of
 * entities, and quickly compute all pairwise similarities within
 * that group.
 *
 * HOW TO USE AS OF 4 MAY 2012:
 *  1. Test groups.  The code is written to process subsets of an overall
 *     data set.  Entities of interest are highlighted by inserting their
 *     IDs in the table entity_test_group.
 *
 *  2. Preprocessing.  All entity data must be preprocessed before analysis.
 *     preprocessing involves tokenization and computation of parameters for
 *     cosine similarity computation.  Use entities_preprocess_all to 
 *     preprocess all entity data in local_data at once, or use
 *     entities_preprocess_test_group to focus on a subset.
 *
 *  3. Cosine similarity.  Attribute-wise similarity scores are computed as
 *     qgram cosine similarities.  Compute these scores for a subset with
 *     entities_field_similarities_for_test_group().
 *
 *  4. Weight learning.  Overall entity-wise similarities are computed
 *     as weighted sums of attribute-wise ones.  Weights can be learned
 *     from training data.  Identify a set of entities to use for training,
 *     and put them in entity_test_group.  All pairs of these entities will
 *     be used for training, so any of these pairs that are true matches
 *     should appear in the entity_matches table.  If necessary, compute
 *     the cosine similarities for the training set (step 3 above). Then
 *     use entities_weights_from_test_group to learn the weights.
 *
 *  5. Overall similarity.  Put the entities you would like to compare
 *     into a test group.  Make sure weights have been set for all
 *     attributes in entity_field_weights.  Then run
 *     entities_results_for_test_group().  Similarity scores will be
 *     output to the entity_similarities table.
 *
 * Daniel Bruckner, 2012
 *
 * Copyright (c) 2011 Massachusetts Institute of Technology
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

CREATE OR REPLACE FUNCTION entities_clean () RETURNS VOID AS
$$
BEGIN
  DROP TABLE entity_tokens CASCADE;
  DROP TABLE entity_field_norms CASCADE;
  DROP TABLE entity_field_cosine_similaries CASCADE;
  DROP TABLE entity_similarities CASCADE;
  DROP TABLE entity_field_weights CASCADE;
  DROP TABLE entity_test_group CASCADE;
END
$$ LANGUAGE plpgsql;


/* Attribute-wise cosine similarity relations */
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

CREATE TABLE entity_field_cosine_similarities (
        entity_a INTEGER,
        entity_b INTEGER,
        field_id INTEGER,
        similarity FLOAT
);


/* Overall entity-to-entity similarity */
CREATE TABLE entity_similarities (
        entity_a INTEGER,
        entity_b INTEGER,
        raw_similarity FLOAT,
        similarity FLOAT,
        human_label TEXT
);

/* Regression weights */
CREATE TABLE entity_field_weights (
        field_id INTEGER,
        weight FLOAT
);

/* Known matches */
CREATE TABLE entity_matches (
        entity_a INTEGER,
        entity_b INTEGER
);

/* Test group table -- auxiliary table for function input */
CREATE TABLE entity_test_group (
        entity_id INTEGER PRIMARY KEY
);


/* Preprocessing is two steps: i. extract tokens (qgrams, q=3) from
 * data values and compute token frequecies (TF) per entity per
 * field.  NB: For qgrams, TF weights are computed as a logarithm
 * instead of a ratio, because documents are assumed to be short.
 * ii. Compute the norm of the TF-vectors for each entity+field.
 */
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


/* Use test_group cosine similarity and matches results to train field weights.
 * Attribute-wise cosine similarities must already be computed for test_group
 * crossed with itself.
 */
CREATE OR REPLACE FUNCTION entities_weights_from_test_group () RETURNS VOID AS
$$
BEGIN
  TRUNCATE entity_field_weights;

  /* Test group gives list of entities. Training pairs are the
   * cross product of test group with itself, i.e., every test
   * entity paired with every other test entity.
   */
  CREATE TEMP TABLE training_pairs AS
       SELECT a.entity_id entity_a, b.entity_id entity_b, 'f'::BOOLEAN is_match
         FROM entity_test_group a, entity_test_group b
        WHERE a.entity_id < b.entity_id;

  UPDATE training_pairs p
     SET is_match = 't'::BOOLEAN
    FROM entity_matches m
   WHERE p.entity_a = m.entity_a
     AND p.entity_b = m.entity_b;

  /* Compute average similarities given match or mismatch. */
  CREATE TEMP TABLE training_stats AS
       SELECT field_id,
              COUNT(CASE WHEN is_match THEN 1 ELSE NULL END) n_match,
              COUNT(CASE WHEN is_match THEN NULL ELSE 1 END) n_mismatch,
              SUM(CASE WHEN is_match THEN s.similarity ELSE 0 END) sum_match,
              SUM(CASE WHEN is_match THEN 0 ELSE s.similarity END) sum_mismatch,
              NULL::FLOAT avg_match, NULL::FLOAT avg_mismatch
         FROM entity_field_cosine_similarities s, training_pairs p
        WHERE s.entity_a = p.entity_a
          AND s.entity_b = p.entity_b
     GROUP BY field_id;

  UPDATE training_stats
     SET avg_match = sum_match::FLOAT / n_match,
         avg_mismatch = sum_mismatch::FLOAT / n_mismatch;

  INSERT INTO entity_field_weights
       SELECT field_id, entities_weight_formula(avg_match, avg_mismatch);
         FROM training_stats;

  DROP TABLE training_pairs;
  DROP TABLE training_stats;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION entities_weight_formula(FLOAT, FLOAT) RETURNS FLOAT AS
$$
DECLARE
  avg_match ALIAS FOR $1;
  avg_mismatch ALIAS FOR $2;
BEGIN
  RETURN avg_match / avg_mismatch;
END
$$ LANGUAGE plpgsql;


/* Attribute-wise cosine similarity.  Assumes preprocessing already
 * done on test group, i.e., tokenization is done and norms computed.
 */
CREATE OR REPLACE FUNCTION entities_field_similarities_for_test_group () RETURNS VOID AS
$$
BEGIN
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
  DELETE FROM entity_field_cosine_similarities
        WHERE entity_a IN (SELECT * FROM entity_test_group)
           OR entity_b IN (SELECT * FROM entity_test_group);
  RAISE INFO 'Got clean.';

  INSERT INTO entity_field_cosine_similarities
       SELECT a.entity_id, b.entity_id, a.field_id,
              SUM(a.tf * b.tf) / (na.norm * nb.norm)
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
END
$$ LANGUAGE plpgsql;


/* Compute overall entity-wise similarities */
CREATE OR REPLACE FUNCTION entities_results_for_test_group (BOOLEAN) RETURNS VOID AS
$$
DECLARE
  compute_cossim ALIAS FOR $1;
  entity_size FLOAT;
BEGIN
  IF compute_cossim THEN
    PERFORM entities_field_similarities_for_test_group();
  END IF;

  entity_size := SUM(weight) FROM entity_field_weights;

  /* Clean out any old calculations */
  DELETE FROM entity_similarities
        WHERE entity_a IN (SELECT * FROM entity_test_group)
           OR entity_b IN (SELECT * FROM entity_test_group);

  /* Overall similarity is the weighted sum of attribute-wise similarities,
   * normalized by the sum of all weights (i.e. the max possible score).
   */
  INSERT INTO entity_similarities
       SELECT cs.entity_a, cs.entity_b, SUM(fw.weight * cs.similarity),
              SUM(fw.weight * cs.similarity) / entity_size
         FROM entity_field_cosine_similarities cs, entity_field_weights fw
        WHERE cs.field_id = fw.field_id
     GROUP BY cs.entity_a, cs.entity_b;
END
$$ LANGUAGE plpgsql;
