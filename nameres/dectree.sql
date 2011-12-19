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

-- A place to store feature records of attribute comparisons
CREATE TABLE attribute_pair_features (
       id SERIAL,
       local_id INTEGER,
       global_id INTEGER,
       mdl FLOAT,
       qgrams FLOAT,
       ngrams FLOAT,
       dist FLOAT
);

CREATE VIEW attribute_pair_svecs AS
     SELECT local_id, global_id, id,
            ('{1,1,1,1}:{' || mdl || ','
                           || qgrams || ','
                           || ngrams || ','
                           || dist || '}')::MADlib.SVEC AS "features"
       FROM attribute_pair_features;

CREATE TABLE dt_training_source (
       id INTEGER,
       features MADlib.SVEC,
       class INTEGER
);

CREATE TABLE classified_points (
       id INTEGER,
       features MADlib.SVEC,
       jump INTEGER,
       class INTEGER,
       prob FLOAT
);

CREATE VIEW howd_it_go AS
     SELECT lf.local_name, ga.name, df.tag_code, cp.class, cp.prob
       FROM public.doit_fields df, local_sources ls, local_fields lf,
            global_attributes ga, attribute_pair_features apf,
            classified_points cp
      WHERE apf.id = cp.id 
        AND apf.local_id = lf.id
        AND apf.global_id = ga.id
        AND ls.id = lf.source_id
        AND df.source_id = ls.local_id::INTEGER
        AND df.name = lf.local_name
   ORDER BY cp.prob DESC;

CREATE VIEW dectree_stats AS
     SELECT COUNT(tag_code) AS "total_classifications",
            COUNT(CASE WHEN tag_code = name THEN tag_code ELSE NULL END) AS "total_fields",
            COUNT(CASE WHEN class = 0 THEN tag_code ELSE NULL END) AS "n_accepted",
            COUNT(CASE WHEN class = 1 THEN tag_code ELSE NULL END) AS "n_rejected",
            COUNT(CASE WHEN class = 0 AND tag_code != name THEN tag_code ELSE NULL END) AS "n_false_pos",
            COUNT(CASE WHEN class = 1 AND tag_code = name THEN tag_code ELSE NULL END) AS "n_false_neg"
       FROM howd_it_go;

CREATE VIEW dectree_pandr AS
     SELECT (n_accepted - n_false_pos)::FLOAT / total_fields AS "recall",
            (n_accepted - n_false_pos)::FLOAT / n_accepted AS "precision"
       FROM dectree_stats;

CREATE VIEW dectree_fmeas AS
     SELECT 2 * precision * recall / (precision + recall)
       FROM dectree_pandr;


-- Collect feature values for attribute comparisons
CREATE OR REPLACE FUNCTION load_features () RETURNS VOID AS
$$
BEGIN
  TRUNCATE TABLE attribute_pair_features;

  -- Create empty feature records for all non-zero comparisons
  INSERT INTO attribute_pair_features (local_id, global_id, mdl, qgrams, ngrams, dist)
       SELECT field_id, match_id, 0, 0, 0, 0
         FROM nr_raw_results
     GROUP BY field_id, match_id;

  -- Get MDL scores
  UPDATE attribute_pair_features apf
     SET mdl = score
    FROM nr_raw_results nrr
   WHERE nrr.method_name = 'mdl'
     AND nrr.field_id = apf.local_id
     AND nrr.match_id = apf.global_id;

  -- Get qgrams scores
  UPDATE attribute_pair_features apf
     SET qgrams = score
    FROM nr_raw_results nrr
   WHERE nrr.method_name = 'qgrams'
     AND nrr.field_id = apf.local_id
     AND nrr.match_id = apf.global_id;

  -- Get ngrams scores
  UPDATE attribute_pair_features apf
     SET ngrams = score
    FROM nr_raw_results nrr
   WHERE nrr.method_name = 'ngrams'
     AND nrr.field_id = apf.local_id
     AND nrr.match_id = apf.global_id;

  -- Get distibution scores
  UPDATE attribute_pair_features apf
     SET dist = score
    FROM nr_raw_results nrr
   WHERE nrr.method_name = 'dist'
     AND nrr.field_id = apf.local_id
     AND nrr.match_id = apf.global_id;

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION train_dt_model () RETURNS VOID AS
$$
BEGIN
  INSERT INTO dt_training_source (id, features, class)
       SELECT aps.id, aps.features,
              CASE WHEN am.local_id IS NOT NULL THEN 1 ELSE 0 END
         FROM attribute_pair_svecs aps
    LEFT JOIN attribute_mappings am
           ON aps.local_id = am.local_id
          AND aps.global_id = am.global_id;

  PERFORM MADlib.train_tree( 'dan.dt_training_source', 'id', 'features', 'class', 2, 500);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dt_classify () RETURNS VOID AS
$$
BEGIN
  DROP TABLE IF EXISTS classified_points1;
  DROP TABLE IF EXISTS classified_points2;
  PERFORM * FROM MADlib.classify_tree( 'dan.attribute_pair_svecs', 'id', 'features', 2 );
  TRUNCATE TABLE classified_points;
  INSERT INTO classified_points SELECT * FROM MADlib.classified_points;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dt_model_test (INTEGER, INTEGER, INTEGER, INTEGER) RETURNS VOID AS
$$
BEGIN
  PERFORM clean_house();
  PERFORM training_load($1, $2);
  PERFORM preprocess_all();
  PERFORM preprocess_global();

  PERFORM nr_results_for_all();

  PERFORM load_features();
  PERFORM train_dt_model();

  TRUNCATE nr_raw_results;

  PERFORM nr_test($3, $4);

  PERFORM load_features();
  PERFORM dt_classify();
END
$$ LANGUAGE plpgsql;

