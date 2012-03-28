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

-- Tables, views, and functions for name resolution

-- Housekeeping
DROP TABLE IF EXISTS nr_raw_results CASCADE;
DROP TABLE IF EXISTS nr_ncomp_results_tbl CASCADE;
DROP TABLE IF EXISTS nr_rwc_results;

CREATE OR REPLACE FUNCTION nr_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM nr_raw_results;
  DELETE FROM nr_ncomp_results_tbl;
END
$$ LANGUAGE plpgsql;


-- Tables and views for results and output
CREATE TABLE nr_raw_results (
       source_id INTEGER,
       field_id INTEGER,
       method_name TEXT,
       match_id INTEGER,
       score FLOAT
);

-- Determine top score for each field and method
CREATE VIEW nr_raw_max_scores AS
     SELECT field_id, method_name, MAX(score) score
       FROM nr_raw_results
   GROUP BY field_id, method_name;

-- Filter raw results for only top scoring matches
CREATE VIEW nr_raw_max_results AS
     SELECT r.*
       FROM nr_raw_results r, nr_raw_max_scores m
      WHERE r.score = m.score
	AND r.field_id = m.field_id
	AND r.method_name = m.method_name;

CREATE VIEW nr_raw_nice_results AS
     SELECT pdf.source_id, pdf.name, r.method_name, g.name AS match, r.score,
            (g.name = NULLIF(pdf.tag_code, 'NO_DISPLAY'))::boolean is_correct, pdf.tag_code correct 
       FROM nr_raw_max_results r
 INNER JOIN global_attributes g ON r.match_id = g.id
 INNER JOIN local_fields f ON r.field_id = f.id
 INNER JOIN local_sources s ON s.id = f.source_id
 INNER JOIN public.doit_fields pdf ON pdf.source_id = s.local_id::INTEGER AND pdf.name = f.local_name;

CREATE VIEW nr_field_count AS
     SELECT COUNT(NULLIF(tag_code, 'NO_DISPLAY')) AS "c"
       FROM public.doit_fields
      WHERE source_id IN (SELECT source_id FROM nr_raw_nice_results);

CREATE VIEW nr_raw_error_counts AS
     SELECT method_name,
     	    COUNT(*) n,
            COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END) n_correct,
	    COUNT(CASE WHEN is_correct OR is_correct IS NULL THEN NULL ELSE 1 END) n_wrong,
	    COUNT(*) - COUNT(is_correct) n_null
       FROM nr_raw_nice_results
   GROUP BY method_name;

CREATE VIEW nr_raw_error_rates AS
     SELECT *,
     	    n_correct::float / f.c AS "success_rate",
	    n_wrong::float / f.c AS "error_rate",
	    n_correct::float / n_wrong AS "ratio"
       FROM nr_raw_error_counts r
 INNER JOIN nr_field_count f
         ON 1 = 1;

-- ncomp: Naive composite scoring by summing over all raw results the
-- normalized scores times the method weight.
CREATE VIEW nr_ncomp_results AS
     SELECT r.source_id, r.field_id, r.match_id, SUM((r.score * m.weight)^2) score
       FROM nr_raw_results r, integration_methods m
      WHERE r.method_name = m.method_name
        AND m.active = true
   GROUP BY r.source_id, r.field_id, r.match_id;

CREATE TABLE nr_ncomp_results_tbl (
       source_id INTEGER,
       field_id INTEGER,
       match_id INTEGER,
       score FLOAT
);
CREATE INDEX idx_nr_ncomp_results_field_id ON nr_ncomp_results_tbl (field_id, score);

CREATE VIEW nr_ncomp_max_results AS
     SELECT r.*
       FROM nr_ncomp_results_tbl r
 INNER JOIN (SELECT field_id, MAX(score) score FROM nr_ncomp_results_tbl GROUP BY field_id) m
         ON r.field_id = m.field_id AND r.score = m.score;

CREATE VIEW nr_ncomp_nice_results AS
     SELECT pdf.source_id, pdf.name, g.name AS match, r.score,
            (g.name = NULLIF(pdf.tag_code, 'NO_DISPLAY'))::boolean is_correct, pdf.tag_code correct 
       FROM nr_ncomp_max_results r
 INNER JOIN global_attributes g ON r.match_id = g.id
 INNER JOIN local_fields f ON r.field_id = f.id
 INNER JOIN local_sources s ON s.id = f.source_id
 INNER JOIN public.doit_fields pdf ON pdf.source_id = s.local_id::INTEGER AND pdf.name = f.local_name;

CREATE VIEW nr_ncomp_error_rates AS
     SELECT f.c tot, COUNT(*) n,
            COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END) n_correct,
	    COUNT(CASE WHEN is_correct OR is_correct IS NULL THEN NULL ELSE 1 END) n_wrong,
	    COUNT(*) - COUNT(is_correct) n_null,
            COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END)::float / f.c success_rate,
	    COUNT(CASE WHEN is_correct OR is_correct IS NULL THEN NULL ELSE 1 END)::float / f.c error_rate,
            COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END)::float /
	    COUNT(CASE WHEN is_correct OR is_correct IS NULL THEN NULL ELSE 1 END) ratio
       FROM nr_ncomp_nice_results r, nr_field_count f
   GROUP BY f.c;


-- mcomp: Naive composite scoring by taking only maximum scoring matches
-- from individual methods, assigning weights to each method, taking max
-- composite scorers
CREATE VIEW nr_mcomp_results AS
     SELECT field_id, match_id, SUM(m.weight) score
       FROM nr_raw_max_results r, integration_methods m
      WHERE r.method_name = m.method_name
   GROUP BY field_id, match_id;

CREATE VIEW nr_mcomp_max_scores AS
     SELECT field_id, MAX(score) score
       FROM nr_mcomp_results
   GROUP BY field_id;

CREATE VIEW nr_mcomp_max_results AS
     SELECT r.*
       FROM nr_mcomp_results r, nr_mcomp_max_scores m
      WHERE r.score = m.score
        AND r.field_id = m.field_id;

CREATE VIEW nr_mcomp_nice_results AS
     SELECT pdf.source_id, pdf.name, g.name AS match, r.score,
            (g.name = NULLIF(pdf.tag_code, 'NO_DISPLAY'))::boolean is_correct, pdf.tag_code correct 
       FROM nr_mcomp_max_results r
 INNER JOIN global_attributes g ON r.match_id = g.id
 INNER JOIN local_fields f ON r.field_id = f.id
 INNER JOIN local_sources s ON s.id = f.source_id
 INNER JOIN public.doit_fields pdf ON pdf.source_id = s.local_id::INTEGER AND pdf.name = f.local_name;

CREATE VIEW nr_mcomp_error_rates AS
     SELECT f.c tot, COUNT(*) n,
            COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END) n_correct,
	    COUNT(CASE WHEN is_correct OR is_correct IS NULL THEN NULL ELSE 1 END) n_wrong,
	    COUNT(*) - COUNT(is_correct) n_null,
            COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END)::float / f.c success_rate,
	    COUNT(CASE WHEN is_correct OR is_correct IS NULL THEN NULL ELSE 1 END)::float / f.c error_rate,
            COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END)::float /
	    COUNT(CASE WHEN is_correct OR is_correct IS NULL THEN NULL ELSE 1 END) ratio
       FROM nr_mcomp_nice_results r, nr_field_count f
   GROUP BY f.c;


-- Load results for composite scoring methods
CREATE OR REPLACE FUNCTION nr_composite_load() RETURNS void AS
$$
BEGIN
  TRUNCATE nr_ncomp_results_tbl;
  INSERT INTO nr_ncomp_results_tbl
       SELECT *
       	 FROM nr_ncomp_results;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION nr_test (INTEGER, INTEGER) RETURNS VOID AS
$$
BEGIN
  RAISE INFO 'nr_test: Importing test data...';
  PERFORM import_random($1, $2);

  PERFORM qgrams_clean();
  PERFORM mdl_clean();
  PERFORM ngrams_clean();
  PERFORM dist_clean();

  PERFORM preprocess_all();

  RAISE INFO 'done.  Preprocessing...';
  PERFORM preprocess_global();

  RAISE INFO 'done.  Computing results...';
  PERFORM nr_results_for_all_unmapped();

  RAISE INFO 'done.';
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION nr_results_for_all () RETURNS VOID AS
$$
BEGIN
  PERFORM qgrams_results_for_all();
  PERFORM dist_results_for_all();
  PERFORM mdl_results_for_all();
  PERFORM ngrams_results_for_all();
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nr_results_for_all_unmapped () RETURNS VOID AS
$$
BEGIN
  PERFORM qgrams_results_for_all_unmapped();
  PERFORM dist_results_for_all_unmapped();
  PERFORM mdl_results_for_all_unmapped();
  PERFORM ngrams_results_for_all_unmapped();
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nr_results_for_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_source_id ALIAS FOR $1;
BEGIN
  PERFORM qgrams_results_for_source(test_source_id);
  PERFORM dist_results_for_source(test_source_id);
  PERFORM mdl_results_for_source(test_source_id);
  PERFORM ngrams_results_for_source(test_source_id);
END
$$ LANGUAGE plpgsql;


/* Probably broken:

CREATE TABLE nr_rwc_results (
       composite text,
       weights text,
       tot integer,
       n integer,
       n_correct integer,
       n_wrong integer,
       n_null integer,
       n_ambig integer,
       success_rate float,
       error_rate float,
       ratio float
);

-- Perform $1 composite analyses of the results with different random weights for each one
-- Resulting error rates are put in nr_rwc_results
CREATE OR REPLACE FUNCTION nr_random_weight_composite_test(integer) RETURNS void AS
$$
DECLARE
  i integer := 0;
  w_mdl float;
  w_att_qgrams float;
  w_val_ngrams float;
  w_val_qgrams float;
  w_dist float;
  w_str text;
BEGIN
  -- Loop $1 times
  WHILE (i < $1) LOOP
    i := i + 1;

    -- Get some random weights
    w_mdl := random()::numeric;
    w_att_qgrams := random()::numeric;
    w_val_ngrams := random()::numeric;
    w_val_qgrams := random()::numeric;
    w_dist := random()::numeric;

    UPDATE integration_methods SET weight = w_mdl WHERE method_name = 'mdl';
    UPDATE integration_methods SET weight = w_att_qgrams WHERE method_name = 'att_qgrams';
    UPDATE integration_methods SET weight = w_val_ngrams WHERE method_name = 'val_ngrams';
    UPDATE integration_methods SET weight = w_val_qgrams WHERE method_name = 'val_qgrams';
    UPDATE integration_methods SET weight = w_dist WHERE method_name = 'dist';

    DELETE FROM nr_ncomp_results_tbl;
    INSERT INTO nr_ncomp_results_tbl SELECT * FROM nr_ncomp_results;

    w_str := (round(w_mdl,3)::text || ',' || round(w_att_qgrams,3) || ',' || round(w_val_ngrams,3) || ',' || round(w_val_qgrams,3) || ',' || round(w_dist,3));

    INSERT INTO nr_rwc_results (composite, weights, tot, n, n_correct, n_wrong, n_null, success_rate, error_rate, ratio)
         SELECT 'n', w_str, *
	   FROM nr_ncomp_error_rates;

    INSERT INTO nr_rwc_results (composite, weights, tot, n, n_correct, n_wrong, n_null, success_rate, error_rate, ratio)
         SELECT 'm', w_str, *
	   FROM nr_mcomp_error_rates;

  END LOOP;
END
$$ LANGUAGE plpgsql;

*/
