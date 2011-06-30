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
  DELETE FROM nr_rwc_results;
END
$$ LANGUAGE plpgsql;


-- Tables and views for results and output
CREATE TABLE nr_raw_results (
       source_id integer,
       name text,
       method_name text,
       match integer,
       score float
);

CREATE VIEW nr_field_count AS
     SELECT COUNT(NULLIF(tag_code,'NO_DISPLAY')) c
       FROM public.doit_fields
      WHERE source_id IN (SELECT source_id FROM nr_raw_results);

-- desc: Make such MAX(score) is tops
CREATE VIEW nr_desc_mdl_results AS
     SELECT source_id, name, match, (1.0 / (score + 1.0)) score
       FROM nr_raw_results
      WHERE method_name = 'mdl';

CREATE VIEW nr_desc_dist_results AS
     SELECT source_id, name, match, (1 - score) score
       FROM nr_raw_results
      WHERE method_name = 'dist';

CREATE VIEW nr_desc_results AS
     SELECT *
       FROM nr_raw_results
      WHERE method_name != 'mdl'
        AND method_name != 'dist'
      UNION
     SELECT source_id, name, 'mdl' method_name, match, score
       FROM nr_desc_mdl_results
      UNION
     SELECT source_id, name, 'dist' method_name, match, score
       FROM nr_desc_dist_results;

CREATE VIEW nr_raw_max_scores AS
     SELECT source_id, name, method_name, MAX(score) score
       FROM nr_desc_results
   GROUP BY source_id, name, method_name;

CREATE VIEW nr_raw_max_results AS
     SELECT r.*
       FROM nr_desc_results r, nr_raw_max_scores m
      WHERE r.score = m.score
        AND r.name = m.name
	AND r.source_id = m.source_id
	AND r.method_name = m.method_name;

-- norm: Scores all in [0,1] -- only MDL needs to be norm'd
CREATE VIEW nr_norm_results AS
     SELECT *
       FROM nr_desc_results
      WHERE method_name != 'mdl'
      UNION
     SELECT r.source_id, r.name, 'mdl', r.match, r.score::float / m.score
       FROM nr_desc_results r, nr_raw_max_results m
      WHERE r.method_name = 'mdl'
        AND m.method_name = 'mdl'
        AND r.source_id = m.source_id
	AND r.name = m.name;

CREATE VIEW nr_norm_max_scores AS
     SELECT source_id, name, method_name, MAX(score) score
       FROM nr_norm_results
    GROUP BY source_id, name, method_name;

CREATE VIEW nr_norm_max_results AS
     SELECT r.*
       FROM nr_norm_results r, nr_norm_max_scores m
      WHERE r.score = m.score
        AND r.name = m.name
	AND r.source_id = m.source_id
	AND r.method_name = m.method_name;

CREATE VIEW nr_raw_nice_results AS
     SELECT r.source_id, r.name, r.method_name, g.name AS match, r.score,
            (g.name = NULLIF(f.tag_code,'NO_DISPLAY'))::boolean is_correct, f.tag_code correct 
       FROM nr_norm_max_results r, global_attributes g, public.doit_fields f
      WHERE r.match = g.id
        AND r.name = f.name
	AND r.source_id = f.source_id;

CREATE VIEW nr_raw_ambig_count AS
     SELECT method_name, SUM(n) c
       FROM (
       	    SELECT method_name, COUNT(*) n 
	      FROM nr_raw_max_results
	  GROUP BY source_id, name, method_name
	    ) t
      WHERE t.n > 1
   GROUP BY method_name;

CREATE VIEW nr_raw_error_counts AS
     SELECT method_name, COUNT(*) n,
            COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END) n_correct,
	    COUNT(CASE WHEN is_correct OR is_correct IS NULL THEN NULL ELSE 1 END) n_wrong,
	    COUNT(*) - COUNT(is_correct) n_null
       FROM nr_raw_nice_results
   GROUP BY method_name;

CREATE VIEW nr_raw_error_rates AS
     SELECT *, n_correct::float / f.c success_rate, n_wrong::float / f.c error_rate, n_correct::float / n_wrong ratio
       FROM nr_raw_error_counts r
 INNER JOIN nr_field_count f
         ON 1 = 1;

-- ncomp: Naive composite scoring by summing over all raw results the
-- normalized scores times the method weight.
CREATE VIEW nr_ncomp_results AS
     SELECT r.source_id, r.name, r.match, SUM((r.score * m.weight)^2) score
       FROM nr_norm_results r, integration_methods m
      WHERE r.method_name = m.method_name
   GROUP BY r.source_id, r.name, r.match;

CREATE TABLE nr_ncomp_results_tbl (
       source_id integer,
       name text,
       match integer,
       score float
);
CREATE INDEX idx_nr_ncomp_results_source_id ON nr_ncomp_results_tbl (source_id, name, score);

CREATE VIEW nr_ncomp_max_scores AS
     SELECT source_id, name, MAX(score) score
       FROM nr_ncomp_results_tbl
   GROUP BY source_id, name;

CREATE VIEW nr_ncomp_max_results AS
     SELECT r.*
       FROM nr_ncomp_results_tbl r, nr_ncomp_max_scores m
      WHERE r.score = m.score
        AND r.name = m.name
	AND r.source_id = m.source_id;

CREATE VIEW nr_ncomp_nice_results AS
     SELECT r.source_id, r.name, g.name AS match, r.score,
            (g.name = NULLIF(f.tag_code,'NO_DISPLAY'))::boolean is_correct, f.tag_code correct 
       FROM nr_ncomp_max_results r, global_attributes g, public.doit_fields f
      WHERE r.match = g.id
        AND r.name = f.name
	AND r.source_id = f.source_id;

CREATE VIEW nr_ncomp_ambig_count AS
     SELECT SUM(n) c
       FROM (SELECT COUNT(*) n FROM nr_ncomp_nice_results GROUP BY source_id, name) t
      WHERE t.n > 1;

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
     SELECT source_id, name, match, SUM(m.weight) score
       FROM nr_raw_max_results r, integration_methods m
      WHERE r.method_name = m.method_name
   GROUP BY source_id, name, match;

CREATE VIEW nr_mcomp_max_scores AS
     SELECT source_id, name, MAX(score) score
       FROM nr_mcomp_results
   GROUP BY source_id, name;

CREATE VIEW nr_mcomp_max_results AS
     SELECT r.*
       FROM nr_mcomp_results r, nr_mcomp_max_scores m
      WHERE r.score = m.score
        AND r.name = m.name
	AND r.source_id = m.source_id;

CREATE VIEW nr_mcomp_nice_results AS
     SELECT r.source_id, r.name, g.name AS match, r.score,
            (g.name = NULLIF(f.tag_code,'NO_DISPLAY'))::boolean is_correct, f.tag_code correct 
       FROM nr_mcomp_max_results r, global_attributes g, public.doit_fields f
      WHERE r.match = g.id
        AND r.name = f.name
	AND r.source_id = f.source_id;

CREATE VIEW nr_mcomp_ambig_count AS
     SELECT SUM(n) c
       FROM (SELECT COUNT(*) n FROM nr_mcomp_nice_results GROUP BY source_id, name) t
      WHERE t.n > 1;

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


-- UDF to run all active processing methods
CREATE OR REPLACE FUNCTION nr_compute_results () RETURNS void AS
$$
BEGIN

  IF EXISTS (SELECT 1 FROM integration_methods WHERE method_name = 'att_qgrams' AND active = 't') THEN
    RAISE INFO 'Performing attribute synonym matching with qgrams...';
    PERFORM syn_load_results();
    RAISE INFO '  done.';
  END IF;

  IF EXISTS (SELECT 1 FROM integration_methods WHERE method_name = 'mdl' AND active = 't') THEN
    RAISE INFO 'Performing MDL dictionary matching...';
    PERFORM mdl_load_results();
    RAISE INFO '  done.';
  END IF;

  IF EXISTS (SELECT 1 FROM integration_methods WHERE method_name = 'val_qgrams' AND active = 't') THEN
    RAISE INFO 'Performing value-set matching with qgrams...';
    PERFORM val_qgrams_stage();
    PERFORM val_qgrams_results();
    RAISE INFO '  done.';
  END IF;

  IF EXISTS (SELECT 1 FROM integration_methods WHERE method_name = 'val_ngrams' AND active = 't') THEN
    RAISE INFO 'Performing value-set matching with ngrams...';
    PERFORM val_ngrams_stage();
    PERFORM val_ngrams_results();
    RAISE INFO '  done.';
  END IF;

  IF EXISTS (SELECT 1 FROM integration_methods WHERE method_name = 'dist' AND active = 't') THEN
    RAISE INFO 'Performing value-set distribution t-test matching...';
    PERFORM dist_results();
    RAISE INFO '  done.';
  END IF;

END
$$ LANGUAGE plpgsql;


-- Load results for composite scoring methods
CREATE OR REPLACE FUNCTION nr_composite_load() RETURNS void AS
$$
BEGIN
  INSERT INTO nr_ncomp_results_tbl (source_id, name, match, score)
       SELECT * FROM nr_ncomp_results;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION nr_test (integer, integer) RETURNS void AS
$$
BEGIN
  PERFORM staging_load($1, $2);
  PERFORM nr_compute_results();
END
$$ LANGUAGE plpgsql;


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