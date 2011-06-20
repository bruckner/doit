-- Tables, views, and functions for name resolution


-- Housekeeping
DROP TABLE IF EXISTS nr_raw_results CASCADE;


-- Tables and views for results and output
CREATE TABLE nr_raw_results (
       source_id integer,
       name text,
       method_name text,
       match integer,
       score float
);

CREATE VIEW nr_desc_mdl_results AS
     SELECT source_id, name, match, (-1 * score) score
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

CREATE VIEW nr_raw_nice_results AS
     SELECT r.source_id, r.name, g.name AS match, r.score,
            (g.name = f.tag_code)::boolean is_correct, f.tag_code correct 
       FROM nr_raw_max_results r, global_attributes g, public.doit_fields f
      WHERE r.match = g.id
        AND r.name = f.name
	AND r.source_id = f.source_id;

CREATE VIEW nr_raw_error_rates AS
     SELECT method_name, COUNT(*) n,
            COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END) n_correct,
	    COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) n_wrong,
	    COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END)::float / COUNT(*)::float error_rate
       FROM nr_raw_nice_results
   GROUP BY method_name;

CREATE VIEW nr_composite_results AS
     SELECT source_id, name, match, SUM(m.weight) score
       FROM nr_raw_max_results r, integration_methods m
      WHERE r.method_name = m.method_name
   GROUP BY source_id, name, match;

CREATE VIEW nr_composite_max_scores AS
     SELECT source_id, name, MAX(score) score
       FROM nr_composite_results
   GROUP BY source_id, name;

CREATE VIEW nr_composite_max_results AS
     SELECT r.*
       FROM nr_composite_results r, nr_composite_max_scores m
      WHERE r.score = m.score
        AND r.name = m.name
	AND r.source_id = m.source_id;

CREATE VIEW nr_composite_nice_results AS
     SELECT r.source_id, r.name, g.name AS match, r.score,
            (g.name = f.tag_code)::boolean is_correct, f.tag_code correct 
       FROM nr_composite_max_results r, global_attributes g, public.doit_fields f
      WHERE r.match = g.id
        AND r.name = f.name
	AND r.source_id = f.source_id;

CREATE VIEW nr_composite_error_rates AS
     SELECT COUNT(*) n,
            COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END) n_correct,
	    COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) n_wrong,
	    COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END)::float / COUNT(*)::float error_rate
       FROM nr_composite_nice_results;



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

CREATE OR REPLACE FUNCTION nr_test (integer, integer) RETURNS void AS
$$
BEGIN
  PERFORM staging_load($1, $2);
  PERFORM nr_compute_results();
END
$$ LANGUAGE plpgsql;


DROP TABLE IF EXISTS nr_rwc_results;
CREATE TABLE nr_rwc_results (
       weights text,
       n integer,
       n_correct integer,
       n_wrong integer,
       error_rate float
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

    INSERT INTO nr_rwc_results (weights, n, n_correct, n_wrong, error_rate)
         SELECT (round(w_mdl,3)::text || ',' || round(w_att_qgrams,3) || ',' || round(w_val_ngrams,3) || ',' || round(w_val_qgrams,3) || ',' || round(w_dist,3)), *
	   FROM nr_composite_error_rates;

  END LOOP;
END
$$ LANGUAGE plpgsql;