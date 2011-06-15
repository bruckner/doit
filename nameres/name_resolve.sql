-- Tables, views, and functions for name resolution


CREATE TABLE nr_raw_results (
       source_id integer,
       name text,
       method_name text,
       match integer,
       score float
);




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
