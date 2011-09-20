
-- Housekeeping
DROP TABLE IF EXISTS local_dist_stats CASCADE;
DROP VIEW IF EXISTS local_dist_sums CASCADE;

CREATE OR REPLACE FUNCTION dist_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM local_dist_stats;
END
$$ LANGUAGE plpgsql;


-- Welch's t-test for comparing samples with unequal size and variance
CREATE OR REPLACE FUNCTION dist_t_test (
       n1 bigint, n2 bigint,
       m1 float,   m2 float,
       v1 float,   v2 float
) RETURNS float AS
$$
import math
import sys

try:
    import stats
except ImportError:
    # workaround for system without standard install of pystats
    sys.path.append('/usca/home/bruckda1/pymods')
    import stats

if (n1 < 2 or n2 < 2):
   return 2;

s1 = v1 / n1
s2 = v2 / n2

if (s1 <= 0 or s2 <= 0):
   return 3;

t = abs(m1 - m2) / math.sqrt(s1 + s2)
df = (s1 + s2)**2 / (s1**2 / (n1-1) + s2**2 / (n2-1))

return 1.0 - stats.lbetai(float(df)/2, 0.5, float(df) / (df + t**2))
$$ LANGUAGE plpythonu;



-- Local distribution data lives here
CREATE TABLE local_dist_stats (
       field_id INTEGER,
       n INTEGER,
       mean FLOAT,
       variance FLOAT
);

-- Views for computating distribution statistics
CREATE VIEW local_dist_sums AS
     SELECT field_id, COUNT(*) n,
            SUM(value::NUMERIC) sm, SUM((value::NUMERIC)^2) smsqr
       FROM local_data
      WHERE to_num(value::TEXT) IS NOT NULL
   GROUP BY field_id;

CREATE VIEW local_dist_stats_vw AS
     SELECT field_id, n, (sm/n)::FLOAT AS "mean", ((smsqr - sm*sm/n) / (n-1))::FLOAT AS "variance"
       FROM local_dist_sums
      WHERE n > 1;

-- Distributions belonging to global attributes live here
CREATE VIEW global_dist_stats AS
     SELECT aa.global_id AS "att_id", lds.n, lds.mean, lds.variance, aa.affinity
       FROM local_dist_stats lds, attribute_affinities aa
      WHERE lds.field_id = aa.local_id;


-- View for comparing distributions
CREATE VIEW dist_comps AS
     SELECT l.field_id, g.att_id,
            dist_t_test(l.n, g.n, l.mean, g.mean, l.variance, g.variance) AS "p",
	    g.affinity
       FROM local_dist_stats l, global_dist_stats g
      WHERE dist_t_test(l.n, g.n, l.mean, g.mean, l.variance, g.variance) < 1.0;


-- Load incoming distribution data into the global set
CREATE OR REPLACE FUNCTION dist_preprocess_source (INTEGER) RETURNS void AS
$$
DECLARE
  new_source_id ALIAS FOR $1;
BEGIN
  PERFORM dist_preprocess_field(id) FROM local_fields WHERE source_id = new_source_id;
END
$$ LANGUAGE plpgsql;


-- Load incoming distribution data into the global set
CREATE OR REPLACE FUNCTION dist_preprocess_field (INTEGER) RETURNS void AS
$$
DECLARE
  new_field_id ALIAS FOR $1;
BEGIN
  INSERT INTO local_dist_stats (field_id, n, mean, variance)
       SELECT *
         FROM local_dist_stats_vw
	WHERE field_id = new_field_id;
EXCEPTION
  WHEN NUMERIC_VALUE_OUT_OF_RANGE THEN
    RETURN;
END
$$ LANGUAGE plpgsql;


-- Compare the distribution of one source's fields against all others
CREATE OR REPLACE FUNCTION dist_results_for_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_source_id ALIAS FOR $1;
BEGIN
  PERFORM dist_results_for_field(id) FROM local_fields WHERE source_id = test_source_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dist_results_for_field (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_field_id ALIAS FOR $1;
BEGIN
  INSERT INTO nr_raw_results (field_id, match_id, score, method_name)
       SELECT field_id, att_id, MAX((1.0 - p) * affinity) AS "score", 'dist'
         FROM dist_comps
        WHERE field_id = test_field_id
     GROUP BY field_id, att_id;
END
$$ LANGUAGE plpgsql;

