
-- Housekeeping
DROP TABLE IF EXISTS val_dists CASCADE;
DROP VIEW IF EXISTS in_val_dist_sums CASCADE;
DROP VIEW IF EXISTS in_val_dists CASCADE;

CREATE OR REPLACE FUNCTION dist_flush () RETURNS void AS
$$
BEGIN
  RETURN;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dist_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM val_dists;
END
$$ LANGUAGE plpgsql;


-- Global data lives here
CREATE TABLE val_dists (
       att_id integer,
       n integer,
       mean float,
       variance float
);


-- Tables/views for computations
CREATE VIEW in_val_dist_sums AS
     SELECT source_id, name, COUNT(*) n,
            SUM(value::float)::float sm, SUM(value::float*value::float)::float smsqr
       FROM in_data
      WHERE to_num(value::text) IS NOT NULL
   GROUP BY source_id, name;

CREATE VIEW in_val_dists AS
     SELECT source_id, name, n, sm/n mean, (smsqr - sm*sm/n) / (n-1) variance
       FROM in_val_dist_sums
      WHERE n > 1;


-- Load incoming distribution data into the global set
CREATE OR REPLACE FUNCTION dist_load () RETURNS void AS
$$
BEGIN
  -- NB: Each local distribution is kept intact in the global dists table.
  INSERT INTO val_dists (att_id, n, mean, variance)
       SELECT g.global_id, i.n, i.mean, i.variance
         FROM in_val_dists i, attribute_clusters g
	WHERE i.source_id = g.local_source_id
	  AND i.name = g.local_name;
END
$$ LANGUAGE plpgsql;


-- Load up results!
CREATE OR REPLACE FUNCTION dist_results () RETURNS void AS
$$
BEGIN
  CREATE TEMP TABLE dist_tmp AS
       SELECT *
         FROM (SELECT a.source_id, a.name, b.att_id, 
                      dist_t_test(a.n, b.n, a.mean, b.mean, a.variance, b.variance) p
                 FROM in_val_dists a, val_dists b) t
        WHERE p < 1.0;

  CREATE TEMP TABLE dist_max_tmp AS
       SELECT source_id, name, att_id, MIN(p) p
         FROM dist_tmp
     GROUP BY source_id, name, att_id;

  INSERT INTO nr_raw_results (source_id, name, method_name, match, score)
       SELECT a.source_id, a.name, 'dist', a.att_id, a.p
         FROM dist_tmp a, dist_max_tmp b
	WHERE a.source_id = b.source_id
	  AND a.name = b.name
	  AND a.att_id = b.att_id
	  AND a.p = b.p;

  DROP TABLE dist_tmp;
  DROP TABLE dist_max_tmp;
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
