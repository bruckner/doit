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

-- Distribution analysis module

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
    sys.path.append('/home/bruckda1/pymods')
    sys.path.append('/usca_legacy/home/bruckda1/pymods')
    import stats

if (n1 < 2 or n2 < 2):
   return 2;

s1 = v1 / n1
s2 = v2 / n2

if (s1 <= 0 or s2 <= 0):
   return 3;

try:
    t = abs(m1 - m2) / math.sqrt(s1 + s2)
    df = (s1 + s2)**2 / (s1**2 / (n1-1) + s2**2 / (n2-1))
    return 1.0 - stats.lbetai(float(df)/2, 0.5, float(df) / (df + t**2))
except OverflowError:
    return 4

$$ LANGUAGE plpythonu;


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

-- Local distribution data lives here
CREATE TABLE local_dist_stats (
       source_id INTEGER,
       field_id INTEGER,
       n INTEGER,
       mean FLOAT,
       variance FLOAT
);
CREATE INDEX idx_local_dist_stats_source_id ON local_dist_stats (source_id);
CREATE INDEX idx_local_dist_stats_field_id ON local_dist_stats (field_id);

-- Distributions belonging to global attributes live here
CREATE TABLE global_dist_stats (
       att_id INTEGER,
       n INTEGER,
       mean FLOAT,
       variance FLOAT,
       affinity FLOAT
);


-- View for comparing distributions
CREATE VIEW dist_comps AS
     SELECT l.source_id, l.field_id, g.att_id, g.affinity,
            dist_t_test(l.n, g.n, l.mean, g.mean, l.variance, g.variance) AS "p"
       FROM local_dist_stats l, global_dist_stats g
      WHERE dist_t_test(l.n, g.n, l.mean, g.mean, l.variance, g.variance) < 1.0;


-- Load incoming distribution data into the global set
CREATE OR REPLACE FUNCTION dist_preprocess_source (INTEGER) RETURNS void AS
$$
DECLARE
  new_source_id ALIAS FOR $1;
BEGIN
  PERFORM dist_preprocess_field(id, new_source_id) FROM local_fields WHERE source_id = new_source_id;
END
$$ LANGUAGE plpgsql;


-- Load incoming distribution data into the global set
CREATE OR REPLACE FUNCTION dist_preprocess_field (INTEGER, INTEGER) RETURNS void AS
$$
DECLARE
  new_field_id ALIAS FOR $1;
  new_source_id ALIAS FOR $2;
BEGIN
  INSERT INTO local_dist_stats (source_id, field_id, n, mean, variance)
       SELECT new_source_id, *
         FROM local_dist_stats_vw
	WHERE field_id = new_field_id;
EXCEPTION
  WHEN NUMERIC_VALUE_OUT_OF_RANGE THEN
    RETURN;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dist_preprocess_all () RETURNS VOID AS
$$
BEGIN
  DROP INDEX idx_local_dist_stats_source_id;
  DROP INDEX idx_locaL_dist_stats_field_id;

  INSERT INTO local_dist_stats (field_id, n, mean, variance)
       SELECT *
         FROM local_dist_stats_vw;

  CREATE INDEX idx_local_dist_stats_source_id ON local_dist_stats (source_id);
  CREATE INDEX idx_local_dist_stats_field_id ON local_dist_stats (field_id);

  UPDATE local_dist_stats a
     SET source_id = b.source_id
    FROM local_fields b
   WHERE a.field_id = b.id;
EXCEPTION
  WHEN NUMERIC_VALUE_OUT_OF_RANGE THEN
    RETURN;
END
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION dist_preprocess_global () RETURNS VOID AS
$$
BEGIN
  INSERT INTO global_dist_stats (att_id, n, mean, variance, affinity)
       SELECT aa.global_id, lds.n, lds.mean, lds.variance, aa.affinity
         FROM local_dist_stats lds, attribute_affinities aa
        WHERE lds.field_id = aa.local_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dist_results_for_all () RETURNS VOID AS
$$
BEGIN
  INSERT INTO nr_raw_results (source_id, field_id, match_id, score, method_name)
       SELECT source_id, field_id, att_id, MAX((1.0 - p) * affinity) AS "score", 'dist'
         FROM dist_comps
     GROUP BY source_id, field_id, att_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dist_results_for_all_unmapped () RETURNS VOID AS
$$
BEGIN
  INSERT INTO nr_raw_results (source_id, field_id, match_id, score, method_name)
       SELECT source_id, field_id, att_id, MAX((1.0 - p) * affinity) AS "score", 'dist'
         FROM dist_comps
        WHERE field_id NOT IN (SELECT local_id FROM attribute_mappings)
     GROUP BY source_id, field_id, att_id;
END
$$ LANGUAGE plpgsql;


-- Compare the distribution of one source's fields against all others
CREATE OR REPLACE FUNCTION dist_results_for_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_source_id ALIAS FOR $1;
BEGIN
  INSERT INTO nr_raw_results (source_id, field_id, match_id, score, method_name)
       SELECT source_id, field_id, att_id, MAX((1.0 - p) * affinity) AS "score", 'dist'
         FROM dist_comps
        WHERE source_id = test_source_id
     GROUP BY source_id, field_id, att_id;
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

