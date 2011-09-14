-- Tables/views/UDFs for synonym matching component of name resolver

-- Housekeeping
DROP VIEW IF EXISTS local_qgrams_raw CASCADE;
DROP TABLE IF EXISTS local_qgrams CASCADE;


CREATE OR REPLACE FUNCTION qgrams_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM local_qgrams;
END
$$ LANGUAGE plpgsql;


-- Tables/views for qgrams tf-idf
CREATE VIEW local_qgrams_raw AS
     SELECT id AS "field_id", qgrams2(local_name,3) gram
       FROM local_fields;

CREATE VIEW local_qgrams_vw AS
     SELECT field_id, gram, count(gram) c
       FROM local_qgrams_raw
   GROUP BY field_id, gram;

CREATE TABLE local_qgrams (
       field_id INTEGER,
       gram TEXT,
       c INTEGER,
       tf FLOAT
);

CREATE OR REPLACE FUNCTION qgrams_preprocess (INTEGER) RETURNS VOID AS
$$
DECLARE
  new_source_id ALIAS FOR $1;
BEGIN

  -- Get qgram data for local fields
  INSERT INTO local_qgrams (field_id, gram, c, tf)
       SELECT *, ln(1+c)
         FROM local_qgrams_vw
	WHERE field_id IN (SELECT id FROM local_fields WHERE source_id = new_source_id);

END
$$ LANGUAGE plpgsql;


-- globalized attribute qgram views
CREATE VIEW global_qgrams AS
     SELECT aa.global_id AS "att_id", lq.gram, SUM(lq.c * aa.affinity) AS "c"
       FROM local_qgrams lq, attribute_affinities aa
      WHERE lq.field_id = aa.local_id
   GROUP BY aa.global_id, lq.gram;

CREATE VIEW global_qgrams_tf AS
     SELECT att_id, gram, ln(1+c) AS "tf"
       FROM global_qgrams;

CREATE VIEW global_qgrams_n_docs AS
     SELECT COUNT(DISTINCT att_id) AS "n"
       FROM global_qgrams;

CREATE VIEW global_qgrams_idf AS
     SELECT gq.gram, sqrt(ln( nd.n::float / COUNT(DISTINCT att_id) )) AS "idf"
       FROM global_qgrams gq, global_qgrams_n_docs nd
   GROUP BY gq.gram, nd.n;

CREATE VIEW global_qgrams_min_idf AS
     SELECT MIN(idf) AS "idf"
       FROM global_qgrams_idf;

CREATE VIEW global_qgrams_norms AS
     SELECT tf.att_id, sqrt(SUM((tf.tf*idf.idf)^2)) norm
       FROM global_qgrams_tf tf, global_qgrams_idf idf
      WHERE tf.gram = idf.gram
   GROUP BY tf.att_id;


-- idf and norm values for local fields:
CREATE VIEW local_qgrams_idf AS
     SELECT a.gram, COALESCE(a.idf, b.idf) idf
       FROM global_qgrams_idf a, global_qgrams_min_idf b;

CREATE VIEW local_qgrams_norms AS
     SELECT tf.field_id, sqrt(SUM((tf.tf*idf.idf)^2)) norm
       FROM local_qgrams tf, local_qgrams_idf idf
      WHERE tf.gram = idf.gram
   GROUP BY tf.field_id;


CREATE OR REPLACE FUNCTION qgrams_results_for_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_source_id ALIAS FOR $1;
BEGIN
  PERFORM qgrams_results_for_field_range(MIN(id), MAX(id)) FROM local_fields WHERE source_id = test_source_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION qgrams_results_for_field_range (INTEGER, INTEGER) RETURNS VOID AS
$$
DECLARE
  test_field_min ALIAS FOR $1;
  test_field_max ALIAS FOR $2;
BEGIN
  INSERT INTO nr_raw_results (field_id, method_name, match_id, score)
       SELECT ltf.field_id, 'qgrams', gtf.att_id,
       	      SUM(ltf.tf * lidf.idf * gtf.tf * gidf.idf)::float / (ln.norm * gn.norm)
         FROM local_qgrams ltf, local_qgrams_idf lidf, local_qgrams_norms ln,
	      global_qgrams_tf gtf, global_qgrams_idf gidf, global_qgrams_norms gn
        WHERE ltf.gram = gtf.gram
          AND ltf.gram = lidf.gram
	  AND ltf.gram = gidf.gram
	  AND ltf.field_id = ln.field_id
	  AND gtf.att_id = gn.att_id
	  AND ltf.field_id >= test_field_min AND ltf.field_id <= test_field_max
     GROUP BY ltf.field_id, gtf.att_id, ln.norm, gn.norm;
END
$$ LANGUAGE plpgsql;
