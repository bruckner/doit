-- Tables/views/UDFs for synonym matching component of name resolver

-- Housekeeping
DROP VIEW IF EXISTS local_qgrams_raw CASCADE;
DROP TABLE IF EXISTS local_qgrams CASCADE;
DROP TABLE IF EXISTS local_qgrams_norms CASCADE;
DROP TABLE IF EXISTS global_qgrams CASCADE;
DROP TABLE IF EXISTS qgrams_idf CASCADE;

CREATE OR REPLACE FUNCTION qgrams_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM local_qgrams;
  DELETE FROM local_qgrams_norms;
  DELETE FROM global_qgrams;
  DELETE FROM qgrams_idf;
END
$$ LANGUAGE plpgsql;


-- Tables/views for qgrams tf-idf
CREATE VIEW local_qgrams_raw AS
     SELECT id AS "field_id", source_id, qgrams2(local_name,3) gram
       FROM local_fields;

CREATE TABLE local_qgrams (
       source_id INTEGER,
       field_id INTEGER,
       gram TEXT,
       c INTEGER,
       tf FLOAT
);

-- globalized attribute qgram relations
CREATE TABLE global_qgrams (
       att_id INTEGER,
       gram TEXT,
       c FLOAT,
       tf FLOAT
);

CREATE TABLE qgrams_idf (
       gram TEXT,
       idf FLOAT
);

CREATE VIEW global_qgrams_norms AS
     SELECT tf.att_id, sqrt(SUM((tf.tf*idf.idf)^2)) norm
       FROM global_qgrams tf, qgrams_idf idf
      WHERE tf.gram = idf.gram
   GROUP BY tf.att_id;

-- norm values for local fields:
CREATE TABLE local_qgrams_norms (
       field_id INTEGER,
       norm FLOAT
);

-- cosine similarity of local names to global qgrams sets
CREATE VIEW qgrams_cosine_similarity AS
     SELECT ltf.source_id, ltf.field_id, gtf.att_id,
            SUM(ltf.tf * idf.idf * gtf.tf * idf.idf)::float / (ln.norm * gn.norm) AS "similarity"
       FROM local_qgrams ltf, qgrams_idf idf, local_qgrams_norms ln,
            global_qgrams gtf, global_qgrams_norms gn
      WHERE ltf.gram = gtf.gram
        AND ltf.gram = idf.gram
        AND ltf.field_id = ln.field_id
        AND gtf.att_id = gn.att_id
   GROUP BY ltf.source_id, ltf.field_id, gtf.att_id, ln.norm, gn.norm;


CREATE OR REPLACE FUNCTION qgrams_preprocess (INTEGER) RETURNS VOID AS
$$
DECLARE
  new_source_id ALIAS FOR $1;
BEGIN
  INSERT INTO local_qgrams (source_id, field_id, gram, c, tf)
       SELECT source_id, field_id, gram, COUNT(gram), ln(1+COUNT(gram))
         FROM local_qgrams_raw
        WHERE source_id = new_source_id
     GROUP BY source_id, field_id, gram;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION qgrams_preprocess_global () RETURNS VOID AS
$$
DECLARE
  doc_count INTEGER;
BEGIN
  TRUNCATE global_qgrams;

  INSERT INTO global_qgrams (att_id, gram, c, tf)
       SELECT aa.global_id AS "att_id", lq.gram, SUM(lq.c * aa.affinity), ln(1+SUM(lq.c * aa.affinity))
         FROM local_qgrams lq, attribute_affinities aa
        WHERE lq.field_id = aa.local_id
     GROUP BY aa.global_id, lq.gram;

  TRUNCATE qgrams_idf;

  INSERT INTO qgrams_idf (gram)
       SELECT DISTINCT gram
         FROM local_qgrams;

  doc_count := COUNT(*) FROM global_attributes;

  UPDATE qgrams_idf a
     SET idf = b.idf
    FROM (SELECT gram, sqrt(ln(doc_count::FLOAT / COUNT(att_id))) AS "idf"
            FROM global_qgrams
        GROUP BY gram) b
   WHERE a.gram = b.gram;

  -- Qgrams not belonging to a global att will never be matched,
  -- so their idf value only affects the norm.  Set them to idf
  -- of gram that belongs to one doc only.
  UPDATE qgrams_idf
     SET idf = sqrt(ln(doc_count))
   WHERE idf IS NULL;


  INSERT INTO local_qgrams_norms
       SELECT tf.field_id, sqrt(SUM((tf.tf*idf.idf)^2)) norm
         FROM local_qgrams tf, qgrams_idf idf
        WHERE tf.gram = idf.gram
     GROUP BY tf.field_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION qgrams_results_for_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_source_id ALIAS FOR $1;
BEGIN
  INSERT INTO nr_raw_results (field_id, method_name, match_id, score)
       SELECT field_id, 'qgrams'::TEXT, att_id, similarity
         FROM qgrams_cosine_similarity
        WHERE source_id = test_source_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION qgrams_results_for_field (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_field ALIAS FOR $1;
BEGIN
  INSERT INTO nr_raw_results (field_id, method_name, match_id, score)

       SELECT field_id, 'qgrams'::TEXT, att_id, similarity
         FROM qgrams_cosine_similarity
        WHERE field_id = test_field_id;
END
$$ LANGUAGE plpgsql;
