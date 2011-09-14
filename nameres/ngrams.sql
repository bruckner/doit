-- Tables, views, and UDFs for value ngram matching method for name resolution

-- Housekeeping
DROP VIEW IF EXISTS local_ngrams_raw CASCADE;
DROP TABLE IF EXISTS local_ngrams CASCADE;

DROP TABLE IF EXISTS global_ngrams CASCADE;
DROP TABLE IF EXISTS global_ngrams_idf CASCADE;


CREATE OR REPLACE FUNCTION ngrams_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM local_ngrams;
  DELETE FROM global_ngrams;
  DELETE FROM global_ngrams_idf;
END
$$ LANGUAGE plpgsql;


-- Tokenizer for incoming values
CREATE VIEW local_ngrams_raw AS
     SELECT field_id, tokenize(value) gram
       FROM local_data;

CREATE TABLE local_ngrams (
       field_id INTEGER NOT NULL,
       gram TEXT NOT NULL,
       c INTEGER
);
ALTER TABLE local_ngrams ADD PRIMARY KEY (gram, field_id);

CREATE TABLE global_ngrams (
       att_id INTEGER NOT NULL,
       gram TEXT NOT NULL,
       c INTEGER NOT NULL,
       tf FLOAT NULL
);
ALTER TABLE global_ngrams ADD PRIMARY KEY (gram, att_id);

-- Merge function for ngrams: $1 - att_id; $2 - gram; $3 - c
CREATE OR REPLACE FUNCTION merge_global_ngrams (integer, text, bigint) RETURNS void AS
$$
BEGIN
    UPDATE global_ngrams SET c = c + $3 WHERE att_id = $1 AND gram = $2;
    IF found THEN
       RETURN;
    END IF;
    BEGIN
      INSERT INTO global_ngrams (att_id,gram,c) VALUES ($1,$2,$3);
      RETURN;
    EXCEPTION WHEN unique_violation THEN
      NULL;
    END;
END
$$ LANGUAGE plpgsql;


CREATE VIEW global_ngrams_doc_lens AS
     SELECT att_id, SUM(c) AS "len"
       FROM global_ngrams
   GROUP BY att_id;

CREATE TABLE global_ngrams_idf (
       gram TEXT NOT NULL PRIMARY KEY,
       idf FLOAT NULL
);

CREATE VIEW global_ngrams_min_idf AS
     SELECT MIN(idf) idf
       FROM global_ngrams_idf;

CREATE VIEW global_ngrams_norms AS
     SELECT a.att_id, sqrt(SUM((a.tf*b.idf)^2)) norm
       FROM global_ngrams a, global_ngrams_idf b
      WHERE a.gram = b.gram
   GROUP BY a.att_id;

-- tfidf for incoming values
CREATE VIEW local_ngrams_doc_lens AS
     SELECT field_id, SUM(c) len
       FROM local_ngrams
   GROUP BY field_id;

CREATE VIEW local_ngrams_tf AS
     SELECT lvn.field_id, lvn.gram, (lvn.c::float / dl.len) tf
       FROM local_ngrams lvn, local_ngrams_doc_lens dl
      WHERE lvn.field_id = dl.field_id;

CREATE VIEW local_ngrams_idf AS
     SELECT b.gram, COALESCE(b.idf, c.idf) idf
       FROM global_ngrams_idf b, global_ngrams_min_idf c;

CREATE VIEW local_ngrams_norms AS
     SELECT tf.field_id, sqrt(SUM((tf.tf*idf.idf)^2)) norm
       FROM local_ngrams_tf tf, local_ngrams_idf idf
      WHERE tf.gram = idf.gram
   GROUP BY tf.field_id;


CREATE OR REPLACE FUNCTION ngrams_preprocess_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  new_source_id ALIAS FOR $1;
BEGIN
  PERFORM ngrams_preprocess_field(id) FROM local_fields WHERE source_id = new_source_id;
END
$$ LANGUAGE plpgsql;


-- Load incoming value-ngrams in local_ngrams table
CREATE OR REPLACE FUNCTION ngrams_preprocess_field (INTEGER) RETURNS void AS
$$
DECLARE
  new_field_id ALIAS FOR $1;
BEGIN
  INSERT INTO local_ngrams (field_id, gram, c)
       SELECT field_id, gram, COUNT(*)
         FROM local_ngrams_raw
	WHERE field_id = new_field_id
     GROUP BY field_id, gram;
END
$$ LANGUAGE plpgsql;


-- Load ngrams for resolved value-sets into global ngram table
CREATE OR REPLACE FUNCTION ngrams_preprocess_global () RETURNS void AS
$$
DECLARE
	att_count float;
BEGIN
  -- Reload global_ngrams
  TRUNCATE global_ngrams;
  ALTER TABLE global_ngrams DROP CONSTRAINT global_ngrams_pkey;

  INSERT INTO global_ngrams (att_id, gram, c)
       SELECT aa.global_id, lvn.gram, SUM(lvn.c * aa.affinity)
         FROM local_ngrams lvn, attribute_affinities aa
	WHERE lvn.field_id = aa.local_id
     GROUP BY aa.global_id, lvn.gram;

  ALTER TABLE global_ngrams ADD PRIMARY KEY (gram, att_id);

  -- Recompute tf scores (slow?!)
  UPDATE global_ngrams gvn
     SET tf = gvn.c::float / dl.len::float
    FROM global_ngrams_doc_lens dl
   WHERE gvn.att_id = dl.att_id;

   -- Recompute idf scores
   att_count := COUNT(*)::float FROM global_attributes;

   TRUNCATE global_ngrams_idf;

   INSERT INTO global_ngrams_idf (gram, idf)
        SELECT gram, sqrt(ln( att_count / COUNT(*) ))
	  FROM global_ngrams
      GROUP BY gram;

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ngrams_results_for_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_source_id ALIAS FOR $1;
BEGIN
  PERFORM ngrams_results_for_field_range(MIN(id), MAX(id)) FROM local_fields WHERE source_id = test_source_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ngrams_results_for_field_range (INTEGER, INTEGER) RETURNS void AS
$$
DECLARE
  test_field_min ALIAS FOR $1;
  test_field_max ALIAS FOR $2;
BEGIN
  INSERT INTO nr_raw_results (field_id, method_name, match_id, score)
       SELECT qtf.field_id, 'ngrams', dtf.att_id,
       	      SUM(qtf.tf * qidf.idf * dtf.tf * didf.idf)::float / (qn.norm * dn.norm)
         FROM local_ngrams_tf qtf, local_ngrams_idf qidf, local_ngrams_norms qn,
	      global_ngrams dtf, global_ngrams_idf didf, global_ngrams_norms dn
        WHERE dtf.gram = qtf.gram
          AND dtf.gram = qidf.gram
	  AND dtf.gram = didf.gram
	  AND qtf.field_id = qn.field_id
	  AND dtf.att_id = dn.att_id
	  AND qtf.field_id >= test_field_min AND qtf.field_id <= test_field_max
     GROUP BY qtf.field_id, dtf.att_id, qn.norm, dn.norm;
END
$$ LANGUAGE plpgsql;

