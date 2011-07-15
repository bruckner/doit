-- UDFs for doit value comparisons

-- Housekeeping
DROP VIEW IF EXISTS in_val_raw_qgrams CASCADE;
DROP TABLE IF EXISTS in_val_qgrams CASCADE;
DROP TABLE IF EXISTS val_qgrams CASCADE;
DROP TABLE IF EXISTS val_qgrams_idf CASCADE;

CREATE OR REPLACE FUNCTION val_qgrams_flush () RETURNS void AS
$$
BEGIN
  DELETE FROM in_val_qgrams;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION val_qgrams_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM val_qgrams;
  DELETE FROM val_qgrams_idf;
END
$$ LANGUAGE plpgsql;


-- Tables/views for use with value-qgram tf-idf
CREATE VIEW in_val_raw_qgrams AS
     SELECT source_id, name, qgrams2(value,3) gram
       FROM in_data;

CREATE TABLE in_val_qgrams (
       source_id integer,
       name text,
       gram text,
       c integer
);

CREATE TABLE val_qgrams (
     att_id integer,
     gram text,
     c integer,
     tf float NULL
);
ALTER TABLE val_qgrams ADD PRIMARY KEY (gram, att_id);

-- Merge function for val_qgrams: $1 - att_id; $2 - gram; $3 - c
CREATE OR REPLACE FUNCTION merge_val_qgrams (integer, text, bigint) RETURNS void AS
$$
BEGIN
    UPDATE val_qgrams SET c = c + $3 WHERE att_id = $1 AND gram = $2;
    IF found THEN
       RETURN;
    END IF;
    BEGIN
      INSERT INTO val_qgrams (att_id,gram,c) VALUES ($1,$2,$3);
      RETURN;
    EXCEPTION WHEN unique_violation THEN
      NULL;
    END;
END
$$ LANGUAGE plpgsql;


CREATE TABLE val_qgrams_idf (
       gram text,
       df integer,
       idf float
);
CREATE INDEX idx_val_qgrams_idf_gram ON val_qgrams_idf (gram);

-- Merge function for val_qgrams_idf: $1 - gram, $2 - df, $3 - #docs
CREATE OR REPLACE FUNCTION merge_val_qgrams_idf (text, bigint, integer) RETURNS void AS
$$
BEGIN
  LOOP
    UPDATE val_qgrams_idf SET df = df + $2, idf = sqrt(ln($3::float/(df+$2))) WHERE gram = $1;
    IF found THEN
       RETURN;
    END IF;
    BEGIN
      INSERT INTO val_qgrams_idf (gram,df,idf) VALUES ($1,$2,sqrt(ln($3::float/$2)));
      RETURN;
    EXCEPTION WHEN unique_violation THEN
      NULL;
    END;
  END LOOP;
END
$$ LANGUAGE plpgsql;


-- Cache incoming qgrams in in_val_qgrams table
CREATE OR REPLACE FUNCTION val_qgrams_stage () RETURNS void AS
$$
BEGIN
  TRUNCATE in_val_qgrams;

  INSERT INTO in_val_qgrams (source_id, name, gram, c)
       SELECT source_id, name, gram, COUNT(*)
         FROM in_val_raw_qgrams
     GROUP BY source_id, name, gram;
END
$$ LANGUAGE plpgsql;


-- Load resolved incoming value-qgrams into global value-qgrams table
CREATE OR REPLACE FUNCTION val_qgrams_load () RETURNS void AS
$$
DECLARE
	att_count integer := COUNT(*) FROM attribute_clusters;
BEGIN

  -- Merge new qgrams into global qgrams
  PERFORM merge_val_qgrams(g.global_id, i.gram, i.c)
     FROM in_val_qgrams i, attribute_clusters g
    WHERE i.source_id = g.local_source_id
      AND i.name = g.local_name;

  -- Merge new idf values into global table
  PERFORM merge_val_qgrams_idf(i.gram, COUNT(*), att_count)
     FROM in_val_qgrams i, attribute_clusters g
    WHERE i.source_id = g.local_source_id
      AND i.name = g.local_name
 GROUP BY i.gram;

  -- Recompute tf scores
  UPDATE val_qgrams a
     SET tf = ln(1+a.c);

  -- Still need to recompute idf values if att_count has changed...

END
$$ LANGUAGE plpgsql;


-- Compute results for tf-idf of incoming qgrams against global
-- qgram index.  Store results in val_qgrams_results. 
CREATE OR REPLACE FUNCTION val_qgrams_results () RETURNS void AS
$$
BEGIN
  INSERT INTO nr_raw_results (source_id, name, method_name, match, score)
       SELECT i.source_id, i.name, tf.att_id, SUM(tf.tf*idf.idf)
         FROM in_val_raw_qgrams i, val_qgrams tf, val_qgrams_idf idf
        WHERE i.gram = tf.gram
          AND tf.gram = idf.gram
     GROUP BY i.source_id, i.name, tf.att_id;
END
$$ LANGUAGE plpgsql;


