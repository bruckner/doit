-- Tables, views, and UDFs for value ngram matching method for name resolution

-- Housekeeping
DROP VIEW IF EXISTS in_val_raw_ngrams CASCADE;
DROP TABLE IF EXISTS in_val_ngrams CASCADE;
DROP TABLE IF EXISTS val_ngrams CASCADE;
DROP TABLE IF EXISTS val_ngrams_doc_lens CASCADE;
DROP TABLE IF EXISTS val_ngrams_idf CASCADE;

CREATE OR REPLACE FUNCTION val_ngrams_flush () RETURNS void AS
$$
BEGIN
  DELETE FROM in_val_ngrams;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION val_ngrams_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM val_ngrams;
  DELETE FROM val_ngrams_doc_lens;
  DELETE FROM val_ngrams_idf;
END
$$ LANGUAGE plpgsql;



-- Tokenizer for incoming values
CREATE VIEW in_val_raw_ngrams AS
     SELECT source_id, name, tokenize(value) gram
       FROM in_data;

CREATE TABLE in_val_ngrams (
       source_id integer,
       name text,
       gram text,
       c integer
);


CREATE TABLE val_ngrams (
       att_id integer,
       gram text,
       c integer,
       tf float NULL
);
ALTER TABLE val_ngrams ADD PRIMARY KEY (gram, att_id);

-- Merge function for val_ngrams: $1 - att_id; $2 - gram; $3 - c
CREATE OR REPLACE FUNCTION merge_val_ngrams (integer, text, bigint) RETURNS void AS
$$
BEGIN
--  LOOP
    UPDATE val_ngrams SET c = c + $3 WHERE att_id = $1 AND gram = $2;
    IF found THEN
       RETURN;
    END IF;
    BEGIN
      INSERT INTO val_ngrams (att_id,gram,c) VALUES ($1,$2,$3);
      RETURN;
    EXCEPTION WHEN unique_violation THEN
      NULL;
    END;
--  END LOOP;
END
$$ LANGUAGE plpgsql;


CREATE TABLE val_ngrams_doc_lens (
       att_id integer,
       len integer
);


CREATE TABLE val_ngrams_idf (
 	 gram text,
	 df integer,
	 idf float NULL
);
ALTER TABLE val_ngrams_idf ADD PRIMARY KEY (gram);

-- Merge function for val_ngrams_idf: $1 - gram; $2 - df; $3 - #docs
CREATE OR REPLACE FUNCTION merge_val_ngrams_idf (text, bigint, integer) RETURNS void AS
$$
BEGIN
  LOOP
    UPDATE val_ngrams_idf SET df = df + $2, idf = sqrt(ln($3::float/(df+$2))) WHERE gram = $1;
    IF found THEN
       RETURN;
    END IF;
    BEGIN
      INSERT INTO val_ngrams_idf (gram,df,idf) VALUES ($1, $2, sqrt(ln($3::float/$2)));
      RETURN;
    EXCEPTION WHEN unique_violation THEN
      NULL;
    END;
  END LOOP;
END
$$ LANGUAGE plpgsql;


-- Cache incoming value-ngrams in in_val_ngrams table
CREATE OR REPLACE FUNCTION val_ngrams_stage () RETURNS void AS
$$
BEGIN
  DELETE FROM in_val_ngrams;

  INSERT INTO in_val_ngrams (source_id, name, gram, c)
       SELECT source_id, name, gram, COUNT(*)
         FROM in_val_raw_ngrams
     GROUP BY source_id, name, gram;
END
$$ LANGUAGE plpgsql;


-- Load ngrams for resolved value-sets into global ngram table
CREATE OR REPLACE FUNCTION val_ngrams_load () RETURNS void AS
$$
DECLARE
	att_count integer := COUNT(*) FROM attribute_clusters;
BEGIN

  -- Merge incoming ngrams into val_ngrams table
  PERFORM merge_val_ngrams(g.global_id, i.gram, i.c)
     FROM in_val_ngrams i, attribute_clusters g
    WHERE i.source_id = g.local_source_id
      AND i.name = g.local_name;

  -- Update document lengths
  DELETE FROM val_ngrams_doc_lens;
  INSERT INTO val_ngrams_doc_lens (att_id, len)
       SELECT att_id, SUM(c)
         FROM val_ngrams
     GROUP BY att_id;

  -- Merge incoming grams into val_ngrams_idf table
  PERFORM merge_val_ngrams_idf(i.gram, COUNT(*), att_count)
     FROM in_val_ngrams i, attribute_clusters g
    WHERE i.source_id = g.local_source_id
      AND i.name = g.local_name
 GROUP BY i.gram;

  -- Recompute tf scores (slow?!)
  UPDATE val_ngrams a
     SET tf = a.c::float / b.len::float
    FROM val_ngrams_doc_lens b
   WHERE a.att_id = b.att_id;

   -- Still need to recompute idf scores if att_count has changed...

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION val_ngrams_results () RETURNS void AS
$$
BEGIN
  INSERT INTO nr_raw_results (source_id, name, method_name, match, score)
       SELECT i.source_id, i.name, 'val_ngrams', tf.att_id, SUM(tf.tf*idf.idf)
         FROM in_val_ngrams i, val_ngrams tf, val_ngrams_idf idf
        WHERE i.gram = tf.gram
          AND tf.gram = idf.gram
     GROUP BY i.source_id, i.name, tf.att_id;
END
$$ LANGUAGE plpgsql;

