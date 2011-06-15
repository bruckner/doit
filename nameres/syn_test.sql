-- Tables/views/UDFs for synonym matching component of name resolver

-- Housekeeping
DROP VIEW IF EXISTS in_att_raw_qgrams CASCADE;
DROP VIEW IF EXISTS in_att_qgrams CASCADE;
DROP TABLE IF EXISTS att_qgrams CASCADE;
DROP TABLE IF EXISTS att_qgrams_idf CASCADE;

CREATE OR REPLACE FUNCTION att_qgrams_flush () RETURNS void AS
$$
BEGIN
  RETURN;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION att_qgrams_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM att_qgrams;
  DELETE FROM att_qgrams_idf;
END
$$ LANGUAGE plpgsql;


-- Tables/views for qgrams tf-idf
CREATE VIEW in_att_raw_qgrams AS
     SELECT source_id, name, qgrams2(name,3) gram
       FROM in_fields;

CREATE VIEW in_att_qgrams AS
     SELECT source_id, name, gram, count(gram) c
       FROM in_att_raw_qgrams
   GROUP BY source_id, name, gram;

CREATE TABLE att_qgrams (
       id serial,
       att_id integer,
       gram text,
       c integer,
       tf float
);
CREATE INDEX idx_att_qgrams_gram ON att_qgrams (gram);
CREATE INDEX idx_att_qgrams_att_id ON att_qgrams (att_id);

CREATE TABLE att_qgrams_idf (
       id serial,
       gram text,
       df integer,
       idf float NULL
);
CREATE INDEX idx_att_qgrams_idf_gram ON att_qgrams_idf (gram);


-- Update attribute name tf-idf tables with incoming qgrams.
-- Assumes that local attributes have been matched to globals 
-- in the attribute_clusters table.
CREATE OR REPLACE FUNCTION syn_load_qgrams () RETURNS void AS
$$
DECLARE
	att_count integer := COUNT(*) FROM global_attributes;
BEGIN

-- Add incoming qgrams to global qgrams table
INSERT INTO att_qgrams (att_id, gram, c)
     SELECT g.global_id, i.gram, i.c
       FROM in_att_qgrams i, attribute_clusters g
      WHERE i.source_id = g.local_source_id
        AND i.name = g.local_name;

-- Dedup global qgrams table
UPDATE att_qgrams a
   SET c = a.c + b.c
  FROM att_qgrams b
 WHERE a.att_id = b.att_id
   AND a.gram = b.gram
   AND a.id < b.id;

DELETE FROM att_qgrams a
      USING att_qgrams b
      WHERE a.att_id = b.att_id
        AND a.gram = b.gram
	AND a.id > b.id;

-- Recompute tf scores
UPDATE att_qgrams
   SET tf = ln(1+c);

-- Add new grams to idf table
INSERT INTO att_qgrams_idf (gram, df)
     SELECT gram, COUNT(*)
       FROM att_qgrams
   GROUP BY gram;

-- Dedup idf table
UPDATE att_qgrams_idf a
   SET df = a.df + b.df
  FROM att_qgrams_idf b
 WHERE a.gram = b.gram
   AND a.id < b.id;

DELETE FROM att_qgrams_idf a
      USING att_qgrams_idf b
      WHERE a.gram = b.gram
        AND a.id > b.id;

-- Recompute idf scores
UPDATE att_qgrams_idf
   SET idf = sqrt(ln(att_count / df));

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION syn_load_results () RETURNS void AS
$$
BEGIN
	INSERT INTO nr_raw_results (source_id, name, method_name, match, score)
	     SELECT i.source_id, i.name, 'att_qgrams' mn, q.att_id, SUM(q.tf*idf.idf) score
	       FROM in_att_raw_qgrams i, att_qgrams q, att_qgrams_idf idf
	      WHERE i.gram = q.gram
		AND q.gram = idf.gram
	   GROUP BY i.source_id, i.name, mn, q.att_id;
END
$$ LANGUAGE plpgsql;
