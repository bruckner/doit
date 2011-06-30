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



CREATE VIEW att_qgrams_min_idf AS
     SELECT MIN(idf) idf
       FROM att_qgrams_idf;

CREATE VIEW att_qgrams_norms AS
     SELECT att_id, sqrt(SUM((a.tf*b.idf)^2)) norm
       FROM att_qgrams a, att_qgrams_idf b
      WHERE a.gram = b.gram
   GROUP BY att_id;


-- tf-idf for incoming values
CREATE VIEW in_att_qgrams_query_lens AS
     SELECT source_id, name, SUM(c) len
       FROM in_att_qgrams
   GROUP BY source_id, name;

CREATE VIEW in_att_qgrams_tf AS
     SELECT a.source_id, a.name, a.gram, (a.c::float / b.len) tf
       FROM in_att_qgrams a, in_att_qgrams_query_lens b
      WHERE a.source_id = b.source_id
        AND a.name = b.name;

CREATE VIEW in_att_qgrams_idf AS
     SELECT a.gram, COALESCE(a.idf, b.idf) idf
       FROM att_qgrams_idf a, att_qgrams_min_idf b;

CREATE VIEW in_att_qgrams_norms AS
     SELECT a.source_id, a.name, sqrt(SUM((a.tf*b.idf)^2)) norm
       FROM in_att_qgrams_tf a, in_att_qgrams_idf b
      WHERE a.gram = b.gram
   GROUP BY a.source_id, a.name;




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
       SELECT g.global_id, i.gram, SUM(i.c)
         FROM in_att_qgrams i, attribute_clusters g
        WHERE i.source_id = g.local_source_id
--          AND i.name = g.local_name
     GROUP BY g.global_id, i.gram;

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
     SET idf = sqrt(ln(att_count::float / df));

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION syn_load_results () RETURNS void AS
$$
BEGIN
  INSERT INTO nr_raw_results (source_id, name, method_name, match, score)
       SELECT qtf.source_id, qtf.name, 'att_qgrams', dtf.att_id,
       	      SUM(qtf.tf * qidf.idf * dtf.tf * didf.idf)::float / (qn.norm * dn.norm) score
         FROM in_att_qgrams_tf qtf, in_att_qgrams_idf qidf, in_att_qgrams_norms qn,
	      att_qgrams dtf, att_qgrams_idf didf, att_qgrams_norms dn
        WHERE dtf.gram = qtf.gram
          AND dtf.gram = qidf.gram
	  AND dtf.gram = didf.gram
	  AND qtf.source_id = qn.source_id
	  AND qtf.name = qn.name
	  AND dtf.att_id = dn.att_id
     GROUP BY qtf.source_id, qtf.name, dtf.att_id, qn.norm, dn.norm;
END
$$ LANGUAGE plpgsql;
