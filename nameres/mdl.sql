-- Tables/Views/UDFs used for MDL name resolution

-- Housekeeping
DROP TABLE IF EXISTS mdl_dictionaries CASCADE;

CREATE OR REPLACE FUNCTION mdl_flush () RETURNS void AS
$$
BEGIN
  NULL;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION mdl_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM mdl_dictionaries;
END
$$ LANGUAGE plpgsql;

-- Preprocessed dictionary data
CREATE TABLE mdl_dictionaries (
       id serial,
       att_id integer,
       value text
);
CREATE INDEX idx_mdl_dictionaries_value ON mdl_dictionaries USING hash (value);

-- Merge function for mdl_dictionaries: $1 - att_id; $2 - value
CREATE OR REPLACE FUNCTION merge_mdl_dictionaries (integer, text) RETURNS void AS
$$
BEGIN
  IF EXISTS (SELECT 1 FROM mdl_dictionaries WHERE att_id = $1 AND value = $2) THEN
    RETURN;
  END IF;
  INSERT INTO mdl_dictionaries (att_id, value) VALUES ($1, $2);
END
$$ LANGUAGE plpgsql;



-- Tables/views for computing description length
-- In general DL = plogm + avgValLen*(log(alphabetSize)) 
--               + fplog maxValLen + (f/n)sum_n(sum_p(log (# vals ok /# vals possible)))
-- In our case, p = 1, m = const, alphabetSize = 128, so we get
-- DL = avgValLen*7 + f*log maxValLen + (f/n) sum_n[log(#vals ok) - log(#vals possible)]
-- Where n is size of input dict, f is fraction of values accepted,
-- and (#vals ok/#vals possible) is length specific.

CREATE VIEW mdl_dict_card_by_len AS
     SELECT att_id, length(value) l, COUNT(*) card
       FROM mdl_dictionaries
   GROUP BY att_id, l;

CREATE VIEW mdl_input_dict_stats AS
     SELECT source_id, name, COUNT(*) n,
            AVG(length(value)) avglen, MAX(length(value)) maxlen
       FROM in_data
   GROUP BY source_id, name;

CREATE VIEW mdl_matches AS
     SELECT i.source_id, i.name, d.att_id, i.value
       FROM in_data i, mdl_dictionaries d
      WHERE i.value = d.value;

CREATE VIEW mdl_match_counts_by_len AS
     SELECT source_id, name, att_id, length(value) l, COUNT(*) card
       FROM mdl_matches
   GROUP BY source_id, name, att_id, l;

CREATE VIEW mdl_input_match_fracs AS
     SELECT m.source_id, m.name, m.att_id, (SUM(m.card)::float / s.n::float) f
       FROM mdl_match_counts_by_len m, mdl_input_dict_stats s
      WHERE m.source_id = s.source_id
        AND m.name = s.name
   GROUP BY m.source_id, m.name, m.att_id, s.n;

-- Unencoded (generic string) description length
CREATE VIEW mdl_base_dl AS
     SELECT source_id, name, 'STRING'::text domain_name, (1.0 * avglen * ln(255)) dl
       FROM mdl_input_dict_stats;

CREATE VIEW mdl_description_length AS
     SELECT i.source_id, i.name, i.att_id,
   	    (f.f * ln(s.maxlen)) term1,
	    (1.0 - f.f) * s.avglen * ln(255) term2,
	    (f.f / s.n::float) * SUM(ln(i.card * l.card)) term3
       FROM mdl_match_counts_by_len i, mdl_input_dict_stats s,
	    mdl_input_match_fracs f, mdl_dict_card_by_len l
      WHERE i.source_id = s.source_id
	AND i.name = s.name
	AND i.source_id = f.source_id
	AND i.name = f.name
	AND i.att_id = f.att_id
	AND i.att_id = l.att_id
   GROUP BY i.source_id, i.name, i.att_id, s.avglen, s.maxlen, f.f, s.n;


-- UDF to move processed input data into MDL dictionaries.
-- Assumes name resolution is completed, i.e. attribute_clusters
-- has records for incoming data
-- NB: mdl_dictionaries may contain duplicate recs after this!
CREATE OR REPLACE FUNCTION mdl_load_dictionaries () RETURNS void AS
$$
BEGIN
  -- Add new values to dictionaries

  CREATE TEMP TABLE  mdl_dict_tmp AS
       SELECT a.global_id, i.value
         FROM in_data i, attribute_clusters a
        WHERE i.source_id = a.local_source_id
          AND i.name = a.local_name
     GROUP BY a.global_id, i.value;

  CREATE INDEX idx_mdl_dict_tmp ON mdl_dict_tmp USING hash (value);
  CREATE INDEX idx_mdl_dict_tmp2 ON mdl_dict_tmp (global_id);

  DELETE FROM mdl_dict_tmp t
        USING mdl_dictionaries d
        WHERE t.global_id = d.att_id
          AND t.value = d.value;

  INSERT INTO mdl_dictionaries (att_id, value)
  SELECT global_id, value FROM mdl_dict_tmp;

  DROP TABLE mdl_dict_tmp;
  RETURN;

 PERFORM merge_mdl_dictionaries(a.global_id, i.value)
    FROM in_data i, attribute_clusters a
   WHERE i.source_id = a.local_source_id
     AND i.name = a.local_name;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mdl_load_input () RETURNS void AS
$$
BEGIN
  NULL;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mdl_load_results () RETURNS void AS
$$
BEGIN
  CREATE INDEX idx_mdl_input_value ON in_data USING hash (value);

  INSERT INTO nr_raw_results (source_id, name, method_name, match, score)
  SELECT a.source_id, a.name, 'mdl', a.att_id, GREATEST(0, 1.0 - (a.term1+a.term2+a.term3) / b.dl)
    FROM mdl_description_length a, mdl_base_dl b
   WHERE a.source_id = b.source_id
     AND a.name = b.name;

  DROP INDEX IF EXISTS idx_mdl_input_value;
END
$$ LANGUAGE plpgsql;
