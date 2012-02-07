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

-- Tables/Views/UDFs used for MDL name resolution

CREATE OR REPLACE FUNCTION mdl_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM local_mdl_dictionaries;
  DELETE FROM global_mdl_dictionaries;
  DELETE FROM mdl_input_stats;
  DELETE FROM mdl_dict_card_by_len;
  DELETE FROM mdl_base_dl;
END
$$ LANGUAGE plpgsql;

-- Preprocessed dictionary data
CREATE TABLE local_mdl_dictionaries (
       source_id INTEGER,
       field_id INTEGER,
       value TEXT,
       c INTEGER
);
ALTER TABLE local_mdl_dictionaries ADD PRIMARY KEY (value, field_id);

CREATE VIEW global_mdl_dictionaries_vw AS
     SELECT a.global_id AS "att_id", d.value, LEAST(1.0, SUM(a.affinity)) AS "affinity"
       FROM local_mdl_dictionaries d, attribute_affinities a, training_threshold_affinity t
      WHERE d.field_id = a.local_id
        AND a.affinity >= t.threshold_affinity
   GROUP BY a.global_id, d.value;

CREATE TABLE global_mdl_dictionaries (
       att_id INTEGER,
       value TEXT,
       affinity FLOAT
);


-- Tables/views for computing description length
-- In general DL = plogm + avgValLen*(log(alphabetSize)) 
--               + fplog maxValLen + (f/n)sum_n(sum_p(log (# vals ok /# vals possible)))
-- In our case, p = 1, m = const, alphabetSize = 256, so we get
-- DL = avgValLen*8 + f*log maxValLen + (f/n) sum_n[log(#vals ok) - log(#vals possible)]
-- Where n is size of input dict, f is fraction of values accepted,
-- and (#vals ok/#vals possible) is length specific.

-- Note on affinities and MDL: elements are now considered to only fractionally
-- belong to global dictionaries, with fraction = affinity(local_dict(element), global_dict)

CREATE TABLE mdl_dict_card_by_len (
       att_id INTEGER,
       l INTEGER,
       card FLOAT,
       lg_card FLOAT
);

CREATE TABLE mdl_input_stats (
       source_id INTEGER,
       field_id INTEGER,
       n INTEGER,
       avglen FLOAT,
       maxlen FLOAT
);

CREATE VIEW mdl_matches AS
     SELECT i.source_id, i.field_id, d.att_id, i.value, (i.c * d.affinity) AS c
       FROM local_mdl_dictionaries i, global_mdl_dictionaries d
      WHERE i.value = d.value;

CREATE VIEW mdl_match_counts_by_len AS
     SELECT source_id, field_id, att_id, length(value) l, SUM(c) card, log(2, SUM(c)::NUMERIC) lg_card
       FROM mdl_matches
   GROUP BY source_id, field_id, att_id, l;

-- Unencoded (generic string) description length
CREATE TABLE mdl_base_dl (
       field_id INTEGER,
       domain_name TEXT,
       dl FLOAT
);

-- Observation: the Potter's Wheel final formula appears to have an error.  Instead, I
-- use the next to last form, except I sum over lengths, not individual values.
-- Note: Match fraction f = SUM(card) / (COUNT(*) * n)
CREATE VIEW mdl_description_length AS
     SELECT i.source_id, i.field_id, i.att_id,
   	    (SUM(i.card) / COUNT(*) /s.n) * log(2, s.maxlen::NUMERIC) term1,
	    (1.0 - (SUM(i.card) / COUNT(*) / s.n)) * s.avglen * 8.0 term2,
	    ((SUM(i.card) / COUNT(*) / s.n) / s.n) * SUM(i.lg_card + l.lg_card) term3
       FROM mdl_match_counts_by_len i, mdl_input_stats s, mdl_dict_card_by_len l
      WHERE i.field_id = s.field_id
	AND i.att_id = l.att_id
   GROUP BY i.source_id, i.field_id, i.att_id, s.avglen, s.maxlen, s.n;


CREATE OR REPLACE FUNCTION mdl_preprocess_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  new_source_id ALIAS FOR $1;
BEGIN
  PERFORM mdl_preprocess_field(id, new_source_id) FROM local_fields WHERE source_id = new_source_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mdl_preprocess_field (INTEGER, INTEGER) RETURNS VOID AS
$$
DECLARE
  new_field_id ALIAS FOR $1;
  new_source_id ALIAS FOR $2;
BEGIN
  INSERT INTO local_mdl_dictionaries (source_id, field_id, value, c)
       SELECT new_source_id, field_id, SUBSTRING(value FOR 1000), COUNT(*)
         FROM local_data
	WHERE field_id = new_field_id
	  AND value IS NOT NULL
     GROUP BY field_id, SUBSTRING(value FOR 1000);

  INSERT INTO mdl_input_stats (source_id, field_id, n, avglen, maxlen)
       SELECT source_id, field_id, SUM(c),
              SUM(length(value) * c)::FLOAT / SUM(c), MAX(length(value))::FLOAT
         FROM local_mdl_dictionaries
        WHERE field_id = new_field_id
     GROUP BY source_id, field_id;

  INSERT INTO mdl_base_dl (field_id, domain_name, dl)
       SELECT field_id, 'STRING'::text, (1.0 * avglen * 8.0)
         FROM mdl_input_stats
        WHERE field_id = new_field_id;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION mdl_preprocess_all () RETURNS VOID AS
$$
BEGIN
  TRUNCATE local_mdl_dictionaries;
  TRUNCATE mdl_input_stats;
  TRUNCATE mdl_base_dl;

  -- MDL Dictionaries
  ALTER TABLE local_mdl_dictionaries DROP CONSTRAINT local_mdl_dictionaries_pkey;

  INSERT INTO local_mdl_dictionaries (field_id, value, c)
       SELECT field_id, SUBSTRING(value FOR 1000), COUNT(*)
         FROM local_data
        WHERE value IS NOT NULL
     GROUP BY field_id, SUBSTRING(value FOR 1000);

  UPDATE local_mdl_dictionaries a
     SET source_id = b.source_id
    FROM local_fields b
   WHERE a.field_id = b.id;

  ALTER TABLE local_mdl_dictionaries ADD PRIMARY KEY (value, field_id);

  -- MDL Statistics
  INSERT INTO mdl_input_stats (source_id, field_id, n, avglen, maxlen)
       SELECT source_id, field_id, SUM(c),
              SUM(length(value) * c)::FLOAT / SUM(c), MAX(length(value))::FLOAT
         FROM local_mdl_dictionaries
     GROUP BY source_id, field_id;

  INSERT INTO mdl_base_dl (field_id, domain_name, dl)
       SELECT field_id, 'STRING'::text, (1.0 * avglen * 8.0)
         FROM mdl_input_stats;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mdl_preprocess_global () RETURNS VOID AS
$$
BEGIN
  TRUNCATE global_mdl_dictionaries;
  INSERT INTO global_mdl_dictionaries
       SELECT *
         FROM global_mdl_dictionaries_vw;

  TRUNCATE mdl_dict_card_by_len;
  INSERT INTO mdl_dict_card_by_len (att_id, l, card)
     SELECT att_id, length(value), SUM(affinity)
       FROM global_mdl_dictionaries
   GROUP BY att_id, length(value);

  UPDATE mdl_dict_card_by_len
     SET lg_card = log(2, card::NUMERIC);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mdl_results_for_all_unmapped () RETURNS VOID AS
$$
BEGIN
  -- Description length normalized by base (generic string) DL, then subtracted from 1
  INSERT INTO nr_raw_results (source_id, field_id, method_name, match_id, score)
  SELECT a.source_id, a.field_id, 'mdl', a.att_id, GREATEST(0, 1.0 - (a.term1+a.term2+a.term3) / b.dl)
    FROM mdl_description_length a, mdl_base_dl b
   WHERE a.field_id = b.field_id
     AND a.field_id NOT IN (SELECT local_id FROM attribute_mappings);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mdl_results_for_all () RETURNS VOID AS
$$
BEGIN
  -- Description length normalized by base (generic string) DL, then subtracted from 1
  INSERT INTO nr_raw_results (source_id, field_id, method_name, match_id, score)
  SELECT a.source_id, a.field_id, 'mdl', a.att_id, GREATEST(0, 1.0 - (a.term1+a.term2+a.term3) / b.dl)
    FROM mdl_description_length a, mdl_base_dl b
   WHERE a.field_id = b.field_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mdl_results_for_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_source ALIAS FOR $1;
BEGIN
  -- Description length normalized by base (generic string) DL, then subtracted from 1
  INSERT INTO nr_raw_results (source_id, field_id, method_name, match_id, score)
  SELECT a.source_id, a.field_id, 'mdl', a.att_id, GREATEST(0, 1.0 - (a.term1+a.term2+a.term3) / b.dl)
    FROM mdl_description_length a, mdl_base_dl b
   WHERE a.field_id = b.field_id
     AND a.source_id = test_source;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mdl_results_for_field (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_field ALIAS FOR $1;
BEGIN
  -- Description length normalized by base (generic string) DL, then subtracted from 1
  INSERT INTO nr_raw_results (field_id, method_name, match_id, score)
  SELECT a.field_id, 'mdl', a.att_id, GREATEST(0, 1.0 - (a.term1+a.term2+a.term3) / b.dl)
    FROM mdl_description_length a, mdl_base_dl b
   WHERE a.field_id = b.field_id
     AND a.field_id = test_field;
END
$$ LANGUAGE plpgsql;

