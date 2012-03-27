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
 *
 * ngrams.sql -- Computes cosine similarity of columns of data.
 *
 * Column data from local_data is tokenized and loaded in local_ngrams (via 
 * local_ngrams_raw view).  TF weights are stored local_ngrams, and IDF weights
 * in ngrams_idf.  As columns are mapped to global attributes, ngrams are put
 * in global_ngrams as appropriate.
 *
 * The view ngrams_cosine_similarity computes similarity by joining
 * local_ngrams (TF), global_ngrams (TF), ngrams_idf (IDF), and local/global_
 * ngrams_norms.
 *
 * The ngrams_preprocess_[source|field|all] functions handle loading new data 
 * from local_data. The ngrams_preprocess_global function handles labeling
 * tokens for global columns, and computes most of the term weights. The
 * ngrams_results_* functions get similarity values and put them into
 * nr_raw_results.
 * 
 */

-- Tables, views, and UDFs for value ngram matching method for name resolution

CREATE OR REPLACE FUNCTION ngrams_clean () RETURNS void AS
$$
BEGIN
  DELETE FROM local_ngrams;
  DELETE FROM global_ngrams;
  DELETE FROM ngrams_idf;
  DELETE FROM local_ngrams_norms;
  DELETE FROM global_ngrams_norms;
END
$$ LANGUAGE plpgsql;


-- Tokenizer for incoming values
CREATE VIEW local_ngrams_raw AS
     SELECT field_id, tokenize(value) gram
       FROM local_data;

CREATE TABLE local_ngrams (
       source_id INTEGER,
       field_id INTEGER NOT NULL,
       gram TEXT NOT NULL,
       c INTEGER,
       tf FLOAT
);
ALTER TABLE local_ngrams ADD PRIMARY KEY (gram, field_id);
CREATE INDEX idx_local_ngrams_source_id ON local_ngrams (source_id);


CREATE VIEW global_ngrams_raw AS
     SELECT att_id, tokenize(value) gram
       FROM global_data;

CREATE TABLE global_ngrams (
       att_id INTEGER NOT NULL,
       gram TEXT NOT NULL,
       c INTEGER NOT NULL,
       tf FLOAT NULL
);
ALTER TABLE global_ngrams ADD PRIMARY KEY (gram, att_id);


/*
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
*/

CREATE VIEW global_ngrams_doc_lens AS
     SELECT att_id, SUM(c) AS "len"
       FROM global_ngrams
   GROUP BY att_id;

CREATE TABLE ngrams_idf (
       gram TEXT NOT NULL PRIMARY KEY,
       idf FLOAT NULL
);

CREATE TABLE global_ngrams_norms (
       att_id INTEGER,
       norm FLOAT
);

-- tfidf for incoming values
CREATE VIEW local_ngrams_doc_lens AS
     SELECT field_id, SUM(c) len
       FROM local_ngrams
   GROUP BY field_id;

CREATE TABLE local_ngrams_norms (
       source_id INTEGER,
       field_id INTEGER,
       norm FLOAT
);

-- cosine similarity of local fields to global attribtues
CREATE VIEW ngrams_cosine_similarity AS
     SELECT qtf.source_id, qtf.field_id, dtf.att_id,
            SUM(qtf.tf * idf.idf * dtf.tf * idf.idf)::FLOAT / (qn.norm * dn.norm) AS "similarity"
       FROM local_ngrams qtf, ngrams_idf idf, local_ngrams_norms qn,
            global_ngrams dtf, global_ngrams_norms dn
      WHERE dtf.gram = qtf.gram
        AND dtf.gram = idf.gram
        AND qtf.field_id = qn.field_id
        AND dtf.att_id = dn.att_id
   GROUP BY qtf.source_id, qtf.field_id, dtf.att_id, qn.norm, dn.norm;

CREATE VIEW ngrams_results AS
     SELECT source_id, field_id, 'ngrams'::TEXT AS "method_name",
            att_id AS "match_id", similarity AS "score"
       FROM ngrams_cosine_similarity;



CREATE OR REPLACE FUNCTION ngrams_preprocess_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  new_source_id ALIAS FOR $1;
BEGIN
  PERFORM ngrams_preprocess_field(id, new_source_id) FROM local_fields WHERE source_id = new_source_id;
END
$$ LANGUAGE plpgsql;


-- Load incoming value-ngrams in local_ngrams table
CREATE OR REPLACE FUNCTION ngrams_preprocess_field (INTEGER, INTEGER) RETURNS void AS
$$
DECLARE
  new_field_id ALIAS FOR $1;
  new_source_id ALIAS FOR $2;
BEGIN
  INSERT INTO local_ngrams (source_id, field_id, gram, c)
       SELECT new_source_id, field_id, gram, COUNT(*)
         FROM local_ngrams_raw
	WHERE field_id = new_field_id
     GROUP BY field_id, gram;

  UPDATE local_ngrams ln
     SET tf = ln.c::FLOAT / dl.len
    FROM local_ngrams_doc_lens dl
   WHERE ln.field_id = new_field_id
     AND dl.field_id = new_field_id;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ngrams_preprocess_all () RETURNS void AS
$$
BEGIN
  TRUNCATE local_ngrams;

  ALTER TABLE local_ngrams DROP CONSTRAINT local_ngrams_pkey;
  DROP INDEX idx_local_ngrams_source_id;

  INSERT INTO local_ngrams (field_id, gram, c)
       SELECT field_id, gram, COUNT(*)
         FROM local_ngrams_raw
     GROUP BY field_id, gram;

  UPDATE local_ngrams a
     SET source_id = b.source_id
    FROM local_fields b
   WHERE a.field_id = b.id;

  -- Compute local TF weights
  UPDATE local_ngrams ln
     SET tf = ln.c::FLOAT / dl.len
    FROM local_ngrams_doc_lens dl
   WHERE ln.field_id = dl.field_id;

  ALTER TABLE local_ngrams ADD PRIMARY KEY (gram, field_id);
  CREATE INDEX idx_local_ngrams_source_id ON local_ngrams (source_id);
END
$$ LANGUAGE plpgsql;


-- Load ngrams for resolved value-sets into global ngram table
CREATE OR REPLACE FUNCTION ngrams_preprocess_global () RETURNS void AS
$$
DECLARE
  doc_count FLOAT;
BEGIN
  -- Reload global_ngrams
  TRUNCATE global_ngrams;
  ALTER TABLE global_ngrams DROP CONSTRAINT global_ngrams_pkey;

  INSERT INTO global_ngrams (att_id, gram, c)
       SELECT aa.global_id, lvn.gram, SUM(lvn.c * aa.affinity)
         FROM local_ngrams lvn, attribute_affinities aa
	WHERE lvn.field_id = aa.local_id
     GROUP BY aa.global_id, lvn.gram;

  INSERT INTO global_ngrams (att_id, gram, c)
       SELECT att_id, gram, COUNT(*)
         FROM global_ngrams_raw
     GROUP BY att_id, gram;

  -- Recompute tf scores (slow?!)
  UPDATE global_ngrams gvn
     SET tf = gvn.c::float / dl.len::float
    FROM global_ngrams_doc_lens dl
   WHERE gvn.att_id = dl.att_id;

  ALTER TABLE global_ngrams ADD PRIMARY KEY (gram, att_id);

  -- Recompute idf scores
  TRUNCATE ngrams_idf;
  ALTER TABLE ngrams_idf DROP CONSTRAINT ngrams_idf_pkey;

  INSERT INTO ngrams_idf (gram)
       SELECT DISTINCT gram
         FROM local_ngrams;

  doc_count := COUNT(*)::FLOAT FROM global_attributes;

  UPDATE ngrams_idf a
     SET idf = sqrt(ln(doc_count / c))
    FROM (SELECT gram, COUNT(att_id) AS "c"
            FROM global_ngrams
        GROUP BY gram) b
   WHERE a.gram = b.gram;

  -- Ngrams not in global training set have never been mapped
  -- and cannot be matched, but they do affect local norms.
  -- They are set to idf of a gram appearing in one document.
  UPDATE ngrams_idf
     SET idf = sqrt(ln(doc_count))
   WHERE idf IS NULL;

  ALTER TABLE ngrams_idf ADD PRIMARY KEY (gram);

  -- Recompute global norms
  TRUNCATE global_ngrams_norms;
  INSERT INTO global_ngrams_norms (att_id, norm)
       SELECT a.att_id, sqrt(SUM((a.tf*b.idf)^2))
         FROM global_ngrams a, ngrams_idf b
        WHERE a.gram = b.gram
     GROUP BY a.att_id;

  -- Recompute local norms
  TRUNCATE local_ngrams_norms;
  INSERT INTO local_ngrams_norms (source_id, field_id, norm)
       SELECT tf.source_id, tf.field_id, sqrt(SUM((tf.tf*idf.idf)^2))
         FROM local_ngrams tf, ngrams_idf idf
        WHERE tf.gram = idf.gram
     GROUP BY tf.source_id, tf.field_id;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ngrams_results_for_all () RETURNS VOID AS
$$
BEGIN
  INSERT INTO nr_raw_results (source_id, field_id, method_name, match_id, score)
       SELECT source_id, field_id, 'ngrams'::TEXT, att_id, similarity
         FROM ngrams_cosine_similarity;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ngrams_results_for_all_unmapped () RETURNS VOID AS
$$
BEGIN
  INSERT INTO nr_raw_results (source_id, field_id, method_name, match_id, score)
       SELECT source_id, field_id, 'ngrams'::TEXT, att_id, similarity
         FROM ngrams_cosine_similarity
        WHERE field_id != ANY (ARRAY(SELECT local_id FROM attribute_mappings));
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ngrams_results_for_source (INTEGER) RETURNS VOID AS
$$
DECLARE
  test_source ALIAS FOR $1;
BEGIN
  INSERT INTO nr_raw_results (source_id, field_id, method_name, match_id, score)
       SELECT source_id, field_id, 'ngrams'::TEXT, att_id, similarity
         FROM ngrams_cosine_similarity
        WHERE source_id = test_source;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ngrams_results_for_field (INTEGER) RETURNS void AS
$$
DECLARE
  test_field ALIAS FOR $1;
BEGIN
  INSERT INTO nr_raw_results (field_id, method_name, match_id, score)
       SELECT field_id, 'ngrams'::TEXT, att_id, similarity
         FROM ngrams_cosine_similarity
        WHERE field_id = test_field;
END
$$ LANGUAGE plpgsql;

