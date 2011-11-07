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

-- Relations and functions for factoring run-on attribute names

-- Housekeeping
DROP TABLE IF EXISTS fields_to_factor CASCADE;
DROP TABLE IF EXISTS attribute_synonyms CASCADE;


-- Relations
CREATE TABLE fields_to_factor (
       field_id INT
);

CREATE TABLE attribute_synonyms (
       global_id INT,
       token TEXT
);

CREATE VIEW knockout_terms AS
     SELECT lf.id AS "field_id", lf.local_name AS "field_name", group_concat(asyn.token, ','::text) AS "k"
       FROM local_fields lf, attribute_synonyms asyn, attribute_affinities aa
      WHERE LOWER(lf.name) LIKE '%' || LOWER(asyn.token) || '%'
        AND lf.id = aa.local_id
	AND asyn.global_id = aa.global_id
	AND lf.id IN (SELECT field_id FROM fields_to_factor)
   GROUP BY lf.id, lf.local_name;

CREATE VIEW knockouts AS
     SELECT field_id, remove_substrs(field_name, k) k
       FROM knockout_terms;

CREATE VIEW leftovers AS
     SELECT field_id, dsubstrs(k) factor
       FROM knockouts;

CREATE VIEW hidden_gems AS
     SELECT a.field_id, a.factor, b.pterm
       FROM leftovers a, public.nova_metastore_raw b
      WHERE a.factor = b.token;



CREATE OR REPLACE FUNCTION factor_names () RETURNS VOID AS
$$
BEGIN
  NULL;
END
$$ LANGUAGE plpgsql;
