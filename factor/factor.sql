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
