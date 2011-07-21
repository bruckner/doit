-- Relations and functions for factoring run-on attribute names

-- Housekeeping
DROP TABLE IF EXISTS names_to_factor CASCADE;
DROP TABLE IF EXISTS attribute_synonyms CASCADE;


-- Relations
CREATE TABLE names_to_factor (
       source_id INT,
       name TEXT
);

CREATE TABLE attribute_synonyms (
       global_id INT,
       token TEXT
);

CREATE VIEW knockout_terms AS
     SELECT a.source_id, a.name, group_concat(b.token, ','::text) k
       FROM names_to_factor a, attribute_synonyms b, attribute_clusters c
      WHERE lower(a.name) LIKE '%' || lower(b.token) || '%'
        AND a.source_id = c.local_source_id
	AND a.name = c.local_name
	AND b.global_id = c.global_id
   GROUP BY a.source_id, a.name;

CREATE VIEW knockouts AS
     SELECT source_id, name, remove_substrs(name, k) k
       FROM knockout_terms;

CREATE VIEW leftovers AS
     SELECT source_id, name, dsubstrs(k) factor
       FROM knockouts;

CREATE VIEW hidden_gems AS
     SELECT a.source_id, a.name, a.factor, b.pterm
       FROM leftovers a, public.nova_metastore_raw b
      WHERE a.factor = b.token;



CREATE OR REPLACE FUNCTION factor_names () RETURNS VOID AS
$$
BEGIN
  NULL;
END
$$ LANGUAGE plpgsql;
