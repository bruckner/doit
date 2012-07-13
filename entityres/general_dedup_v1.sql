--------------------- create output and temp tables ----------------------------

CREATE OR REPLACE FUNCTION multiply_aggregate(double precision,double precision) RETURNS double precision AS
' select $1 * $2; ' language sql IMMUTABLE STRICT; 

CREATE AGGREGATE product (basetype=double precision, sfunc=multiply_aggregate, stype=double precision,
initcond=1 ) ;

CREATE OR REPLACE FUNCTION to_num_besk(v_input text)
RETURNS REAL AS $$
DECLARE v_int_value REAL DEFAULT NULL;
BEGIN
    BEGIN
        v_int_value := v_input::REAL;
    EXCEPTION WHEN OTHERS THEN        
        RETURN NULL;
    END;
RETURN v_int_value;
END;
$$ LANGUAGE plpgsql;


create or replace function tokenize_besk (text) returns setof text as
$$
begin
  return query
  select trim(both '{}' from ts_lexize('english_stem', token)::text)
    from (
    	 select * from ts_parse('default', $1)
	  where tokid != 12
	    and length(token) < 2048
	 ) t;
end
$$ language plpgsql;

/*
create or replace function tokenize_besk (text) returns setof text as
$$
begin
  return query
     select token from ts_parse('default', $1)
	 where tokid != 12 and length(token) < 2048;	 
end;
$$ language plpgsql;
*/

DROP TABLE IF EXISTS data_from_new_source CASCADE;
CREATE TABLE data_from_new_source (entity_id integer, cluster_id integer, tag_id integer, value text);
--CREATE INDEX data_from_new_source__tag_id ON data_from_new_source(tag_id);
--CREATE INDEX data_from_new_source__entity_id_tag_id ON data_from_new_source(entity_id,tag_id);

DROP TABLE IF EXISTS data_from_new_source_qgrams CASCADE;
CREATE TABLE data_from_new_source_qgrams(entity_id integer, cluster_id integer, tag_id integer, qgram text, freq double precision, qgram_norm double precision);
CREATE INDEX data_from_new_source_qgrams__qgram_tag_id ON data_from_new_source_qgrams(qgram,tag_id);

DROP TABLE IF EXISTS data_from_new_source_real CASCADE;
CREATE TABLE data_from_new_source_real (entity_id integer, cluster_id integer, tag_id integer, value double precision);
--CREATE INDEX data_from_new_source_real__entity_id_tag_id ON data_from_new_source_real(entity_id,tag_id);
CREATE INDEX data_from_new_source_real__value_tag_id ON data_from_new_source_real(value, tag_id);


DROP TABLE IF EXISTS inserted_data CASCADE;
CREATE TABLE inserted_data(entity_id integer, cluster_id integer, tag_id integer, value text); 
--CREATE INDEX inserted_data__category_id_tag_id ON inserted_data(category_id, tag_id);
--CREATE INDEX inserted_data__entity_id_tag_id ON inserted_data(entity_id, tag_id);
--CREATE INDEX inserted_data__cluster_id ON inserted_data(cluster_id);

DROP TABLE IF EXISTS inserted_data_qgrams CASCADE;
CREATE TABLE inserted_data_qgrams (entity_id integer, cluster_id integer, tag_id integer, qgram text, freq double precision, qgram_norm double precision); 
CREATE INDEX inserted_data_qgrams__qgram_tag_id ON inserted_data_qgrams(qgram, tag_id);


DROP TABLE IF EXISTS inserted_data_real CASCADE;
CREATE TABLE inserted_data_real(cluster_id integer, entity_id integer, tag_id integer, value double precision); 
--CREATE INDEX inserted_data_real__entity_id_tag_id ON inserted_data_real(entity_id, tag_id);
CREATE INDEX inserted_data_real__value_tag_id ON inserted_data_real(value, tag_id);

DROP TABLE IF EXISTS entity_clustering CASCADE;
CREATE TABLE entity_clustering(entity_id integer, cluster_id integer);

DROP VIEW IF EXISTS similarity_self_join_qrams CASCADE;
CREATE TABLE similarity_self_join_qrams (entity1_id integer, entity2_id integer, tag_id integer, cos_sim double precision);

DROP TABLE IF EXISTS similarity_self_join_result CASCADE;
CREATE TABLE similarity_self_join_result(entity1_id integer, entity2_id integer, m_prob double precision, prob_s_m double precision, prob_s_u double precision);
CREATE INDEX similarity_self_join_result__entity1_id ON similarity_self_join_result(entity1_id);
CREATE INDEX similarity_self_join_result__entity2_id ON similarity_self_join_result(entity2_id);

DROP TABLE IF EXISTS similarity_self_join_result_tmp CASCADE;
CREATE TABLE similarity_self_join_result_tmp(entity1_id integer, entity2_id integer);
CREATE INDEX similarity_self_join_result_tmp__entity1_id ON similarity_self_join_result_tmp(entity1_id);
CREATE INDEX similarity_self_join_result_tmp__entity2_id ON similarity_self_join_result_tmp(entity2_id);

DROP TABLE IF EXISTS candidate_pairs CASCADE;
CREATE TABLE candidate_pairs (entity1_id integer, entity2_id integer);

DROP TABLE IF EXISTS candidate_attributes CASCADE;
CREATE TABLE candidate_attributes(entity1_id integer, entity2_id integer, tag_id integer, similarity double precision);
--CREATE INDEX candidate_attributes__entity1_id ON candidate_attributes(entity1_id);
--CREATE INDEX candidate_attributes__entity2_id ON candidate_attributes(entity2_id);
CREATE INDEX candidate_attributes__entity1_id_entity2_id_tag_id ON candidate_attributes(entity1_id, entity2_id, tag_id);

DROP TABLE IF EXISTS  candidate_attributes_text_qgram CASCADE;
CREATE TABLE candidate_attributes_text_qgram(entity1_id integer, entity2_id integer, tag_id integer, sim double precision);
--CREATE INDEX candidate_attributes_text_qgram__entity1_id_entity2_id_tag_id ON candidate_attributes_text_qgram(entity1_id, entity2_id, tag_id);


DROP TABLE IF EXISTS candidate_pairs_2way CASCADE;
CREATE TABLE candidate_pairs_2way (entity1_id integer, cluster1_id integer, entity2_id integer, cluster2_id integer);

DROP TABLE IF EXISTS candidate_attributes_2way CASCADE;
CREATE TABLE candidate_attributes_2way(entity1_id integer, cluster1_id integer, entity2_id integer, cluster2_id integer, tag_id integer, similarity double precision);
--CREATE INDEX candidate_attributes__entity1_id ON candidate_attributes(entity1_id);
--CREATE INDEX candidate_attributes__entity2_id ON candidate_attributes(entity2_id);
CREATE INDEX candidate_attributes_2way__entity1_id_entity2_id_tag_id ON candidate_attributes_2way(entity1_id, entity2_id, tag_id);

DROP TABLE IF EXISTS  candidate_attributes_text_qgram_2way CASCADE;
CREATE TABLE candidate_attributes_text_qgram_2way(entity1_id integer, cluster1_id integer, entity2_id integer, cluster2_id integer,tag_id integer, sim double precision);
--CREATE INDEX candidate_attributes_text_qgram__entity1_id_entity2_id_tag_id ON candidate_attributes_text_qgram(entity1_id, entity2_id, tag_id);



DROP TABLE IF EXISTS tag_values_frequency CASCADE;
CREATE TABLE tag_values_frequency(tag_id integer, value text, tuples_count int);
CREATE INDEX tag_values_frequency__tag_id_value ON tag_values_frequency(tag_id, md5(value));
CREATE INDEX tag_values_frequency__freq ON tag_values_frequency(tuples_count);

DROP TABLE IF EXISTS tag_frequency CASCADE;
CREATE TABLE tag_frequency(tag_id integer, tuples_count integer);
CREATE INDEX tag_frequency__tag_id ON tag_frequency(tag_id);

DROP VIEW IF EXISTS tag_count CASCADE;
CREATE VIEW tag_count AS
	SELECT tag_id, count(distinct entity_id) AS tuples_count
	FROM data_from_new_source
	GROUP BY tag_id;

DROP VIEW IF EXISTS tag_values_count CASCADE;
CREATE VIEW tag_values_count AS
	SELECT tag_id, value, count(distinct entity_id) AS tuples_count
	FROM data_from_new_source
	GROUP BY tag_id, value;

DROP TABLE IF EXISTS dedup_qgrams_idf CASCADE;
CREATE TABLE dedup_qgrams_idf(tag_id integer, qgram text, doc_count integer);
CREATE INDEX dedup_qgrams_idf__tag_id_qgram ON dedup_qgrams_idf(tag_id, qgram);

DROP TABLE IF EXISTS similarity_2way_join_result CASCADE;
CREATE TABLE similarity_2way_join_result(cluster1_id integer, cluster2_id integer, entity1_id integer, entity2_id integer, m_prob double precision, prob_s_m double precision, prob_s_u double precision);
--CREATE INDEX similarity_2way_join_result__entity1_id ON similarity_self_join_result(entity1_id);
--CREATE INDEX similarity_2way_join_result__entity2_id ON similarity_self_join_result(entity2_id);

DROP VIEW IF EXISTS entity_count_per_cluster_in_new_source CASCADE;
CREATE VIEW entity_count_per_cluster_in_new_source AS
SELECT cluster_id, count(distinct entity_id) AS entity_count 
FROM data_from_new_source
GROUP BY cluster_id;


DROP VIEW IF EXISTS entity_count_per_cluster_in_old_sources CASCADE;
CREATE VIEW entity_count_per_cluster_in_old_sources AS
SELECT cluster_id, count(distinct entity_id) AS entity_count 
FROM entity_clustering
GROUP BY cluster_id;

DROP TABLE IF EXISTS similarity_cluster_join_result CASCADE;
CREATE TABLE similarity_cluster_join_result(cluster1_id integer, cluster2_id integer, similarity double precision);


DROP VIEW IF EXISTS max_sim CASCADE;
CREATE VIEW max_sim AS
SELECT cluster1_id, max(similarity) as max_sim_val
FROM similarity_cluster_join_result
GROUP BY cluster1_id;


DROP TABLE IF EXISTS match_cluster CASCADE;
CREATE TABLE match_cluster (cluster1_id integer, cluster2_id integer); 


----------------- function definitions--------------------------

-- add a new source. The paramter is the source_id
CREATE OR REPLACE FUNCTION add_source_id(integer, real, real) RETURNS void AS
$$
DECLARE
  new_source_id ALIAS FOR $1;
  threshold ALIAS for $2;
  prob_m ALIAS for $3;
BEGIN

select extract_new_data(new_source_id, true);
select self_join(prob_m);
select cluster(threshold, prob_m);
select two_way_join(prob_m);
select incr_cluster(threshold);

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION extract_new_data(integer, bool) RETURNS void AS
$$
DECLARE
  new_source_id ALIAS FOR $1;
  truncate_current_temp_data ALIAS FOR $2;
  c integer;
BEGIN

--update the tag_id's
UPDATE global_attrs_types_thr a
SET tag_id = (select id from global_attributes b where b.name = a.tag_code);

-- extract all entities an their attributes that belong to the passes source_id
IF (truncate_current_temp_data) THEN
	TRUNCATE data_from_new_source;	
	TRUNCATE data_from_new_source_qgrams;
	TRUNCATE data_from_new_source_real;
END IF;

INSERT INTO data_from_new_source (entity_id, cluster_id, tag_id, value)
       SELECT  entity_id, entity_id, m.global_id , array_to_string(array_agg(value), ' , ')
         FROM local_entities e, local_data d, attribute_mappings m
     where e.source_id=new_source_id AND d.value is not NULL AND d.entity_id=e.id AND m.local_id = d.field_id  
     GROUP BY entity_id, entity_id, m.global_id;

RAISE INFO 'data_from_new_source has the data now';

--select count(*) into c from data_from_new_source;
--RAISE INFO 'data_from_new_source has % values', c;

--select count(distinct entity_id) into c from data_from_new_source;
--RAISE INFO 'data_from_new_source has % distinct entity', c;

-- update frequency table and remove any value that occur in more than 5% of tuples

UPDATE tag_frequency t1
SET tuples_count = t1.tuples_count + t2.tuples_count
FROM tag_count t2
WHERE t1.tag_id = t2.tag_id;

INSERT INTO tag_frequency
(SELECT *
FROM tag_count t1
WHERE t1.tag_id NOT IN (SELECT tag_id FROM tag_frequency));

UPDATE tag_values_frequency t1
SET tuples_count = t1.tuples_count + t2.tuples_count
FROM tag_values_count t2
WHERE t1.tag_id = t2.tag_id AND md5(t1.value) = md5(t2.value);

INSERT INTO tag_values_frequency
(SELECT *
FROM tag_values_count t1
WHERE (t1.tag_id, md5(t1.value)) NOT IN (SELECT tag_id, md5(value) FROM tag_values_frequency));


DELETE FROM data_from_new_source d
USING tag_frequency tf,  tag_values_frequency tvf
WHERE d.tag_id = tf.tag_id AND d.tag_id = tvf.tag_id AND d.value = tvf.value AND tvf.tuples_count/tf.tuples_count::double precision > 0.5;

-- populate q-grams

INSERT INTO data_from_new_source_qgrams(entity_id, cluster_id, tag_id, qgram, freq)
SELECT entity_id, cluster_id, d.tag_id, tokenize_besk(value) as qgram, count(*) as freq
FROM data_from_new_source d, global_attrs_types_thr f
WHERE d.tag_id = f.tag_id AND f.type = 'TEXT'
GROUP BY entity_id, cluster_id, d.tag_id, qgram;

RAISE INFO 'Constructed all q-grams for the new data';

DELETE FROM data_from_new_source_qgrams WHERE trim(qgram)='';

UPDATE dedup_qgrams_idf a
SET doc_count = a.doc_count + b.doc_count
FROM (SELECT tag_id, qgram , count(distinct entity_id) as doc_count FROM data_from_new_source_qgrams group by tag_id, qgram) b
WHERE a.tag_id= b.tag_id AND a.qgram = b.qgram;

INSERT INTO dedup_qgrams_idf(tag_id, qgram, doc_count)
SELECT tag_id, qgram , count(distinct entity_id) as doc_count 
FROM data_from_new_source_qgrams
WHERE (tag_id,qgram) not in (select tag_id, qgram FROM dedup_qgrams_idf)
GROUP BY tag_id,qgram;


--truncate frequent, non-distinctive q-grams that occur in more than 60% of the tuples
DELETE FROM data_from_new_source_qgrams
WHERE (tag_id, qgram) IN (
SELECT q.tag_id, qgram
FROM dedup_qgrams_idf q, tag_frequency t
WHERE q.tag_id = t.tag_id AND q.doc_count::double precision/t.tuples_count > 0.1);

-- update the freq of qgrams by multiplying by the idf
UPDATE data_from_new_source_qgrams d
SET freq = freq * log(t.tuples_count::double precision / q.doc_count)
FROM dedup_qgrams_idf q, tag_frequency t
WHERE d.tag_id=q.tag_id AND d.qgram=q.qgram AND q.tag_id = t.tag_id;

--update the norm
UPDATE data_from_new_source_qgrams d
SET qgram_norm = |/agg.norm
FROM (SELECT entity_id, tag_id, sum(freq*freq) as norm
	FROM data_from_new_source_qgrams
	GROUP BY entity_id, tag_id
) agg
WHERE agg.entity_id = d.entity_id AND agg.tag_id = d.tag_id;  


INSERT INTO data_from_new_source_real(entity_id, cluster_id, tag_id, value)
SELECT entity_id, cluster_id, d.tag_id, to_num_besk(value)
FROM data_from_new_source d, global_attrs_types_thr f
WHERE d.tag_id = f.tag_id AND f.type='REAL' AND to_num_besk(value) is not null;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION self_join(real) RETURNS void AS
$$
DECLARE
  est_dup ALIAS for $1; -- estimated probability that a pair is duplicates , suggested value 0.002, should be the same as the value used for learning weights
  updated_entities_count integer;	
  null_prod_m double precision;
  null_prod_u double precision;
BEGIN


DELETE FROM data_from_new_source_qgrams
WHERE tag_id not in (SELECT tag_id FROM features);
-- Part 1: perform similarity join

DELETE FROM data_from_new_source_real
WHERE tag_id not in (SELECT tag_id FROM features);

DELETE FROM data_from_new_source
WHERE tag_id not in (SELECT tag_id FROM features);


-- self join and cluster data_from_new_source

RAISE INFO 'Starting self-join. Timestamp : %', (select timeofday());

SET enable_nestloop TO OFF;
SET enable_mergejoin TO OFF;


TRUNCATE candidate_pairs;	
INSERT INTO candidate_pairs
	SELECT distinct a.entity_id, b.entity_id
	FROM data_from_new_source_qgrams a, data_from_new_source_qgrams b, global_attrs_types_thr f
	WHERE a.tag_id = b.tag_id AND a.entity_id < b.entity_id AND a.qgram=b.qgram AND a.tag_id = f.tag_id
	GROUP BY a.entity_id, b.entity_id, a.tag_id, f.threshold
	HAVING  sum(a.freq*b.freq)/max(a.qgram_norm)/max(b.qgram_norm) >= f.threshold;
	

SET enable_nestloop TO ON;
SET enable_mergejoin TO ON;

RAISE INFO 'Q-gram join done. Timestamp : %', (select timeofday()) ;

-- remember, f.threshold is a negative value 
INSERT INTO candidate_pairs
SELECT distinct a.entity_id, b.entity_id
FROM data_from_new_source_real a, data_from_new_source_real b, global_attrs_types_thr f
WHERE a.entity_id < b.entity_id AND f.threshold is not null AND a.tag_id=b.tag_id AND a.tag_id=f.tag_id AND a.value BETWEEN b.value + f.threshold AND b.value - f.threshold 
	AND b.value BETWEEN a.value + f.threshold AND a.value - f.threshold;

RAISE INFO 'Real-based candidate pairs obtained. Timestamp : %', (select timeofday()) ;

TRUNCATE candidate_attributes;
INSERT INTO candidate_attributes
   SELECT dup.entity1_id, dup.entity2_id, q1.tag_id, SUM(q1.freq * q2.freq)/max(q1.qgram_norm)/max(q2.qgram_norm) AS sim
   FROM (select distinct entity1_id, entity2_id from candidate_pairs) dup, data_from_new_source_qgrams q1, data_from_new_source_qgrams q2
   WHERE dup.entity1_id = q1.entity_id AND dup.entity2_id = q2.entity_id
        AND q1.tag_id = q2.tag_id AND q1.qgram = q2.qgram
   GROUP BY dup.entity1_id, dup.entity2_id, q1.tag_id;

RAISE INFO 'Adding qgram frequencies done. Timestamp : %', (select timeofday()) ;


INSERT INTO candidate_attributes
SELECT entity1_id, entity2_id, d1.tag_id , - abs(d1.value - d2.value)
FROM (select distinct entity1_id, entity2_id from candidate_pairs) dup, data_from_new_source_real d1, data_from_new_source_real d2
WHERE dup.entity1_id=d1.entity_id AND dup.entity2_id=d2.entity_id AND d1.tag_id = d2.tag_id;

RAISE INFO 'Computing real similarities done. Timestamp : %', (select timeofday()) ;


INSERT INTO candidate_attributes
(SELECT dup.entity1_id, dup.entity2_id, d1.tag_id, 0 
FROM (select distinct entity1_id, entity2_id from candidate_pairs) dup, data_from_new_source d1, data_from_new_source d2
	WHERE dup.entity1_id = d1.entity_id AND dup.entity2_id = d2.entity_id AND d1.tag_id = d2.tag_id)
		EXCEPT ALL (Select entity1_id,entity2_id, tag_id, 0 from candidate_attributes);

RAISE INFO 'Adding zero qgrams done. Timestamp : %', (select timeofday()) ;


CREATE TEMP TABLE feature_nulls AS
     SELECT * FROM features
      WHERE t1 IS NULL;

null_prod_m := product(f_given_m) FROM feature_nulls;
null_prod_u := product(f_given_u) FROM feature_nulls;

TRUNCATE similarity_self_join_result;
Insert into similarity_self_join_result
SELECT entity1_id, entity2_id, null,
       null_prod_m *    est_dup  * product(f.f_given_m) / product(n.f_given_m),
       null_prod_u * (1-est_dup) * product(f.f_given_u) / product(n.f_given_u)
FROM candidate_attributes c, features f, feature_nulls n
WHERE c.tag_id = f.tag_id and c.tag_id = n.tag_id
  and c.similarity >= f.t1 and c.similarity < f.t2 
GROUP BY entity1_id, entity2_id;

DROP TABLE feature_nulls;


RAISE INFO 'Computing prob of dup done. Timestamp : %', (select timeofday()) ;

UPDATE similarity_self_join_result
SET m_prob = prob_s_m / (prob_s_m + prob_s_u)
WHERE prob_s_m >0 OR prob_s_u >0;

RAISE INFO 'Self-join done. Timestamp : %', (select timeofday()) ;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cluster(real) RETURNS void AS
$$
DECLARE
  prob_threshold ALIAS for $1;  
  updated_entities_count integer;	
  
BEGIN


-- truncate pairs with prob(M) < threshold; default should be above 0.5
DELETE FROM similarity_self_join_result
WHERE m_prob < prob_threshold;

-- now obtain the transtive closure

INSERT INTO similarity_self_join_result
	SELECT distinct entity_id, entity_id , 1,1,1
	FROM data_from_new_source;

updated_entities_count := 1;

WHILE updated_entities_count > 0 LOOP 

TRUNCATE similarity_self_join_result_tmp;
INSERT INTO similarity_self_join_result_tmp
SELECT min(entity1_id), entity2_id
FROM similarity_self_join_result
GROUP BY entity2_id;

TRUNCATE similarity_self_join_result;
INSERT INTO similarity_self_join_result
SELECT s1.entity1_id, s2.entity2_id , 1, 1, 1
FROM similarity_self_join_result_tmp s1, similarity_self_join_result_tmp s2
WHERE s1.entity2_id = s2.entity1_id;

Select into updated_entities_count count(*)  
FROM similarity_self_join_result s1, similarity_self_join_result_tmp s2
WHERE s1.entity2_id = s2.entity2_id AND s1.entity1_id <> s2.entity1_id;

END LOOP;

-- update the clustering information in data_from_new_source

UPDATE data_from_new_source
SET cluster_id = entity1_id
FROM (select distinct entity1_id, entity2_id FROM similarity_self_join_result) a
WHERE entity2_id = entity_id;

UPDATE data_from_new_source_qgrams
SET cluster_id = entity1_id
FROM (select distinct entity1_id, entity2_id FROM similarity_self_join_result) a
WHERE entity2_id = entity_id;

UPDATE data_from_new_source_real
SET cluster_id = entity1_id
FROM (select distinct entity1_id, entity2_id FROM similarity_self_join_result) a 
WHERE entity2_id = entity_id;

END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION two_way_join(real) RETURNS void AS
$$
DECLARE
  est_dup ALIAS for $1; -- estimated probability that a pair is duplicates , suggested value 0.002, should be the same as the value used for learning weights  
  null_prod_m double precision;
  null_prod_u double precision;
BEGIN

RAISE INFO 'Starting similarity-join..';

SET enable_nestloop TO OFF;
SET enable_mergejoin TO OFF;

TRUNCATE candidate_pairs_2way;	
INSERT INTO candidate_pairs_2way
	SELECT distinct a.entity_id, a.cluster_id, b.entity_id, b.cluster_id
	FROM data_from_new_source_qgrams a, inserted_data_qgrams b, global_attrs_types_thr f
	WHERE a.tag_id = b.tag_id AND a.qgram=b.qgram AND a.tag_id = f.tag_id
	GROUP BY a.entity_id, a.cluster_id, b.entity_id, b.cluster_id, a.tag_id, f.threshold
	HAVING  sum(a.freq*b.freq)/max(a.qgram_norm)/max(b.qgram_norm) >= f.threshold;
	
SET enable_nestloop TO ON;
SET enable_mergejoin TO ON;

RAISE INFO 'Q-gram join done. Timestamp : %', (select timeofday()) ;

INSERT INTO candidate_pairs_2way
SELECT distinct a.entity_id, a.cluster_id, b.entity_id , b.cluster_id
FROM data_from_new_source_real a, inserted_data_real b, global_attrs_types_thr f
WHERE f.threshold is not null AND a.tag_id=b.tag_id AND a.tag_id=f.tag_id AND a.value BETWEEN b.value + f.threshold AND b.value - f.threshold 
	AND b.value BETWEEN a.value + f.threshold AND a.value - f.threshold;

RAISE INFO 'Real-based candidate pairs obtained. Timestamp : %', (select timeofday()) ;

TRUNCATE candidate_attributes_2way;
INSERT INTO candidate_attributes_2way
   SELECT dup.entity1_id, dup.cluster1_id, dup.entity2_id, dup.cluster2_id, q1.tag_id, SUM(q1.freq * q2.freq)/max(q1.qgram_norm)/max(q2.qgram_norm) AS sim
   FROM (select distinct entity1_id, cluster1_id, entity2_id, cluster2_id from candidate_pairs_2way) dup, data_from_new_source_qgrams q1, inserted_data_qgrams q2
   WHERE dup.entity1_id = q1.entity_id AND dup.entity2_id = q2.entity_id
        AND q1.tag_id = q2.tag_id AND q1.qgram = q2.qgram
   GROUP BY dup.entity1_id, dup.cluster1_id, dup.entity2_id, dup.cluster2_id, q1.tag_id;

RAISE INFO 'Adding qgram frequencies done. Timestamp : %', (select timeofday()) ;

INSERT INTO candidate_attributes_2way
SELECT dup.entity1_id, dup.cluster1_id, dup.entity2_id, dup.cluster2_id, d1.tag_id , - abs(d1.value - d2.value)
FROM (select distinct entity1_id, cluster1_id, entity2_id, cluster2_id from candidate_pairs_2way) dup, data_from_new_source_real d1, inserted_data_real d2
WHERE dup.entity1_id=d1.entity_id AND dup.entity2_id=d2.entity_id AND d1.tag_id = d2.tag_id;

RAISE INFO 'Computing real similarities done. Timestamp : %', (select timeofday()) ;


INSERT INTO candidate_attributes_2way
(SELECT dup.entity1_id, dup.cluster1_id, dup.entity2_id, dup.cluster2_id, d1.tag_id, 0 
FROM (select distinct entity1_id, cluster1_id, entity2_id, cluster2_id from candidate_pairs_2way) dup, data_from_new_source d1, inserted_data d2
WHERE dup.entity1_id = d1.entity_id AND dup.entity2_id = d2.entity_id AND d1.tag_id = d2.tag_id)
EXCEPT ALL (select entity1_id, cluster1_id, entity2_id, cluster2_id, tag_id, 0 from candidate_attributes_2way);

RAISE INFO 'Adding zero qgrams done. Timestamp : %', (select timeofday()) ;


CREATE TEMP TABLE feature_nulls AS
     SELECT * FROM features
      WHERE t1 IS NULL;

null_prod_m := product(f_given_m) FROM feature_nulls;
null_prod_u := product(f_given_u) FROM feature_nulls;

TRUNCATE similarity_2way_join_result;
Insert into similarity_2way_join_result
SELECT cluster1_id, cluster2_id, entity1_id, entity2_id, null,
       null_prod_m *    est_dup  * product(f.f_given_m) / product(n.f_given_m),
       null_prod_u * (1-est_dup) * product(f.f_given_u) / product(n.f_given_u)
FROM candidate_attributes_2way c, features f, feature_nulls n
WHERE c.tag_id = f.tag_id and c.tag_id = n.tag_id
  and c.similarity >= f.t1 and c.similarity < f.t2 
GROUP BY cluster1_id, cluster2_id,entity1_id, entity2_id;

RAISE INFO 'similarity_2way_join done. Timestamp : %', (select timeofday()) ;

DROP TABLE feature_nulls;

/*
UPDATE similarity_2way_join_result s
SET cluster1_id = (Select min(cluster_id) FROM data_from_new_source d where s.entity1_id = d.entity_id);

UPDATE similarity_2way_join_result s
SET cluster2_id = (Select cluster_id FROM entity_clustering d where s.entity2_id = d.entity_id);

RAISE INFO 'Computing prob of dup done. Timestamp : %', (select timeofday()) ;
*/

UPDATE similarity_2way_join_result
SET m_prob = prob_s_m / (prob_s_m + prob_s_u)
WHERE prob_s_m >0 OR prob_s_u >0;

RAISE INFO 'Two-way-join done. Timestamp : %', (select timeofday()) ;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION incr_cluster(real) RETURNS void AS
$$
DECLARE
  m_prob_threshold ALIAS FOR $1;  
BEGIN
	
TRUNCATE similarity_cluster_join_result;
INSERT INTO similarity_cluster_join_result 
	SELECT cluster1_id, cluster2_id, sum(m_prob) / en.entity_count / eo.entity_count
	FROM similarity_2way_join_result s, entity_count_per_cluster_in_new_source en, entity_count_per_cluster_in_old_sources eo
	WHERE s.cluster1_id = en.cluster_id AND s.cluster2_id=eo.cluster_id
	GROUP BY cluster1_id, cluster2_id, en.entity_count, eo.entity_count;

-- TODO: we removed some pairs from self_join results. We should do that after computing the aggregate score for pairs of clusters
DELETE FROM similarity_cluster_join_result
WHERE similarity < m_prob_threshold;

-- now, for each entity, decide which cluster to add to (or form a new cluster)

TRUNCATE match_cluster;
INSERT INTO match_cluster 
SELECT m.cluster1_id, min(cluster2_id) as cluster2_id
FROM max_sim m, similarity_cluster_join_result j
WHERE m.cluster1_id = j.cluster1_id AND m.max_sim_val = j.similarity
GROUP BY m.cluster1_id;

-- finally, udpate the record clustering, and the clusters info

INSERT INTO inserted_data (cluster_id, entity_id, tag_id, value) 
SELECT cluster_id, entity_id, tag_id, value 
FROM data_from_new_source 
WHERE cluster_id NOT IN (SELECT cluster1_id FROM match_cluster);

INSERT INTO inserted_data (cluster_id, entity_id, tag_id, value) 
SELECT s.cluster2_id, d.entity_id, tag_id, value
FROM data_from_new_source d, match_cluster s
WHERE d.cluster_id = s.cluster1_id;


INSERT INTO inserted_data_qgrams (entity_id, cluster_id, tag_id, qgram, freq, qgram_norm) 
SELECT entity_id, cluster_id, tag_id, qgram, freq, qgram_norm
FROM data_from_new_source_qgrams 
WHERE cluster_id NOT IN (SELECT cluster1_id FROM match_cluster);

INSERT INTO inserted_data_qgrams (entity_id, cluster_id, tag_id, qgram, freq, qgram_norm) 
SELECT d.entity_id, s.cluster2_id, tag_id , qgram, freq,  qgram_norm
FROM data_from_new_source_qgrams d, match_cluster s
WHERE d.cluster_id = s.cluster1_id;

INSERT INTO inserted_data_real (cluster_id, entity_id, tag_id, value)
SELECT cluster_id, entity_id, tag_id, value
FROM data_from_new_source_real
WHERE cluster_id NOT IN (SELECT cluster1_id FROM match_cluster);

INSERT INTO inserted_data_real (cluster_id, entity_id, tag_id, value)
SELECT s.cluster2_id, d.entity_id, tag_id, value
FROM data_from_new_source_real d, match_cluster s
WHERE d.cluster_id = s.cluster1_id;

TRUNCATE entity_clustering;
INSERT INTO entity_clustering(entity_id, cluster_id)
SELECT distinct entity_id, cluster_id 
FROM inserted_data;

END;
$$ LANGUAGE plpgsql;


-- Test precision and recall


CREATE OR REPLACE FUNCTION clean_up() RETURNS void AS
$$
BEGIN

TRUNCATE data_from_new_source;
TRUNCATE data_from_new_source_qgrams;
TRUNCATE data_from_new_source_real;
TRUNCATE inserted_data;
TRUNCATE inserted_data_qgrams;
TRUNCATE inserted_data_real;
TRUNCATE tag_values_frequency;
TRUNCATE tag_frequency;
TRUNCATE dedup_qgrams_idf;

END;
$$ LANGUAGE plpgsql;


