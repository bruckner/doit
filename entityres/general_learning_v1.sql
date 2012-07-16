/*
Input tables:

1- data_from_new_source: this should contains the data we need to dedup. I use the procedure extract_new_data(0,new_category_id) to extract a specific category from doit_data
2- tag_frequency, tag_values_frequency, qgrams_idf (already built for you by extract_new_data)
4- training_clustering

*/



DROP TABLE IF EXISTS features CASCADE;
CREATE TABLE features(tag_id integer, t1 double precision, t2 double precision, f_given_m double precision, f_given_u double precision);


DROP TABLE IF EXISTS duplicate_pairs CASCADE;
CREATE TABLE duplicate_pairs (entity1_id integer, entity2_id integer);
DROP TABLE IF EXISTS duplicate_attributes CASCADE;
CREATE TABLE duplicate_attributes (entity1_id integer, entity2_id integer, tag_id integer, similarity double precision);

DROP TABLE IF EXISTS random_pairs CASCADE;
CREATE TABLE random_pairs (entity1_id integer, entity2_id integer);
DROP TABLE IF EXISTS random_attributes CASCADE;
CREATE TABLE random_attributes (entity1_id integer, entity2_id integer, tag_id integer, similarity double precision);


-- populate manual_rules such that tag_id refers to id in global_attributes, dup_key is true if similar values indicate duplicates, or false if dissimilar values indicate non-duplciates, and probability indicate the confidence in this rule
-- NOTE: in case of having an attribute with both dup_key = true and false, place two separate tuples in manual_rules for this attribute
--DROP TABLE IF EXISTS manual_rules CASCADE;
--CREATE TABLE manual_rules (tag_id int, range_start real, range_end real, prob_m_given_f real);

DROP TABLE IF EXISTS learning_attrs CASCADE;
CREATE TABLE learning_attrs(tag_id int);

DROP TABLE IF EXISTS questions CASCADE;
CREATE TABLE questions(entity1_id int, entity2_id int, tag_id int, bin int, range_start real, range_end real, human_label text); 	-- human_label = 'Yes', 'No', 'Maybe' (case sensitive)

CREATE OR REPLACE FUNCTION populate_questions() RETURNS void AS
$$
DECLARE
global_attr_id int;
questions_per_attr int;
bin int;
bin_count int;
min_sim double precision;
max_sim double precision;
range_start double precision;
range_end double precision;
BEGIN


TRUNCATE random_pairs;
INSERT INTO random_pairs
SELECT distinct e1,e2 FROM (
SELECT a.entity_id as e1, b.entity_id as e2
FROM (select entity_id from data_from_new_source order by random() limit 10000) a, (select entity_id from data_from_new_source order by random() limit 10000) b
where a.entity_id <> b.entity_id 
order by random()
limit 100000) a;

TRUNCATE random_attributes;

questions_per_attr := ceil((select value::integer from configuration_properties where name= 'question_budget') / (select count(*) from learning_attrs))::integer;
bin_count := (select value::int from configuration_properties where name= 'bins_count');

FOR global_attr_id in Select tag_id from learning_attrs LOOP

IF (SELECT type from global_attributes where id = global_attr_id) = 'TEXT' THEN
INSERT INTO random_attributes
     SELECT dup.entity1_id, dup.entity2_id, q1.tag_id, SUM(q1.freq * q2.freq)/max(q1.qgram_norm)/max(q2.qgram_norm) AS sim
       FROM random_pairs dup, data_from_new_source_qgrams q1, data_from_new_source_qgrams q2
        WHERE dup.entity1_id = q1.entity_id AND dup.entity2_id = q2.entity_id AND q1.tag_id = global_attr_id
        AND q1.tag_id = q2.tag_id AND q1.qgram = q2.qgram
   GROUP BY dup.entity1_id, dup.entity2_id, q1.tag_id;

INSERT INTO random_attributes
(SELECT dup.entity1_id, dup.entity2_id, d1.tag_id, 0
FROM random_pairs dup, data_from_new_source d1, data_from_new_source d2
WHERE dup.entity1_id = d1.entity_id AND dup.entity2_id = d2.entity_id AND d1.tag_id = d2.tag_id AND d1.tag_id = global_attr_id)
EXCEPT ALL (select  entity1_id, entity2_id, tag_id, 0 FROM random_attributes);

ELSE
 
INSERT INTO random_attributes
SELECT entity1_id, entity2_id, d1.tag_id , - abs(d1.value - d2.value)
FROM random_pairs dup, data_from_new_source_real d1, data_from_new_source_real d2
WHERE dup.entity1_id=d1.entity_id AND dup.entity2_id=d2.entity_id AND d1.tag_id = d2.tag_id;

END IF;

min_sim := (select min(similarity) from random_attributes where tag_id = global_attr_id);
max_sim := (select max(similarity) + 1e-7 from random_attributes where tag_id = global_attr_id);

TRUNCATE questions;

FOR bin in 0..bin_count-1 LOOP

range_start := min_sim + bin / bin_count::real * (max_sim - min_sim);
range_end := min_sim + (bin+1) / bin_count::real * (max_sim - min_sim);

INSERT INTO questions
Select entity1_id, entity2_id, global_attr_id, bin,  range_start, range_end, null
FROM random_attributes 
WHERE tag_id = global_attr_id and similarity >= range_start AND similarity < range_end 
ORDER BY random()
limit (ceil(questions_per_attr / bin_count::real))::int;

END LOOP;

END LOOP;


END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION learn_manual_weights() RETURNS void AS
$$
DECLARE
est_dup real;
abs_perf_threshold real;
rel_perf_threshold real;
i int;

tag_record RECORD;
sim_record RECORD;
sim_itr integer;
sim_step integer;
prob_s_n double precision;
prob_f double precision;
acc_prob_m double precision;
acc_prob_u double precision;
global_attr_itr int;
prob_f_given_u double precision;
prob_f_given_m double precision;
prob_m_given_f double precision;
BEGIN

--est_dup := (select to_num(value) from configuration_properties where name='est_dup');
rel_perf_threshold := (select to_num(value) from configuration_properties where name='rel_perf_threshold');
abs_perf_threshold := (select to_num(value) from configuration_properties where name='abs_perf_threshold');

UPDATE global_attributes
SET threshold = null;

TRUNCATE features;

--now for each tag_id, and for each threshold value T, get Pr(S>T|M) and Pr(S>T)
est_dup:=0;

FOR tag_record IN select distinct tag_id, bin, range_start, range_end from questions order by tag_id, bin LOOP

SELECT INTO prob_f (SELECT count(*) FROM random_attributes r where r.tag_id = tag_record.tag_id and r.similarity >= tag_record.range_start and r.similarity < tag_record.range_end)/ (select count(*) from random_pairs)::double precision;			

prob_m_given_f:= (select count(*) from questions where tag_id = tag_record.tag_id and bin = tag_record.bin and human_label = 'Yes')/ (select count(*) from questions where tag_id = tag_record.tag_id and bin = tag_record.bin and (human_label = 'Yes' or human_label = 'No'))::double precision;

--Raise info 'Adding % * % to est_prob', prob_f , prob_m_given_f;
est_dup:= est_dup + prob_f * prob_m_given_f;
	
END LOOP;

Raise Info 'est_dup = %', est_dup;

FOR tag_record IN select distinct tag_id, bin, range_start, range_end from questions order by tag_id, bin LOOP

-- get \tau given pr(f|M) and pr(M)
			
	IF (SELECT count(*) from features where tag_id = tag_record.tag_id and t1 is null)=0 THEN		
		SELECT INTO prob_s_n (SELECT count(*) FROM random_attributes where tag_id = tag_record.tag_id)/ (select count(*) from random_pairs)::double precision;		
		INSERT INTO features values (tag_record.tag_id, null, null, 1-prob_s_n, 1-prob_s_n);
	END IF;
	--SELECT ceil(count(*)/200.0) INTO sim_step FROM random_attributes WHERE tag_id = tag_record.tag_id;
	
	SELECT INTO prob_f (SELECT count(*) FROM random_attributes r where r.tag_id = tag_record.tag_id and r.similarity >= tag_record.range_start and r.similarity < tag_record.range_end)/ (select count(*) from random_pairs)::double precision;			

	
	prob_m_given_f:= (select count(*) from questions where tag_id = tag_record.tag_id and bin = tag_record.bin and human_label = 'Yes')/ (select count(*) from questions where tag_id = tag_record.tag_id and bin = tag_record.bin and (human_label = 'Yes' or human_label = 'No'))::double precision;

	prob_f_given_m := prob_m_given_f * prob_f / est_dup;
	prob_f_given_u := (1-prob_m_given_f) * prob_f / (1 - est_dup);
	INSERT INTO features values (tag_record.tag_id, tag_record.range_start, tag_record.range_end, prob_f_given_m, prob_f_given_u);	
	
END LOOP;

RAISE INFO 'now, setting the threshold';

FOR global_attr_itr IN SELECT distinct tag_id from features LOOP
	
	acc_prob_m := (select f_given_m FROM features where t1 is null and tag_id =global_attr_itr);
	acc_prob_u := (select f_given_u FROM features where t1 is null and tag_id =global_attr_itr);
	
	FOR tag_record IN (SELECT * FROM features where tag_id = global_attr_itr and t1 is not null order by t1) LOOP
		FOR i in 1..200 LOOP		
			acc_prob_m := acc_prob_m + tag_record.f_given_m / 200.0;
			acc_prob_u := acc_prob_u + tag_record.f_given_u / 200.0;		
			IF (1-acc_prob_m > abs_perf_threshold and (1-acc_prob_m) / (1-acc_prob_u) > rel_perf_threshold) THEN			
				RAISE INFO 'Found threshold at iteration % in [%,%], acc_prob_m = %, acc_prob_u = %', i, tag_record.t1, tag_record.t2,acc_prob_m, acc_prob_u;
				UPDATE global_attributes t
				SET threshold = tag_record.t1 + (tag_record.t2 - tag_record.t1) * i / 200.0
				WHERE id = global_attr_itr; 
				EXIT;
			END IF;						
		END LOOP;		
		IF (select threshold is not null from global_attributes where id = global_attr_itr)  THEN		
			EXIT;
		END IF;
		
	END LOOP;

END LOOP;

update features set f_given_m=1e-9 where f_given_m=0;
update features set f_given_u=1e-9 where f_given_u=0;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION learn_weights(training_source int) RETURNS void AS
$$
DECLARE

-- training_mode: 1 = from goby_entity_result, 2 = from entity_clustering
prob_dist_threshold real;
est_dup real;
bins_count integer;
rel_perf_threshold real;
abs_perf_threshold real;

min_sim_m double precision;
min_sim_u double precision;
max_sim_m double precision;
max_sim_u double precision;
cur_sim double precision;

prob_s_n_given_m double precision;
prob_s_n_given_u double precision;
prob_s_n double precision;

prob_f_given_m double precision;
prob_f double precision;
prob_f_given_u double precision;

sim_thr_max_likelihood double precision;
next_sim double precision;
tag_record RECORD;
pdf_diff double precision;
m_corr double precision;
u_corr double precision;
acc_prob_f_given_m double precision;
threshold_set bool;
sim_record RECORD;
sim_itr integer;
sim_step integer;
BEGIN

prob_dist_threshold:= (select to_num(value) from configuration_properties where name='prob_dist_threshold');
est_dup := (select to_num(value) from configuration_properties where name='est_dup');
bins_count:= (select to_num(value)::int from configuration_properties where name='bins_count');
rel_perf_threshold := (select to_num(value) from configuration_properties where name='rel_perf_threshold');
abs_perf_threshold := (select to_num(value) from configuration_properties where name='abs_perf_threshold');


TRUNCATE features;

UPDATE global_attributes 
SET threshold = null;

TRUNCATE duplicate_pairs;
--add duplciates from goby clustering

IF (training_source = 1) THEN
INSERT INTO duplicate_pairs 
select la.id, lb.id 
from public.goby_entity_result a, public.goby_entity_result b , local_entities la, local_entities lb
where a.global_entity_id = b.global_entity_id
AND a.local_entity_id = la.local_id::integer AND b.local_entity_id = lb.local_id::integer
AND la.id < lb.id 
AND la.id in (select entity_id from data_from_new_source)
AND lb.id in (select entity_id from data_from_new_source);

ELSIF (training_source = 2) THEN

INSERT INTO duplicate_pairs 
select a.entity_id, b.entity_id
from entity_clustering a, entity_clustering b 
where a.cluster_id = b.cluster_id
AND a.entity_id < b.entity_id
AND a.entity_id in (select entity_id from data_from_new_source)
AND b.entity_id in (select entity_id from data_from_new_source);

END IF;
--optionally add human-lablebed duplcaites as well
/*
INSERT INTO duplicate_pairs
SELECT entity1_id, entity2_id
FROM similar_entities_manual_weights
WHERE human_label= 'Yes' AND entity1_id in (select entity_id from data_from_new_source) and entity2_id in (select entity_id from data_from_new_source);
*/


TRUNCATE duplicate_attributes;

INSERT INTO duplicate_attributes
     SELECT dup.entity1_id, dup.entity2_id, q1.tag_id, SUM(q1.freq * q2.freq)/max(q1.qgram_norm)/max(q2.qgram_norm) AS sim
       FROM duplicate_pairs dup, data_from_new_source_qgrams q1, data_from_new_source_qgrams q2
         WHERE dup.entity1_id = q1.entity_id AND dup.entity2_id = q2.entity_id
        AND q1.tag_id = q2.tag_id  AND q1.qgram = q2.qgram
    GROUP BY dup.entity1_id, dup.entity2_id, q1.tag_id;


INSERT INTO duplicate_attributes
SELECT entity1_id, entity2_id, d1.tag_id , - abs(d1.value - d2.value)
FROM duplicate_pairs dup, data_from_new_source_real d1, data_from_new_source_real d2
WHERE dup.entity1_id=d1.entity_id AND dup.entity2_id=d2.entity_id AND d1.tag_id = d2.tag_id;

INSERT INTO duplicate_attributes
(SELECT dup.entity1_id, dup.entity2_id, d1.tag_id, 0
FROM duplicate_pairs dup, data_from_new_source d1, data_from_new_source d2
WHERE dup.entity1_id = d1.entity_id AND dup.entity2_id = d2.entity_id AND d1.tag_id = d2.tag_id) 
 	EXCEPT ALL (select entity1_id, entity2_id, tag_id, 0 FROM duplicate_attributes);

	
TRUNCATE random_pairs;
INSERT INTO random_pairs
SELECT distinct e1,e2 FROM (
SELECT a.entity_id as e1, b.entity_id as e2
FROM (select entity_id from data_from_new_source order by random() limit 10000) a, (select entity_id from data_from_new_source order by random() limit 10000) b
where a.entity_id < b.entity_id 
order by random()
limit 100000) a;


TRUNCATE random_attributes;
INSERT INTO random_attributes
     SELECT dup.entity1_id, dup.entity2_id, q1.tag_id, SUM(q1.freq * q2.freq)/max(q1.qgram_norm)/max(q2.qgram_norm) AS sim
       FROM random_pairs dup, data_from_new_source_qgrams q1, data_from_new_source_qgrams q2
        WHERE dup.entity1_id = q1.entity_id AND dup.entity2_id = q2.entity_id
        AND q1.tag_id = q2.tag_id AND q1.qgram = q2.qgram
   GROUP BY dup.entity1_id, dup.entity2_id, q1.tag_id;

        

INSERT INTO random_attributes
SELECT entity1_id, entity2_id, d1.tag_id , - abs(d1.value - d2.value)
FROM random_pairs dup, data_from_new_source_real d1, data_from_new_source_real d2
WHERE dup.entity1_id=d1.entity_id AND dup.entity2_id=d2.entity_id AND d1.tag_id = d2.tag_id;


INSERT INTO random_attributes
(SELECT dup.entity1_id, dup.entity2_id, d1.tag_id, 0
FROM random_pairs dup, data_from_new_source d1, data_from_new_source d2
WHERE dup.entity1_id = d1.entity_id AND dup.entity2_id = d2.entity_id AND d1.tag_id = d2.tag_id)
EXCEPT ALL (select  entity1_id, entity2_id, tag_id, 0 FROM random_attributes);
--now for each tag_id, and for each threshold value T, get Pr(S>T|M) and Pr(S>T)

RAISE INFO 'Random data computed';

FOR tag_record IN (Select * from global_attributes) LOOP

RAISE INFO 'Processing tag % , %', tag_record.id, tag_record.name;


	Select into min_sim_m, max_sim_m min(similarity), max(similarity) from duplicate_attributes d where d.tag_id = tag_record.id;
	Select into min_sim_u, max_sim_u min(similarity), max(similarity) from random_attributes r where r.tag_id = tag_record.id;
	
	RAISE INFO 'Similarity ranges   min_sim_u=%, min_sim_m=%,  max_sim_u=% , max_sim_m=%', min_sim_u, min_sim_m, max_sim_u, max_sim_m;	
	IF (min_sim_m is null) THEN
		continue;
	END IF;
	
	IF (max_sim_u > max_sim_m) THEN
		max_sim_m := max_sim_u;		
	END IF;
	
	--Raise INFO 'Range of the tag % is [%,%]', tag_record.name, min_sim_1, max_sim_1;
	
	SELECT INTO prob_s_n_given_m (SELECT count(*) FROM duplicate_attributes r where r.tag_id = tag_record.id)/ (select count(*) from duplicate_pairs)::double precision;			
	SELECT INTO prob_s_n (SELECT count(*) FROM random_attributes r where r.tag_id = tag_record.id)/ (select count(*) from random_pairs)::double precision;			
	
	prob_s_n_given_m := greatest(1e-9, 1 - prob_s_n_given_m);
	prob_s_n := 1 - prob_s_n;
	prob_s_n_given_u := least(1,greatest(1e-9,(prob_s_n - prob_s_n_given_m * est_dup)/ (1- est_dup)));
	
	
	INSERT INTO features values(tag_record.id, null, null, prob_s_n_given_m, prob_s_n_given_u);
	
	IF (min_sim_u < min_sim_m) THEN		
		SELECT INTO prob_f (SELECT count(*) FROM random_attributes r where r.tag_id = tag_record.id and r.similarity >= min_sim_u and r.similarity < min_sim_m)/ (select count(*) from random_pairs)::double precision;				
		prob_f_given_m := 1e-9;
		prob_f_given_u := prob_f / (1- est_dup);		
		INSERT INTO features values(tag_record.id, min_sim_u, min_sim_m, prob_f_given_m , prob_f_given_u);
	END IF;
	
	cur_sim := min_sim_m;
	acc_prob_f_given_m := 0;
	threshold_set := false;
	max_sim_m := max_sim_m + 1e-9;
	
	WHILE (cur_sim < max_sim_m) LOOP		
		
		next_sim := cur_sim + (max_sim_m - min_sim_m) / bins_count;
		SELECT INTO prob_f_given_m (SELECT count(*) FROM duplicate_attributes r where r.tag_id = tag_record.id and r.similarity >= cur_sim and r.similarity < next_sim)/ (select count(*) from duplicate_pairs)::double precision;
		SELECT INTO prob_f (SELECT count(*) FROM random_attributes r where r.tag_id = tag_record.id and r.similarity >= cur_sim and r.similarity < next_sim)/ (select count(*) from random_pairs)::double precision;			
		
		prob_f_given_m := greatest(1e-9, prob_f_given_m);				
		prob_f_given_u := least(1,greatest(1e-9,(prob_f - prob_f_given_m * est_dup)/ (1- est_dup)));		
		INSERT INTO features values(tag_record.id, cur_sim , next_sim, prob_f_given_m , prob_f_given_u);
				
		cur_sim := cur_sim + (max_sim_m - min_sim_m) / bins_count;			
		EXIT WHEN abs(max_sim_m - min_sim_m) < 1e-6;
	END LOOP;
	
	SELECT INTO m_corr corr(f_given_m, t1+t2) 
	FROM features
	WHERE tag_id  = tag_record.id;
	
	SELECT INTO u_corr corr(f_given_u, t1+t2)
	FROM features
	WHERE tag_id  = tag_record.id;

	
	RAISE INFO 'For % : M-corr = %, U-corr = %', tag_record.name, m_corr, u_corr;
		-- compute pdf difference 
	SELECT INTO pdf_diff SUM(diff) 
	FROM (SELECT abs(f_given_m - f_given_u) as diff
		FROM features
		WHERE tag_id = tag_record.id) a;
	
	RAISE INFO 'PDF diff for % is %', tag_record.name, pdf_diff;
	IF (pdf_diff < prob_dist_threshold or m_corr < 0 or u_corr > 0) THEN			
		DELETE FROM features
		WHERE tag_id = tag_record.id;	
		
		--UPDATE field_types
		--SET threshold = null 
		--WHERE tag_id = tag_record.id;
	ELSE
		SELECT ceil(count(*)/200.0) INTO sim_step FROM duplicate_attributes WHERE tag_id = tag_record.id;
		sim_itr := 0;
		FOR sim_record IN (SELECT * FROM duplicate_attributes WHERE tag_id = tag_record.id ORDER BY similarity) LOOP
			sim_itr := sim_itr + 1;
			IF (sim_itr % sim_step <> 0) THEN
				continue;
			ELSE
				prob_f_given_m :=  (SELECT count(*) FROM duplicate_attributes r where r.tag_id = tag_record.id and r.similarity >= sim_record.similarity) / (select count(*) from duplicate_pairs)::double precision;								
				SELECT INTO prob_f (SELECT count(*) FROM random_attributes r where r.tag_id = tag_record.id and r.similarity >= sim_record.similarity)/ (select count(*) from random_pairs)::double precision;											
				prob_f_given_u := least(1,greatest(1e-9,(prob_f - prob_f_given_m * est_dup)/ (1- est_dup)));
				
				IF (prob_f_given_m > abs_perf_threshold and prob_f_given_m / prob_f_given_u > rel_perf_threshold) THEN
					RAISE INFO 'Setting threshold of % to %', tag_record.name, sim_record.similarity;
					UPDATE global_attributes t
					SET threshold = sim_record.similarity
					WHERE tag_id = tag_record.id; 
					EXIT;
				END IF;				
			END IF;			
		END LOOP;		
	
	END IF;

END LOOP;

END;
$$ LANGUAGE plpgsql;






