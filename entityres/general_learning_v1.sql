/*
Input tables:

1- data_from_new_source: this should contains the data we need to dedup. I use the procedure extract_new_data(0,new_category_id) to extract a specific category from doit_data
2- tag_frequency, tag_values_frequency, qgrams_idf (already built for you by extract_new_data)
3- global_attrs_types_thr (only attribute type is needed), field_keys
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




CREATE OR REPLACE FUNCTION learn_weights(real,real,integer, real,real ) RETURNS void AS
$$
DECLARE
prob_dist_threshold ALIAS FOR $1; -- a ratio for truncating the non-distinguishing attributes, suggested value 0.05
est_dup  ALIAS FOR $2; -- estimated probability that a pair is duplicates , suggested value 0.002
bins_count ALIAS FOR $3; -- number of bins per attribute, suggested value 3-5
rel_perf_threshold ALIAS FOR $4; -- suggested value 1000. Decrease the value to get better recall but worse performance
abs_perf_threshold ALIAS FOR $5; -- suggested value 0.2. Decrease the value to get better recall but worse performance.


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


TRUNCATE features;

UPDATE global_attrs_types_thr 
SET threshold = null;

TRUNCATE duplicate_pairs;
--add duplciates from goby clustering
INSERT INTO duplicate_pairs 
select la.id, lb.id 
from public.goby_entity_result a, public.goby_entity_result b , local_entities la, local_entities lb
where a.global_entity_id = b.global_entity_id
AND a.local_entity_id = la.local_id::integer AND b.local_entity_id = lb.local_id::integer
AND la.id < lb.id 
AND la.id in (select entity_id from data_from_new_source)
AND lb.id in (select entity_id from data_from_new_source);

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
SELECT a.entity_id, b.entity_id
FROM (select entity_id from data_from_new_source order by random() limit 10000) a, (select entity_id from data_from_new_source order by random() limit 10000) b
where a.entity_id <> b.entity_id 
order by random()
limit 100000;


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

FOR tag_record IN (Select * from global_attrs_types_thr) LOOP

RAISE INFO 'Processing tag % , %', tag_record.tag_id, tag_record.tag_code;


	Select into min_sim_m, max_sim_m min(similarity), max(similarity) from duplicate_attributes d where d.tag_id = tag_record.tag_id;
	Select into min_sim_u, max_sim_u min(similarity), max(similarity) from random_attributes r where r.tag_id = tag_record.tag_id;
	
	RAISE INFO 'Similarity ranges   min_sim_u=%, min_sim_m=%,  max_sim_u=% , max_sim_m=%', min_sim_u, min_sim_m, max_sim_u, max_sim_m;	
	IF (min_sim_m is null) THEN
		continue;
	END IF;
	
	IF (max_sim_u > max_sim_m) THEN
		max_sim_m := max_sim_u;		
	END IF;
	
	--Raise INFO 'Range of the tag % is [%,%]', tag_record.tag_code, min_sim_1, max_sim_1;
	
	SELECT INTO prob_s_n_given_m (SELECT count(*) FROM duplicate_attributes r where r.tag_id = tag_record.tag_id)/ (select count(*) from duplicate_pairs)::double precision;			
	SELECT INTO prob_s_n (SELECT count(*) FROM random_attributes r where r.tag_id = tag_record.tag_id)/ (select count(*) from random_pairs)::double precision;			
	
	prob_s_n_given_m := greatest(1e-9, 1 - prob_s_n_given_m);
	prob_s_n := 1 - prob_s_n;
	prob_s_n_given_u := least(1,greatest(1e-9,(prob_s_n - prob_s_n_given_m * est_dup)/ (1- est_dup)));
	
	
	INSERT INTO features values(tag_record.tag_id, null, null, prob_s_n_given_m, prob_s_n_given_u);
	
	IF (min_sim_u < min_sim_m) THEN		
		SELECT INTO prob_f (SELECT count(*) FROM random_attributes r where r.tag_id = tag_record.tag_id and r.similarity >= min_sim_u and r.similarity < min_sim_m)/ (select count(*) from random_pairs)::double precision;				
		prob_f_given_m := 1e-9;
		prob_f_given_u := prob_f / (1- est_dup);		
		INSERT INTO features values(tag_record.tag_id, min_sim_u, min_sim_m, prob_f_given_m , prob_f_given_u);
	END IF;
	
	cur_sim := min_sim_m;
	acc_prob_f_given_m := 0;
	threshold_set := false;
	max_sim_m := max_sim_m + 1e-9;
	
	WHILE (cur_sim < max_sim_m) LOOP		
		
		next_sim := cur_sim + (max_sim_m - min_sim_m) / bins_count;
		SELECT INTO prob_f_given_m (SELECT count(*) FROM duplicate_attributes r where r.tag_id = tag_record.tag_id and r.similarity >= cur_sim and r.similarity < next_sim)/ (select count(*) from duplicate_pairs)::double precision;
		SELECT INTO prob_f (SELECT count(*) FROM random_attributes r where r.tag_id = tag_record.tag_id and r.similarity >= cur_sim and r.similarity < next_sim)/ (select count(*) from random_pairs)::double precision;			
		
		prob_f_given_m := greatest(1e-9, prob_f_given_m);				
		prob_f_given_u := least(1,greatest(1e-9,(prob_f - prob_f_given_m * est_dup)/ (1- est_dup)));		
		INSERT INTO features values(tag_record.tag_id, cur_sim , next_sim, prob_f_given_m , prob_f_given_u);
		
		/*
		acc_prob_f_given_m := acc_prob_f_given_m + 	prob_f_given_m;		
		IF (acc_prob_f_given_m > false_negative_threshold and threshold_set = false ) THEN
			RAISE INFO 'Setting threshold of % to %', tag_record.tag_code, cur_sim;
			UPDATE global_attrs_types_thr t
			SET threshold = cur_sim 
			WHERE tag_id = tag_record.tag_id; 
			threshold_set := true;
		END IF;
		*/		
		cur_sim := cur_sim + (max_sim_m - min_sim_m) / bins_count;			
		EXIT WHEN abs(max_sim_m - min_sim_m) < 1e-6;
	END LOOP;
	
	SELECT INTO m_corr corr(f_given_m, t1+t2) 
	FROM features
	WHERE tag_id  = tag_record.tag_id;
	
	SELECT INTO u_corr corr(f_given_u, t1+t2)
	FROM features
	WHERE tag_id  = tag_record.tag_id;

	
	RAISE INFO 'For % : M-corr = %, U-corr = %', tag_record.tag_code, m_corr, u_corr;
		-- compute pdf difference 
	SELECT INTO pdf_diff SUM(diff) 
	FROM (SELECT abs(f_given_m - f_given_u) as diff
		FROM features
		WHERE tag_id = tag_record.tag_id) a;
	
	RAISE INFO 'PDF diff for % is %', tag_record.tag_code, pdf_diff;
	IF (pdf_diff < prob_dist_threshold or m_corr < 0 or u_corr > 0) THEN			
		DELETE FROM features
		WHERE tag_id = tag_record.tag_id;	
		
		--UPDATE field_types
		--SET threshold = null 
		--WHERE tag_id = tag_record.tag_id;
	ELSE
		SELECT ceil(count(*)/200.0) INTO sim_step FROM duplicate_attributes WHERE tag_id = tag_record.tag_id;
		sim_itr := 0;
		FOR sim_record IN (SELECT * FROM duplicate_attributes WHERE tag_id = tag_record.tag_id ORDER BY similarity) LOOP
			sim_itr := sim_itr + 1;
			IF (sim_itr % sim_step <> 0) THEN
				continue;
			ELSE
				prob_f_given_m :=  (SELECT count(*) FROM duplicate_attributes r where r.tag_id = tag_record.tag_id and r.similarity >= sim_record.similarity) / (select count(*) from duplicate_pairs)::double precision;								
				SELECT INTO prob_f (SELECT count(*) FROM random_attributes r where r.tag_id = tag_record.tag_id and r.similarity >= sim_record.similarity)/ (select count(*) from random_pairs)::double precision;											
				prob_f_given_u := least(1,greatest(1e-9,(prob_f - prob_f_given_m * est_dup)/ (1- est_dup)));
				
				IF (prob_f_given_m > abs_perf_threshold and prob_f_given_m / prob_f_given_u > rel_perf_threshold) THEN
					RAISE INFO 'Setting threshold of % to %', tag_record.tag_code, sim_record.similarity;
					UPDATE global_attrs_types_thr t
					SET threshold = sim_record.similarity
					WHERE tag_id = tag_record.tag_id; 
					EXIT;
				END IF;				
			END IF;			
		END LOOP;		
	
	END IF;

END LOOP;

END;
$$ LANGUAGE plpgsql;






