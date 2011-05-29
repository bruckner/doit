
create or replace function mdl_init () returns void as
$$
begin
  if schema_exists('mdl') then
    drop schema mdl cascade;
  end if;
  create schema mdl;

  -- Training data lives here
  create table mdl.dictionaries (
  	 tag_code text,
	 value text
  );

  -- Test data lives here
  create table mdl.sources (source_id integer);

  create table mdl.input (
  	 source_id integer,
	 name text,
	 value text
  );

  create table mdl.results (
  	 source_id integer,
	 name text,
	 match text,
	 description_length float
  );

  create view mdl.output as
       select r.source_id, r.name, r.match, f.tag_code, r.description_length
         from (select x.*
                 from mdl.results x,
	             (select source_id, name, min(description_length) dl
	                from mdl.results
	            group by source_id, name) y
	        where x.source_id = y.source_id
	          and x.name = y.name
	          and x.description_length = y.dl) r 
   inner join doit_fields f
           on r.source_id = f.source_id
  	  and r.name = f.name;

  -- Tables/views for computing description length
  -- In general DL = plogm + avgValLen*(log(alphabetSize)) 
  --               + fplog maxValLen + (f/n)sum_n(sum_p(log (# vals ok /# vals possible)))
  -- In our case, p = 1, m = const, alphabetSize = 128, so we get
  -- DL = avgValLen*7 + f*log maxValLen + (f/n) sum_n[log(#vals ok) - log(#vals possible)]
  -- Where n is size of input dict, f is fraction of values accepted,
  -- and (#vals ok/#vals possible) is length specific.

  create view mdl.dict_card_by_len as
       select tag_code, length(value) l, count(*) card
         from mdl.dictionaries
     group by tag_code, l;

  create view mdl.input_dict_stats as
       select source_id, name, count(*) n,
              avg(length(value)) avglen, max(length(value)) maxlen
         from mdl.input
     group by source_id, name;

  create view mdl.input_match_counts_by_len as
       select i.source_id, i.name, d.tag_code, length(i.value) l, count(*) card
         from mdl.input i, mdl.dictionaries d
	where i.value = d.value
     group by i.source_id, i.name, d.tag_code, l;
/*
        union
       select source_id, name, 'DEFAULT' tag_code, length(value) l, 0 card
         from mdl.input
     group by source_id, name, tag_code, l;
*/

  create view mdl.input_match_fracs as
       select m.source_id, m.name, m.tag_code, (sum(m.card)::float / s.n::float) f
         from mdl.input_match_counts_by_len m, mdl.input_dict_stats s
	where m.source_id = s.source_id
	  and m.name = s.name
     group by m.source_id, m.name, m.tag_code, s.n;


  create view mdl.description_length as
       select i.source_id, i.name, i.tag_code,
       	      (f.f * ln(s.maxlen)) term1, (1.0 - f.f) * s.avglen * ln(128) term2,
	      (f.f / s.n::float) * sum(ln(i.card * l.card)) term3
       	      /*(ln(128)*s.avglen) term1, (f.f * ln(s.maxlen)) term2,
	      (f.f / s.n::float) * sum(ln(l.card) - l.l * ln(128)) term3*/
         from mdl.input_match_counts_by_len i, mdl.input_dict_stats s,
	      mdl.input_match_fracs f, mdl.dict_card_by_len l
	where i.source_id = s.source_id
	  and i.name = s.name
	  and i.source_id = f.source_id
	  and i.name = f.name
	  and i.tag_code = f.tag_code
	  and i.tag_code = l.tag_code
     group by i.source_id, i.name, i.tag_code, s.avglen, s.maxlen, f.f, s.n;

end
$$ language plpgsql;


create or replace function mdl_load_training () returns void as
$$
begin
  drop index if exists mdl.idx_dictionaries_value;

  delete from mdl.dictionaries;

  insert into mdl.dictionaries
  select tag_code, value
    from training.data
   where value is not null
group by tag_code, value;

  create index idx_dictionaries_value on mdl.dictionaries using hash (value);
end
$$ language plpgsql;


create or replace function mdl_add_source (integer) returns integer as
$$
begin
	insert into mdl.sources (source_id) values ($1);
	return $1;
end
$$ language plpgsql;


create or replace function mdl_load_test () returns void as
$$
begin
  drop index if exists mdl.idx_input_value;

  delete from mdl.input;

  insert into mdl.input (source_id, name, value)
  select source_id, name, value
    from doit_data
   where source_id
      in (select source_id from mdl.sources)
     and value is not null;

  create index idx_input_value on mdl.input using hash (value);
end
$$ language plpgsql;


create or replace function mdl_load_results () returns void as
$$
begin
  delete from mdl.results;

  insert into mdl.results (source_id, name, match, description_length)
  select source_id, name, tag_code, term1+term2+term3 dl
    from mdl.description_length;

end
$$ language plpgsql;
