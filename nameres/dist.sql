
create or replace function dist_init () returns void as
$$
begin
  if schema_exists('dist') then
     drop schema dist cascade;
  end if;
  create schema dist;

  -- Global data lives here
  create table dist.global_dists (
  	 tag_code text,
	 n integer,
	 mean float,
	 variance float
  );

  -- Incoming local data lives here
  create table dist.sources (source_id integer);

  create table dist.input (
  	 source_id integer,
	 name text,
	 value numeric
  );

  -- Scores, results, output live here
  create table dist.results (
  	 source_id integer,
	 name text,
	 match text,
	 score float
  );

  -- Tables/views for computations
  create view dist.sums as
       select source_id, name, count(*) n,
              sum(value::float)::float sm, sum(value::float*value::float)::float smsqr
         from dist.input
	where to_num(value::text) is not null
     group by source_id, name;

  create view dist.local_dists as
       select source_id, name, n, sm/n mean, (smsqr - sm*sm/n) / (n-1) variance
         from dist.sums
	where n > 1;
end
$$ language plpgsql;


create or replace function dist_load_training () returns void as
$$
begin
  delete from dist.global_dists;

  insert into dist.global_dists (tag_code, n, mean, variance)
       select tag_code, n, sm/n mean, (smsqr - sm*sm/n)/n variance
         from (select tag_code, count(*) n,
	              sum(value::float)::float sm, sum(value::float*value::float)::float smsqr
	         from training.data
		where to_num(value) is not null
	     group by tag_code) s
	where n > 1;
end
$$ language plpgsql;


create or replace function dist_add_source (integer) returns integer as
$$
begin
	insert into dist.sources (source_id) values ($1);
	return $1;
end
$$ language plpgsql;

create or replace function dist_load_test () returns void as
$$
begin
  delete from dist.input;

  insert into dist.input (source_id, name, value)
       select source_id, name, value::numeric
         from doit_data
	where source_id
	   in (select source_id from dist.sources)
	  and to_num(value) is not null;
end
$$ language plpgsql;


create or replace function dist_load_results () returns void as
$$
begin
  delete from dist.results;

  insert into dist.results (source_id, name, match, score)
       select source_id, name, tag_code, greatest(mrat, vrat) score
         from (select l.source_id, l.name, g.tag_code,
	              greatest(abs(l.mean),abs(g.mean))/least(abs(l.mean),abs(g.mean)) mrat,
		      greatest(abs(l.variance)/abs(g.variance)) / 
		      least(abs(l.variance),abs(g.variance)) vrat
          	 from dist.local_dists l, dist.global_dists g
		where l.mean != 0
		  and l.variance != 0
		  and g.mean != 0
		  and g.variance != 0) t;
	--where (mrat < 1.1 and mrat > 0.9) and (vrat < 1.1 and vrat > 0.9);
end
$$ language plpgsql;
