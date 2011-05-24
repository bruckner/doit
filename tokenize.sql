
create or replace function tokenize (text) returns setof text as
$$
begin
  return query
  select tokid || '||' || trim(both '{}' from ts_lexize('english_stem', token)::text)
    from (
    	 select * from ts_parse('default', $1)
	  where tokid != 12
	    and length(token) < 2048
	 ) t;
end
$$ language plpgsql;

