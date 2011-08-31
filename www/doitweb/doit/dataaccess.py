import psycopg2


# convert float (0 to 1) to 8 bit web color (e.g. 00 to ff)
def f2c(x):
	if x > 1.0:
		x = 1.0
	if x < 0.0:
		x = 0.0
	c = hex(int(255*x))[2:]
	if len(c) == 1:
		c = '0' + c
	return c


class DoitDB:
    conn = None

    def __init__(self, dbname):
        if self.conn is None:
            self.conn = psycopg2.connect(database=dbname, user='django', password='django', host='localhost')

    def sources(self):
        cur = self.conn.cursor()
        cur.execute('SELECT DISTINCT source_id FROM dan.nr_raw_results;')
        
        source_list = []
        for s in cur.fetchall():
            source_list.append(s[0])
            
        cur.close()
        return source_list


    def matches(self, source_id):
        cur = self.conn.cursor()
        cur.execute('SELECT a.name, b.id as match_id, b.name as match, a.score ' +
                      'FROM dan.nr_ncomp_results a, dan.global_attributes b ' +
                     'WHERE a.match = b.id ' +
                       'AND a.source_id = %s ' +
                  'ORDER BY score desc;', (source_id,))

        scores = dict()
        for rec in cur.fetchall():
            scores.setdefault(rec[0], [])
            scores[rec[0]].append((rec[2], rec[1], rec[3], f2c(rec[3]/3.0), f2c(1.0 - rec[3]/4.0)))

        return scores

    def lowscorers(self, n):
	    cur = self.conn.cursor()
	    cur.execute('SELECT a.name, b.id as match_id, b.name as match, a.score, a.source_id ' +
			  'FROM dan.nr_ncomp_results_tbl a, dan.global_attributes b, ' +
			       '(SELECT source_id, name FROM dan.nr_ncomp_max_results ORDER BY score ASC LIMIT %s) c '
			 'WHERE a.match = b.id ' +
			   'AND a.source_id = c.source_id AND a.name = c.name ' +
		      'ORDER BY score desc;', (n,))

	    scores = dict()
	    for rec in cur.fetchall():
		    scores.setdefault(unicode(rec[4]) + ': ' +  rec[0], [])
		    scores[unicode(rec[4]) + ': ' + rec[0]].append((rec[2], rec[1], rec[3], f2c(rec[3]/3.0), f2c(1.0 - rec[3]/4.0)))

	    return scores

    # get metadata about a given field
    def fieldmeta(self, source_id, field_name):
	    cur = self.conn.cursor()
	    cmd = 'SELECT n, n_distinct FROM ? WHERE source_id = %s AND name = %s;'
	    cur.execute(cmd, (source_id, field_name))
	    return None

    # example values from a local data field
    def fieldexamples(self, source_id, field_name, n, distinct=True):
	cur = self.conn.cursor()
	if distinct:
		cmd = 'SELECT value FROM ' + \
		    ' (SELECT DISTINCT value FROM public.doit_data WHERE source_id = %s AND name = %s) t ' + \
		    'ORDER BY random() LIMIT %s;'
	else:
		cmd = 'SELECT value FROM public.doit_data WHERE source_id = %s AND name = %s ' + \
		    '     AND value IS NOT NULL ORDER BY random() LIMIT %s;'

	cur.execute(cmd, (int(source_id), field_name, int(n)))

	values = []
	for rec in cur.fetchall():
		values.append(rec[0])
	return values


    # example values from a global attribute
    def globalfieldexamples(self, att_id, n, distinct=True):
	cur = self.conn.cursor()
	if distinct:
		cmd = 'SELECT value FROM (SELECT DISTINCT value FROM dan.mdl_dictionaries ' + \
		    '   WHERE att_id = %s) t ORDER BY random() LIMIT %s;'
	else:
		cmd = 'SELECT value FROM dan.mdl_dictionaries WHERE att_id = %s ' + \
		    '     AND VALUE IS NOT NULL ORDER BY random() LIMIT %s;'

	cur.execute(cmd, (int(att_id), int(n)))

	values = []
	for rec in cur.fetchall():
		values.append(rec[0])
	return values


    # get a list of values shared by a local field and global attribute
    def sharedvalues(self, source_id, field_name, att_id, n=10):
	    cur = self.conn.cursor()
	    cur.execute('SELECT DISTINCT value FROM dan.mdl_dictionaries ' +
			'WHERE value IN (SELECT DISTINCT value FROM public.doit_data WHERE source_id = %s AND name = %s) ' +
			'  AND att_id = %s LIMIT %s', (source_id, field_name, att_id, n))

	    values = []
	    for rec in cur.fetchall():
		    values.append(rec[0])
	    return values


    # 1 example value for each field in a source
    def examplevalues(self, source_id):
        cur = self.conn.cursor()

        r = 1 # <-- TODO: replace with rand!
        cur.execute('CREATE TEMP TABLE tmp_egs__%s AS SELECT name, value, random() r ' +
                      'FROM public.doit_data WHERE source_id = %s AND value IS NOT NULL;' +
                    'SELECT a.name, a.value FROM tmp_egs__%s a, (SELECT name, MAX(r) r FROM tmp_egs__%s GROUP BY name) b ' + 
                     'WHERE a.name = b.name AND a.r = b.r;', (r,source_id,r,r))

        egs = dict()
        for rec in cur.fetchall():
            egs[rec[0]] = rec[1]

        return egs


    # individual scoring breakdown
    def indivscores(self, source_id, attr_name):
	    cur = self.conn.cursor()

	    cur.execute('SELECT a.method_name, b.id as match_id, b.name as match, a.score, c.score as cscore ' +
			  'FROM dan.nr_raw_results a, dan.global_attributes b, ' +
			       'dan.nr_ncomp_results_tbl c ' +
			 'WHERE a.source_id = %s AND a.name = %s ' +
			   'AND a.source_id = c.source_id AND a.name = c.name ' +
			   'AND a.match = b.id AND a.match = c.match;'
			,(int(source_id), attr_name))

	    matches = dict()
	    for rec in cur.fetchall():
		    matches.setdefault(rec[1], {'id': rec[1], 'name': rec[2], 'cscore': rec[4]})
		    matches[rec[1]][rec[0]] = rec[3]

	    return sorted(matches.values(), key=lambda match: match['cscore'], reverse=True)
