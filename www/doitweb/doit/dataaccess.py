import psycopg2
from operator import itemgetter

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
            self.conn = psycopg2.connect(database=dbname, user='django',
                                         password='django', host='localhost')

    def sources(self):
        cur = self.conn.cursor()
        cmd = '''SELECT lf.source_id, COUNT(*) AS total, COUNT(am.global_id) mapped,
                        COUNT(am.global_id)::FLOAT / COUNT(*) AS ratio
                   FROM local_fields lf LEFT JOIN attribute_mappings am
                     ON lf.id = am.local_id 
                  WHERE lf.id IN (SELECT field_id FROM nr_raw_results)
               GROUP BY lf.source_id
               ORDER BY COUNT(*) - COUNT(am.global_id) DESC, total DESC;'''
        cur.execute(cmd)
        source_list = []
        for s in cur.fetchall():
            source_list.append({'id': s[0], 'n_attr': s[1], 
                                'n_mapped': s[2]})
        cur.close()
        return source_list

    def process_source(self, source_id, method_index):
	cur = self.conn.cursor()
	methods = ['nr', 'qgrams', 'dist', 'mdl', 'ngrams']
	method_name = methods[int(method_index)]
	if method_name == 'nr':
	    cur.execute('SELECT nr_composite_load();')
	else:
	    cmd = 'SELECT ' + method_name + '_results_for_source(%s);'
	    cur.execute(cmd, (source_id,))
	self.conn.commit()
	return method_name

    def source_meta(self, source_id):
        cur = self.conn.cursor()
        cmd = 'SELECT meta_name, value FROM local_source_meta ' \
              ' WHERE source_id = %s;'
	cur.execute(cmd, (source_id,))
	metadata = []
	for rec in cur.fetchall():
		metadata.append({'name': rec[0], 'value': rec[1]})
	return metadata

    def field_meta(self, field_id):
	cur = self.conn.cursor()
	cmd = 'SELECT meta_name, value FROM local_field_meta ' \
              ' WHERE field_id = %s;'
	cur.execute(cmd, (field_id,))
	metadata = []
	for rec in cur.fetchall():
		metadata.append({'name': rec[0], 'value': rec[1]})
	cmd = 'SELECT local_id, local_name, local_desc ' \
              '  FROM local_fields WHERE id = %s;'
	cur.execute(cmd, (field_id,))
	rec = cur.fetchone()
	metadata.insert(0, {'name': 'Description', 'value': rec[2]})
	metadata.insert(0, {'name': 'ID', 'value': rec[0]})
	metadata.insert(0, {'name': 'Name', 'value': rec[1]})
	return metadata

    def create_mappings(self, pairs):
        if len(pairs) == 0:
            return
        cur = self.conn.cursor()
	params = []
	cmd = 'INSERT INTO attribute_mappings (' \
              '    local_id, global_id, confidence, authority, who_created, ' \
              '    when_created, why_created) ' \
              'VALUES '
        for local_id, global_id in pairs:
            cmd = cmd + '(%s, %s, 1.0, 0.5, %s, NOW(), %s), '
	    params.append(local_id)
	    params.append(global_id)
	    params.append('WEB')
	    params.append('WEB')
	cmd = cmd[0:-2] + ';'
	cur.execute(cmd, params)
	self.conn.commit()
	return str([cmd, params])

    def field_mappings(self, source_id):
        cur = self.conn.cursor()
        cmd = 'SELECT ama.local_id, lf.local_name, ama.global_id, ga.name ' \
              '  FROM local_fields lf, attribute_max_affinities ama, ' \
              '       global_attributes ga ' \
              ' WHERE lf.id = ama.local_id ' \
              '   AND ama.global_id = ga.id ' \
              '   AND lf.source_id = %s'
        cur.execute(cmd, (source_id,))
        fields = dict()
        for rec in cur.fetchall():
            fields.setdefault(rec[0], dict())
            fields[rec[0]]['id'] = rec[0]
            fields[rec[0]]['name'] = rec[1]
            fields[rec[0]]['mapping'] = {'id': rec[2], 'name': rec[3]}
            fields[rec[0]].setdefault('matches', [])
        cmd = 'SELECT lf.id, lf.local_name, nnr.match_id, ga.name as match, ' \
              '       nnr.score ' \
              '  FROM nr_ncomp_results_tbl nnr, local_fields lf, ' \
              '       global_attributes ga ' \
              ' WHERE nnr.match_id = ga.id  AND nnr.field_id = lf.id ' \
              '   AND nnr.source_id = %s ' \
              'ORDER BY score desc;'
        cur.execute(cmd, (source_id,))
        for rec in cur.fetchall():
            fields.setdefault(rec[0], dict())
            fields[rec[0]]['id'] = rec[0]
            fields[rec[0]]['name'] = rec[1]
            fields[rec[0]].setdefault('mapping', None)
            fields[rec[0]].setdefault('matches', [])
            fields[rec[0]]['matches'].append({
                'id': rec[2], 'name': rec[3], 'score': rec[4],
                'green': f2c(rec[4]/2.0), 'red': f2c(1.0 - rec[4] / 3.0)})
        return fields

    def lowscorers(self, n):
        pass
#	    cur = self.conn.cursor()
#	    cur.execute('SELECT a.name, b.id as match_id, b.name as match, a.score, a.source_id ' +
#			  'FROM dan.nr_ncomp_results_tbl a, dan.global_attributes b, ' +
#			       '(SELECT source_id, name FROM dan.nr_ncomp_max_results ORDER BY score ASC LIMIT %s) c ' +
#			 'WHERE a.match = b.id ' +
#			   'AND a.source_id = c.source_id AND a.name = c.name ' +
#		      'ORDER BY score desc;', (n,))
#
#	    scores = dict()
#	    for rec in cur.fetchall():
#		    scores.setdefault(unicode(rec[4]) + ': ' +  rec[0], [])
#		    scores[unicode(rec[4]) + ': ' + rec[0]].append((rec[2], rec[1], rec[3], f2c(rec[3]/3.0), f2c(1.0 - rec[3]/4.0)))
#
#	    return scores


    def fieldname(self, field_id):
	    cur = self.conn.cursor()
	    cmd = 'SELECT local_name FROM local_fields WHERE id = %s'
	    cur.execute(cmd, (field_id,))
	    return cur.fetchone()[0]

    # get metadata about a given field
    def fieldmeta(self, field_id):
	    cur = self.conn.cursor()
	    cmd = 'SELECT * FROM local_field_meta WHERE field_id = %s;'
	    cur.execute(cmd, (field_id,))
	    # TODO: fetch the results...
	    return None

    # example values from a local data field
    def fieldexamples(self, field_id, n, distinct=True):
	cur = self.conn.cursor()
	cmd = 'SELECT value FROM local_mdl_dictionaries ' \
              ' WHERE field_id = %s ORDER BY random() LIMIT %s;'
	cur.execute(cmd, (int(field_id), int(n)))
	values = []
	for rec in cur.fetchall():
		values.append(rec[0])
	return values


    # example values from a global attribute
    def globalfieldexamples(self, att_id, n, distinct=True):
	cur = self.conn.cursor()
	cmd = 'SELECT value FROM global_mdl_dictionaries ' \
              ' WHERE att_id = %s ORDER BY random() LIMIT %s;'
	cur.execute(cmd, (int(att_id), int(n)))
	values = []
	for rec in cur.fetchall():
		values.append(rec[0])
	return values

    # get a list of values shared by a local field and global attribute
    def sharedvalues(self, field_id, att_id, n=10):
        cur = self.conn.cursor()
        cmd = 'SELECT DISTINCT value FROM global_mdl_dictionaries ' \
              ' WHERE value IN (SELECT value FROM local_data ' \
              '                  WHERE field_id = %s) ' \
              '   AND att_id = %s LIMIT %s'
        cur.execute(cmd, (field_id, att_id, n))
        values = []
        for rec in cur.fetchall():
            values.append(rec[0])
        return values

    # 1 example value for each field in a source
    def examplevalues(self, source_id):
        cur = self.conn.cursor()
        r = 1 # <-- TODO: replace with rand!
        cmd = 'CREATE TEMP TABLE tmp_egs__%s AS ' \
	      '     SELECT field_id, value, random() r ' \
              '       FROM local_mdl_dictionaries ' \
	      '      WHERE source_id = %s AND value IS NOT NULL' \
              '      LIMIT 10000; ' \
              'SELECT a.field_id, a.value ' \
              '  FROM tmp_egs__%s a, (SELECT field_id, MAX(r) r ' \
              '                       FROM tmp_egs__%s GROUP BY field_id) b ' \
              ' WHERE a.field_id = b.field_id AND a.r = b.r;'
        cur.execute(cmd, (r,source_id,r,r))
        egs = dict()
        for rec in cur.fetchall():
            egs[rec[0]] = rec[1]
        return egs

    # individual scoring breakdown
    def indivscores(self, field_id):
        cur = self.conn.cursor()
        cmd = 'SELECT rr.method_name, rr.match_id, ga.name as match, ' \
              '       rr.score, cr.score as cscore ' \
              '  FROM nr_raw_results rr, dan.global_attributes ga, ' \
              '       nr_ncomp_results_tbl cr ' \
              ' WHERE rr.field_id = %s ' \
              '   AND rr.field_id = cr.field_id ' \
              '   AND rr.match_id = ga.id AND rr.match_id = cr.match_id;'
        cur.execute(cmd,(int(field_id),))
        matches = dict()
        for rec in cur.fetchall():
            matches.setdefault(rec[1], {'id': rec[1], 'name': rec[2],
                                        'cscore': rec[4]})
            matches[rec[1]][rec[0]] = rec[3]
        return sorted(matches.values(), key=itemgetter('cscore'), reverse=True)
