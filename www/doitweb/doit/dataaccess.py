import psycopg2
import copy
import cPickle
from operator import itemgetter

# convert float (0 to 1) to 8 bit web color (e.g. 00 to ff)
def f2c(x):
    if x > 1.0: x = 1.0
    if x < 0.0: x = 0.0
    c = hex(int(255*x))[2:]
    if len(c) == 1:
        c = '0' + c
    return c

# Faster than copy.deepcopy, but totally hacky:
# http://stackoverflow.com/questions/1410615/copy-deepcopy-vs-pickle
def copyhack(obj):
    return cPickle.loads(cPickle.dumps(obj, -1))


class DoitDB:
    conn = None

    def __init__(self, dbname):
        if self.conn is None:
            self.conn = psycopg2.connect(database=dbname, user='django',
                                         password='django', host='localhost')

    def sources(self):
        cur = self.conn.cursor()
        cmd = '''SET enable_nestloop TO off;
                 SELECT ls.id, lsm.value AS "name", nval.value,
                        ls.n_entities, ls.n_fields,
                        COUNT(am.source_id) AS "mapped",
                        COUNT(am.source_id)::FLOAT / ls.n_fields AS "ratio"
                   FROM local_sources ls 
              LEFT JOIN local_source_meta lsm
                     ON ls.id = lsm.source_id
                    AND upper(lsm.meta_name) = 'NAME'
              LEFT JOIN local_source_meta nval
                     ON ls.id = nval.source_id
                    AND upper(nval.meta_name) = '#VALUES'
              LEFT JOIN (
                        SELECT lf.source_id
                          FROM local_fields lf, attribute_mappings m
                         WHERE m.local_id = lf.id
                           AND lf.n_values > 0) am
                     ON ls.id = am.source_id 
                  WHERE n_fields > 0
               GROUP BY ls.id, ls.n_entities, ls.n_fields, lsm.value, nval.value
               ORDER BY random();'''
        cur.execute(cmd)
        source_list = []
        for rec in cur.fetchall():
            sid, name, nval, nrow, nattr, nmapped, ratio = rec
            source_list.append({'id': sid, 'name': name, 'n_val': nval,
                                'n_attr': nattr, 'n_mapped': nmapped,
                                'n_entities': nrow})
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

    def source_fields(self, source_id):
        cur = self.conn.cursor()
        cmd = '''SELECT id, local_name
                   FROM local_fields
                  WHERE source_id = %s
                    AND (n_values > 0 OR 1 = 1)
               ORDER BY ROUND(sort_factor, 2) ASC, avg_val_len ASC;'''
        cur.execute(cmd, (source_id,))
        fields = []
        for rec in cur.fetchall():
            fid, name = rec
            fields.append({'id': fid, 'name': name})
        return fields

    def source_entities(self, source_id, n_entities):
        cur = self.conn.cursor()
        cmd = '''SELECT entity_id, field_id, value
                   FROM local_data
                  WHERE field_id IN (
                        SELECT id FROM local_fields
                         WHERE source_id = %s)
                    AND entity_id IN (
                        SELECT id
                          FROM (
                               SELECT id FROM local_entities
                                WHERE source_id = %s LIMIT 1000) t
                      ORDER BY random() LIMIT %s);'''
        cur.execute(cmd, (source_id, source_id, n_entities,))
        entities = dict()
        for rec in cur.fetchall():
            entity, field, value = rec
            entities.setdefault(entity, {'id': entity, 'fields': dict()})
            entities[entity]['fields'][field] = value
        return entities.values()

    def source_meta(self, source_id):
        cur = self.conn.cursor()
        cmd = '''SELECT meta_name, value FROM local_source_meta
                  WHERE source_id = %s
               ORDER BY meta_name DESC;'''
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

    def create_mappings(self, pairs, anti=False):
        if len(pairs) == 0:
            return
        cur = self.conn.cursor()
	params = []
	cmd = 'INSERT INTO attribute_mappings (' \
              '    local_id, global_id, confidence, authority, who_created, ' \
              '    when_created, why_created) ' \
              'VALUES '
        if anti: cmd = cmd.replace('mappings', 'antimappings')
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

    def new_attribute(self, ref_id, suggestion, username, comment):
        cur = self.conn.cursor()
        cmd = '''INSERT INTO new_attribute_suggestions (
                             reference_field_id, suggested_name, who_suggested,
                             when_suggested, why_suggested)
                      VALUES (%s, %s, %s, NOW(), %s);'''
        cur.execute(cmd, (ref_id, suggestion, username, comment,))
        self.conn.commit()
        return True

    def global_attributes(self):
        cur = self.conn.cursor()
        cmd = '''SELECT id, name FROM global_attributes;'''
        cur.execute(cmd)
        global_attrs = dict()
        for rec in cur.fetchall():
            global_attrs[rec[0]] = {'id': rec[0], 'name': rec[1]}
        return global_attrs

    def field_candidates(self, field_id):
        cur = self.conn.cursor()
        candidates = self.global_attributes()
        for id in candidates:
            candidates[id].setdefault('score', 0.0)
            candidates[id].setdefault('green', f2c(0.0))
            candidates[id].setdefault('red', f2c(0.0))
        cmd = '''SELECT match_id, score FROM nr_ncomp_results_tbl
                  WHERE field_id = %s;'''
        cur.execute(cmd, (field_id,))
        for rec in cur.fetchall():
            candidates[rec[0]]['score'] = rec[1]
            candidates[rec[0]]['green'] = f2c(rec[1] / 2.0)
            candidates[rec[0]]['red'] = f2c(1.0 - rec[1] / 3.0)
        return sorted(candidates.values(), key=itemgetter('score'), reverse=True)

    def field_mappings_by_source(self, source_id):
        cur = self.conn.cursor()
        fields = dict()
        cmd = '''SELECT ama.local_id, lf.local_name, ama.global_id, ga.name,
                        ama.who_created
                   FROM local_fields lf, attribute_mappings ama,
                        global_attributes ga
                  WHERE lf.id = ama.local_id
                    AND ama.global_id = ga.id
                    AND lf.source_id = %s
                    AND lf.n_values > 0;'''
        cur.execute(cmd, (source_id,))
        for rec in cur.fetchall():
            fields.setdefault(rec[0], dict())
            fields[rec[0]]['id'] = rec[0]
            fields[rec[0]]['name'] = rec[1]
            fields[rec[0]]['match'] = {
                'id': rec[2], 'name': rec[3], 'who_mapped': rec[4],
                'is_mapping': True, 'score': 2.0}
        cmd = '''SELECT lf.id, lf.local_name, nnr.match_id, ga.name, nnr.score
                   FROM nr_ncomp_results_tbl nnr, local_fields lf,
                        global_attributes ga
                  WHERE nnr.field_id = lf.id
                    AND nnr.source_id = %s
                    AND nnr.match_id = ga.id
                    AND lf.n_values > 0
               ORDER BY score desc;'''
        cur.execute(cmd, (source_id,))
        for rec in cur.fetchall():
            if rec[0] not in fields:
                fields[rec[0]] = {'id': rec[0], 'name': rec[1], 'match': {
                    'id': rec[2], 'name': rec[3], 'score': rec[4],
                    'green': f2c(rec[4] / 1.0), 'red':f2c(1.0 - rec[4] / 2.0)}}
        return fields

    def field_mappings_by_source2(self, source_id):
        cur = self.conn.cursor()
        fields = dict()
        cmd = '''SELECT lf.id, lf.local_name, ama.global_id, ga.name,
                        ama.who_created
                   FROM local_fields lf
              LEFT JOIN attribute_mappings ama
                     ON lf.id = ama.local_id
              LEFT JOIN global_attributes ga
                     ON ama.global_id = ga.id
                  WHERE lf.source_id = %s;'''
        cur.execute(cmd, (source_id,))
        for rec in cur.fetchall():
            fid, fname, gid, gname, who = rec
            fields.setdefault(fid, {'id': fid, 'name': fname})
            if gid is not None:
                fields[fid]['match'] = {
                    'id': gid, 'name': gname, 'who_mapped': who,
                    'is_mapping': True, 'score': 2.0}
        cmd = '''SELECT lf.id, lf.local_name, nnr.match_id, ga.name, nnr.score
                   FROM nr_ncomp_results_tbl nnr, local_fields lf,
                        global_attributes ga
                  WHERE nnr.field_id = lf.id
                    AND nnr.source_id = %s
                    AND nnr.match_id = ga.id
                    AND (lf.n_values > 0 OR 1 = 1)
               ORDER BY score desc;'''
        cur.execute(cmd, (source_id,))
        for rec in cur.fetchall():
            fid, fname, gid, gname, score = rec
            fields[fid].setdefault('match', {
                'id': gid, 'name': gname, 'score': score,
                'green': f2c(score / 1.0), 'red':f2c(1.0 - score / 2.0)})
        for fid in fields:
            if 'match' not in fields[fid]:
                fields[fid]['match'] = {'id': 0, 'name': 'Unknown', 'score': 0, 'green': f2c(0), 'red': f2c(1)}
        return fields



    def field_mappings_by_name(self, field_name, exact_match=False, n=100):
        cur = self.conn.cursor()
        fields = dict()
        pattern = field_name
        if not exact_match:
            pattern = '%' + pattern + '%'
        cmd = '''SELECT ama.local_id, lf.local_name, ama.global_id, ga.name,
                        ama.who_created
                   FROM local_fields lf, attribute_mappings ama,
                        global_attributes ga
                  WHERE lf.id = ama.local_id
                    AND ama.global_id = ga.id
                    AND lower(lf.local_name) LIKE %s
                  LIMIT %s;'''
        cur.execute(cmd, (pattern, n / 10,))
        for rec in cur.fetchall():
            fields.setdefault(rec[0], dict())
            fields[rec[0]]['id'] = rec[0]
            fields[rec[0]]['name'] = rec[1]
            fields[rec[0]]['match'] = {
                'id': rec[2], 'name': rec[3], 'who_mapped': rec[4],
                'is_mapping': True}
        cmd = '''SELECT lf.id, lf.local_name, nnr.match_id, ga.name, nnr.score
                   FROM nr_ncomp_results_tbl nnr, local_fields lf,
                        global_attributes ga
                  WHERE nnr.field_id = lf.id
                    AND lower(lf.local_name) LIKE %s
                    AND nnr.match_id = ga.id
               ORDER BY score desc
                  LIMIT %s;'''
        cur.execute(cmd, (pattern, n,))
        for rec in cur.fetchall():
            if rec[0] not in fields:
                fields[rec[0]] = {'id': rec[0], 'name': rec[1], 'match': {
                    'id': rec[2], 'name': rec[3], 'score': rec[4],
                    'green': f2c(rec[4] / 2.0), 'red':f2c(1.0 - rec[4] / 3.0)}}
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

    # get two entites to compare
    def get_entities_to_compare(self, approx_sim):
        cur = self.conn.cursor()
#        cmd = '''SELECT entity1_id, entity2_id, similarity
#                   FROM gbeskales.similar_entities
#                  WHERE similarity BETWEEN %s - 0.05 AND %s + 0.05
#               ORDER BY random()
#                  LIMIT 1;'''
#        cmd = '''SELECT entity_a, entity_b, similarity
#                   FROM entity_similarities
#                  WHERE similarity BETWEEN %s - 0.05 AND %s + 0.05
#               ORDER BY random()
#                  LIMIT 1;'''
        cmd = '''SELECT entity_a, entity_b, similarity
                   FROM public.entity_pair_queue 
                  WHERE human_label IS NULL 
               ORDER BY priority DESC 
                  LIMIT 1;'''		  
        cur.execute(cmd)
        rec = cur.fetchone()
        e1, e2, s = rec
        return (e1, e2, s)

    def entity_data(self, eid):
        cur = self.conn.cursor()
        cmd = '''SELECT tag_id, tag_id, lf.tag_code, ld.value
                   FROM public.doit_data ld
             INNER JOIN public.doit_fields lf
                     ON ld.source_id = lf.source_id AND ld.name = lf.name              
                  WHERE lf.tag_code is not null AND ld.entity_id = %s;'''

        cur.execute(cmd, (int(eid),))
        data = {}
        for rec in cur.fetchall():
            gid, fid, name, value = rec
            value = '' if None else value
            data[name] = value
#            data.append({'global_id': gid, 'local_id': fid,
#                         'name': name, 'value': value})
        return data

    def save_entity_comparison(self, e1id, e2id, answer):
        cur = self.conn.cursor()
#        cmd = '''UPDATE gbeskales.similar_entities SET human_label = %s
#                  WHERE entity1_id = %s AND entity2_id = %s;'''
        cmd = '''UPDATE public.entity_pair_queue SET human_label = %s
                  WHERE entity_a = %s AND entity_b = %s;'''
        cur.execute(cmd, (answer, e1id, e2id))
        self.conn.commit()
