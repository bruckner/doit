import psycopg2
import copy
import cPickle
import re
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


class TamerDB:
    conn = None
    name = None

    def __init__(self, dbname):
        if self.conn is None:
            self.conn = psycopg2.connect(database=dbname, user='django',
                                         password='django', host='localhost')
        name = dbname

    def source_list(self, n):
        cur = self.conn.cursor()
        cmd = '''SELECT id, local_id FROM local_sources LIMIT %s'''
        cur.execute(cmd, (n,))
        sl = []
        for r in cur.fetchall():
            sl.append({'id': r[0], 'name': r[1]})
        return sl

    def recent_sources(self, n):
        cur = self.conn.cursor()
        cmd = '''SELECT COUNT(*), date_added,
                        row_number() OVER (ORDER BY date_added) rank
                   FROM local_sources
               GROUP BY date_added
                  LIMIT %s;'''
        cur.execute(cmd, (n,))
        return [{'date': r[1], 'count': r[0], 'rank': r[2]} for r in cur.fetchall()]

    def schema_tables(self, schemaname):
        cur = self.conn.cursor()
        cmd = '''SELECT tablename FROM pg_tables
                  WHERE schemaname = %s ORDER BY tablename;'''
        cur.execute(cmd, (schemaname,))
        t = []
        for r in cur.fetchall():
            t.append(r[0])
        return t

    def table_attributes(self, tablename):
        cur = self.conn.cursor()
        cmd = '''SELECT attname FROM pg_attribute, pg_type
                  WHERE typname = %s
                    AND attrelid = typrelid
                    AND attname NOT IN ('cmin', 'cmax', 'ctid', 'oid', 'tableoid', 'xmin', 'xmax');'''
        cur.execute(cmd, (tablename,))
        a = []
        for r in cur.fetchall():
            a.append(r[0])
        return a

    def global_attributes(self):
        cur = self.conn.cursor()
        cmd = '''SELECT id, name FROM global_attributes;'''
        cur.execute(cmd)
        return [{'id': r[0], 'name': r[1]} for r in cur.fetchall()]

    def global_attribute_names(self):
        cur = self.conn.cursor()
        cmd = '''SELECT name FROM global_attributes;'''
        cur.execute(cmd)
        return [r[0] for r in cur.fetchall()]

    def source_name(self, sid):
        cur = self.conn.cursor()
        cmd = '''SELECT local_id FROM local_sources WHERE id = %s;'''
        cur.execute(cmd, (sid,))
        return cur.fetchone()[0]

    def source_stats(self, sid):
        cur = self.conn.cursor()
        stats = {}
        cmd = '''SELECT COUNT(*) FROM local_entities WHERE source_id = %s;'''
        cur.execute(cmd, (sid,))
        stats['nent'] = cur.fetchone()[0]

        cmd = '''SELECT COUNT(*), COUNT(a.local_id)
                   FROM local_fields f
              LEFT JOIN attribute_mappings a
                     ON f.id = a.local_id
                  WHERE source_id = %s;'''
        cur.execute(cmd, (sid,))
        r = cur.fetchone()
        stats['ncol'] = r[0]
        stats['nmap'] = r[1]

        cmd = '''SELECT COUNT(*) FROM entity_matches
                  WHERE entity_a IN (SELECT id FROM local_entities WHERE source_id = %s);'''
        cur.execute(cmd, (sid,))
        stats['ndup'] = cur.fetchone()[0]
        return stats

    def config_params(self, model_name):
        cur = self.conn.cursor()
        cmd ='''SELECT name, COALESCE(description, name), value FROM configuration_properties
                 WHERE module = %s;'''
        cur.execute(cmd, (model_name,))
        return [{'name': r[0], 'description': r[1], 'value': r[2]} for r in cur.fetchall()]

    def set_config(self, param_name, param_value):
        cur = self.conn.cursor()
        cmd = '''UPDATE configuration_properties SET value = %s
                  WHERE name = %s;'''
        cur.execute(cmd, (param_value, param_name,))
        self.conn.commit()
        return cmd % (param_name, param_value)

    def dedup_model_exists(self):
        cur = self.conn.cursor()
        #cmd = '''SELECT COUNT(*) FROM learning_attrs;'''
        cmd = '''SELECT COUNT(weight), COUNT(*) FROM entity_field_weights;'''
        cur.execute(cmd)
        r = cur.fetchone()
        return (int(r[0]) == int(r[1]) and int(r[0]) > 0)
        


    ##
    # Major jobs
    ##
    def import_from_pg_table(self, schemaname, tablename, eidattr, sidattr, dataattr):
        cur = self.conn.cursor()

        # Make a copy for importing
        eidconst = 'row_number() over ()' if eidattr is None else eidattr
        sidconst = "'0'" if sidattr is None else sidattr
        cmd = '''CREATE TEMP TABLE import_tmp AS
                      SELECT %s::TEXT AS sid, %s::TEXT AS eid, %s::TEXT FROM %s.%s''' \
            % (sidconst, eidconst, '::TEXT,'.join(dataattr), schemaname, tablename)
        cur.execute(cmd)

        # Add new source(s)
        cmd = '''INSERT INTO local_sources (local_id, date_added)
                      SELECT DISTINCT %s || '/' || sid, NOW() FROM import_tmp;'''
        cur.execute(cmd, (tablename,))

        # Get new source_id(s)
        cmd = '''UPDATE import_tmp i SET sid = s.id FROM local_sources s
                  WHERE s.local_id = %s || '/' || i.sid;'''
        cur.execute(cmd, (tablename,))

        # Add data columns to local_fields
        cmd = '''INSERT INTO local_fields (source_id, local_name)
                      SELECT sid::INTEGER, %s FROM import_tmp GROUP BY sid;'''
        for a in dataattr:
            cur.execute(cmd, (a,))

        # Add entities to local_entities
        cmd = '''INSERT INTO local_entities (source_id, local_id)
                      SELECT sid::INTEGER, eid
                        FROM import_tmp
                    GROUP BY sid, eid;
                 UPDATE import_tmp i SET eid = e.id FROM local_entities e
                  WHERE i.eid = e.local_id;'''
        cur.execute(cmd)

        # Add data to local_data
        for a in dataattr:
            cmd = '''INSERT INTO local_data (field_id, entity_id, value)
                          SELECT f.id, eid::INTEGER, i.%s
                            FROM import_tmp i, local_fields f
                           WHERE i.sid::INTEGER = f.source_id
                             AND f.local_name = %%s
                             AND i.%s IS NOT NULL
                             AND length(i.%s) > 0;''' \
                % (a, a, a)
            cur.execute(cmd, (a,))

        # Preprocess source(s) for map and dedup
        cmd = '''SELECT DISTINCT sid::INTEGER FROM import_tmp;'''
        cur.execute(cmd)
        for r in cur.fetchall():
            self.preprocess_source(r[0])

        self.conn.commit()

    def import_attribute_dictionary(self, att_id, schemaname, tablename, columnname):
        cur = self.conn.cursor()
        cmd = '''INSERT INTO global_data (att_id, value)
                      SELECT %%s, %s::TEXT FROM %s.%s;''' \
            % (columnname, schemaname, tablename)
        cur.execute(cmd, (att_id,))
        self.conn.commit()

    def import_synonym_dictionary(self, att_id, schemaname, tablename, columna, columnb):
        cur = self.conn.cursor()
        cmd = '''INSERT INTO global_synonyms (att_id, value_a, value_b)
                      SELECT %%s, %s::TEXT, %s::TEXT FROM %s.%s;''' \
            % (columna, columnb, schemaname, tablename)
        cur.execute(cmd, (att_id,))
        self.conn.commit()

    def import_attribute_template(self, templatename, schemaname, tablename, columnname):
        cur = self.conn.cursor()
        cmd = '''INSERT INTO templates (name) SELECT %s;'''
        cur.execute(cmd, (templatename,))
        cmd = '''SELECT id FROM templates WHERE name = %s;;'''
        cur.execute(cmd, (templatename,))
        tid = cur.fetchone()[0]
        cmd = '''INSERT INTO attribute_templates (template_id, att_id)
                      SELECT %%s, g.id
                        FROM %s.%s t, global_attributes g
                       WHERE lower(t.%s::TEXT) = lower(g.name);''' \
            % (schemaname, tablename, columnname)
        cur.execute(cmd, (tid,))
        self.conn.commit()

    def import_global_schema(self, schemaname, tablename, columnname):
        cur = self.conn.cursor()
        cmd = '''INSERT INTO global_attributes (name, derived_from)
                      SELECT %s, 'WEB' FROM %s.%s;''' \
            % (columnname, schemaname, tablename)
        cur.execute(cmd)
        self.conn.commit()

    def preprocess_source(self, sid):
        cur = self.conn.cursor()
        cmd = '''SELECT preprocess_source(%s);
                 --SELECT extract_new_data(%s, true);
                 TRUNCATE entity_test_group;
                 INSERT INTO entity_test_group
                      SELECT id FROM local_entities WHERE source_id = %s;
                 SELECT entities_preprocess_test_group('t');'''
        cur.execute(cmd, (sid, sid,sid,))
        self.conn.commit()

    def init_dedup(self, important, irrelevant):
        cur = self.conn.cursor()
        #cmd = '''INSERT INTO learning_attrs (tag_id)
        #              SELECT id
        #                FROM global_attributes
        #               WHERE name = %s;'''
        cmd = '''TRUNCATE entity_field_weights;
                 INSERT INTO entity_field_weights
                      SELECT id, 1.0 FROM global_attributes;'''
        cur.execute(cmd)
        cmd = '''UPDATE entity_field_weights SET initial_bias = %s
                  WHERE field_id IN (SELECT id FROM global_attributes WHERE name = %s);'''
        for attr in important:
            cur.execute(cmd, (10.0, attr))
        for attr in irrelevant:
            cur.execute(cmd, (0.1, attr))
        cmd = '''UPDATE entity_field_weights SET weight = initial_bias;'''
        cur.execute(cmd)
        self.conn.commit()

    def rebuild_dedup_models(self):
        cur = self.conn.cursor()
        cmd = '''--SELECT learn_weights(0.05, 0.00001, 5, 1000, 0.2);
                 TRUNCATE entity_test_group;
                 INSERT INTO entity_test_group SELECT id FROM local_entities;
                 SELECT entities_preprocess_test_group('t');
                 SELECT entities_weights_from_test_group();'''
        cur.execute(cmd)
        self.conn.commit()

    def rebuild_schema_mapping_models(self):
        cur = self.conn.cursor()
        cmd = '''SELECT preprocess_global();'''
        cur.execute(cmd)
        self.conn.commit()

    def schema_map_source(self, sourceid):
        cur = self.conn.cursor()
        cmd = '''SELECT qgrams_results_for_source(%s);
                 SELECT ngrams_results_for_source(%s);
                 SELECT mdl_results_for_source(%s);
                 SELECT nr_composite_load();'''
        cur.execute(cmd, (sourceid, sourceid, sourceid))
        self.conn.commit()

    def dedup_source(self, sid):
        cur = self.conn.cursor()
        self.rebuild_dedup_models()
        cmd = '''TRUNCATE entity_test_group;
                 INSERT INTO entity_test_group
                      SELECT id FROM local_entities WHERE source_id = %s;
                 SELECT entities_preprocess_test_group('t');
                 SELECT entities_field_similarities_for_test_group();
                 SELECT entities_results_for_test_group('f');'''
        #cmd = '''SELECT self_join(0.00001);
        #         --SELECT cluster(0.95);
        #         --SELECT two_way_join(0.00001);
        #         --SELECT incr_cluster(0.95);'''
        cur.execute(cmd, (sid,))
        self.conn.commit()


    def dedup_all(self):
        cur = self.conn.cursor()
        self.rebuild_dedup_models()
        cmd = '''TRUNCATE entity_test_group;
                 INSERT INTO entity_test_group
                      SELECT id FROM local_entities;
                 SELECT entities_preprocess_test_group('t');
                 SELECT entities_field_similarities_for_test_group();
                 SELECT entities_results_for_test_group('f');'''
        cur.execute(cmd)
        self.conn.commit()

    # get two entites to compare
    def get_entities_to_compare(self, approx_sim, sort):
        cur = self.conn.cursor()
        cmd = '''SELECT entity_a, entity_b, similarity
                   FROM entity_similarities
               ORDER BY random()
                  LIMIT 1;'''
        if sort == 'high':
            cmd = '''SELECT entity_a, entity_b, similarity FROM entity_similarities
                      WHERE human_label IS NULL ORDER BY similarity DESC;'''
        if approx_sim is not None:
            cmd = '''SELECT entity_a, entity_b, similarity
                       FROM entity_similarities
                      WHERE similarity BETWEEN %s - 0.05 AND %s + 0.05
                   ORDER BY random()
                      LIMIT 1;'''
            cur.execute(cmd, (approx_sim, approx_sim))
        else:
            cur.execute(cmd)
        rec = cur.fetchone()
        e1, e2, s = rec
        return (e1, e2, s)

    def entity_data(self, eid):
        cur = self.conn.cursor()
        cmd = '''SELECT g.id, lf.id, COALESCE(g.name, lf.local_name), ld.value
                   FROM local_data ld
             INNER JOIN local_fields lf
                     ON ld.field_id = lf.id
              LEFT JOIN (SELECT ga.id, ga.name, am.local_id
                   FROM global_attributes ga, attribute_mappings am
                  WHERE ga.id = am.global_id) g
                     ON lf.id = g.local_id
                  WHERE ld.entity_id = %s;'''
        cur.execute(cmd, (int(eid),))
        data = {}
        for rec in cur.fetchall():
            gid, fid, name, value = rec
            value = '' if None else value
            data[name] = value
        # data.append({'global_id': gid, 'local_id': fid,
        # 'name': name, 'value': value})
        return data

    def save_entity_comparison(self, e1id, e2id, answer):
        cur = self.conn.cursor()
        cmd = '''UPDATE entity_similarities SET human_label = %s
                  WHERE entity_a = %s AND entity_b = %s;'''
        cur.execute(cmd, (answer, e1id, e2id))
        if answer == 'YES':
            cmd = '''INSERT INTO entity_matches SELECT %s, %s;'''
            cur.execute(cmd, (e1id, e2id))
        self.conn.commit()

