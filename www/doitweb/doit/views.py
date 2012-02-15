import math
from doit.util import bucketize
from doit.dataaccess import DoitDB
from operator import itemgetter, attrgetter
from django.shortcuts import render_to_response
from django.http import HttpResponse
from django.utils import simplejson

def source_index(req, dbname):
	db = DoitDB(dbname)
	return render_to_response('doit/source_index.html', {
            'source_list': db.sources(), 'dbname': dbname,})

def source_processor(req, dbname, sid, method_index):
	db = DoitDB(dbname)
	method_name = db.process_source(sid, method_index)
	r = {'method': method_name, 'source': sid,
             'redirect': '/doit/' + dbname + '/' + sid + '/'}
	return HttpResponse(simplejson.dumps(r), mimetype='application/json')

def mapper(req, sid, dbname):
    db = DoitDB(dbname)
    meta = dict()
    meta['data'] = db.source_meta(sid)
    meta['category'] = 'Source %s' % sid
    field_mappings = db.field_mappings_by_source(sid)
    egs = db.examplevalues(sid)
    for fid in field_mappings:
        egs.setdefault(int(fid), None)
        field_mappings[fid]['example'] = egs[int(fid)]
    attr_list = sorted(field_mappings.values(), key=lambda f: f['match']['score'])
    return render_to_response('doit/mapper.html', {
        'attr_list': attr_list, 'source_id': sid, 
        'meta': meta,})

def mapper_by_field_name(req, dbname, field_name, comp_op):
    db = DoitDB(dbname)
    meta = {'category': 'Fields named "%s"' % field_name}
    field_mappings = db.field_mappings_by_name(field_name,
                                               exact_match=(comp_op != 'like'))
#    egs = db.examplevalues(sid)
#    for fid in field_mappings:
#        egs.setdefault(int(fid), None)
#        field_mappings[fid]['example'] = egs[int(fid)]
    return render_to_response('doit/mapper.html', {
        'attr_list': field_mappings.values(), 'field_name': field_name,
        'meta': meta,})

def viewTable_template(req):
    return render_to_response('doit/viewTable_template.html')

def source_data(req, dbname, sid):
    db = DoitDB(dbname)
    data = {'fields': db.source_fields(sid),
            'entities': db.source_entities(sid, 10)}
    return HttpResponse(simplejson.dumps(data),
                        mimetype='application/json')

def source_table(req, dbname, sid):
    db = DoitDB(dbname)
    fields = db.source_fields(sid)
    entities = db.source_entities(sid, 10)
    for entity in entities:
        vals = []
        for field in fields:
            try:
                vals.append(entity['fields'][field['id']])
            except KeyError:
                vals.append('')
            if vals[-1] is None: vals[-1] = ''
        entity['fields'] = vals
    return render_to_response('doit/viewTable_template.html', {
                'fields': fields, 'entities': entities})

def source_entities(req, dbname, sid):
    db = DoitDB(dbname)
    data = {'entities': db.source_entities(sid, 10)}
    return HttpResponse(simplejson.dumps(data),
                        mimetype='application/json')

def field_candidates(req, fid, dbname):
    db = DoitDB(dbname)
    return render_to_response('doit/candidate_list.html', {
        'fid': fid, 'candidates': db.field_candidates(fid)})

# Handles new mapping POST
def mapper_results(req, dbname):
    db = DoitDB(dbname)
    mappings = simplejson.loads(req.POST['mappings'])
    rejects = simplejson.loads(req.POST['rejects'])
    s = db.create_mappings(mappings)
    t = db.create_mappings(rejects, anti=True)
    return HttpResponse(s)

def suggest_new_attribute_form(req, dbname):
    return render_to_response('doit/suggest_new_attribute_form.html', {
        'fid': req.GET['fid'], 'fname': req.GET['fname'], 'dbname': dbname})

def suggest_new_attribute(req, dbname):
    db = DoitDB(dbname)
    field_id = req.POST['fid']
    suggestion = req.POST['suggestion']
    username = req.POST['user']
    comment = req.POST['comment']
    success = db.new_attribute(field_id, suggestion, username, comment)
    return HttpResponse(simplejson.dumps({'success': success}),
                        mimetype='application/json')

# currently broken...
def lowscoremapper(req, dbname):
	db = DoitDB(dbname)
	matchscores = db.lowscorers(25)
	attr_list = []
	for name in matchscores:
		cand = sorted(matchscores[name],
                              key=itemgetter(2), reverse=True)
		attr_list.append({'name': name, 'candidates': cand})
	return render_to_response('doit/mapper.html', {'attr_list': attr_list})

def detail_summary(req, dbname, fid):
	db = DoitDB(dbname)
	attr_name = db.fieldname(fid)
	meta = db.field_meta(fid)
	vals = db.fieldexamples(fid, 1000, distinct=False)
	histo = bucketize(vals)
	return render_to_response('doit/pop_summary.html', {
            'histo': histo, 'attr_name': attr_name, 'source': '', 'fid': fid,
            'metadata': meta, 'db': dbname,})

def detail_examples(req, dbname, fid):
	db = DoitDB(dbname)
	attr_name = db.fieldname(fid)
	egs = [{'name': attr_name, 'values': db.fieldexamples(fid, 10)}]
	matches = db.field_candidates(fid)[:5]
	for match in matches:
		egs.append({'name': match['name'], 
                            'values': db.globalfieldexamples(match['id'], 10)})

	transpose = [[]]
	for eg in egs:
		transpose[0].append(eg['name'])
	for i in range(0, 10):
		transpose.append([])
		for eg in egs:
			try:
				transpose[i+1].append(eg['values'][i])
			except IndexError:
				transpose[i+1].append(' ')

	return render_to_response('doit/pop_egs.html', {
            'examples': transpose, 'attr_name': attr_name, 'fid': fid, 
            'db': dbname})

def detail_shared(req, dbname, fid):
	db = DoitDB(dbname)
	attr_name = db.fieldname(fid)
	shared = []
	matches = db.field_candidates(fid)
	for match in matches[:4]:
	    shared.append({'name': match['name'],
                           'values': db.sharedvalues(fid, match['id'])})
	table = [[]]
	for match in shared:
	    table[0].append(match['name'])
	for i in range(0, 10):
	    table.append([])
	    for match in shared:
	        try:
		    table[i+1].append(match['values'][i])
		except IndexError:
		    table[i+1].append(' ')
	return render_to_response('doit/pop_shared.html', {
            'shared': table, 'attr_name': attr_name, 'fid': fid,
            'db': dbname,})

def detail_distro(req,  dbname, fid):
	db = DoitDB(dbname)
	attr_name = db.fieldname(fid)
	vals = db.fieldexamples(fid, 1000, distinct=False)
	histos = [bucketize(vals)]
	histos[0]['name'] = attr_name
	matches = db.field_candidates(fid)[:4]
	for match in matches:
		histo = bucketize(db.globalfieldexamples(int(match['id']), n=1000, distinct=False))
		histo['name'] = match['name']
		histos.append(histo)
	return render_to_response('doit/pop_distro.html', {
            'histos': histos, 'attr_name': attr_name, 'fid': fid,
            'db': dbname,})

def detail_scoring(req, dbname, fid):
	db = DoitDB(dbname)
	attr_name = db.fieldname(fid)
	matches = db.indivscores(fid)
	return render_to_response('doit/pop_scores.html', {
            'matches': matches, 'attr_name': attr_name, 'fid': fid,
            'db': dbname,})



