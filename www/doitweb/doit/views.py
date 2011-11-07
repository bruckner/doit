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
	meta = db.source_meta(sid)
	field_mappings = db.field_mappings(sid)
	egs = db.examplevalues(sid)
	attr_list = []
	for fid in field_mappings:
	    egs.setdefault(int(fid), None)
	    cand = sorted(field_mappings[fid]['matches'],
                          key=itemgetter('score'), reverse=True)
            field_info = {'name': field_mappings[fid]['name'], 'fid': fid,
                          'candidates': cand, 'example': egs[int(fid)]}
            if field_mappings[fid]['mapping'] is not None:
                field_info['mapping'] = field_mappings[fid]['mapping']
            attr_list.append(field_info)
	return render_to_response('doit/mapper.html', {
            'attr_list': attr_list, 'source_id': sid, 'metadata': meta,})

# Handles new mapping POST
def mapper_results(req, sid, dbname):
	db = DoitDB(dbname)
	s = db.create_mappings(req.POST.items())

	return HttpResponse(s)


# currently broken...
def lowscoremapper(req, dbname):
	db = DoitDB(dbname)
	matchscores = db.lowscorers(25)

	attr_list = []
	for name in matchscores:
		cand = sorted(matchscores[name], key=itemgetter(2), reverse=True)
		attr_list.append({'name': name, 'candidates': cand})

	return render_to_response('doit/mapper.html', {'attr_list': attr_list})


def detail_summary(req, sid, dbname, fid):
	db = DoitDB(dbname)
	attr_name = db.fieldname(fid)
	meta = db.field_meta(fid)
	vals = db.fieldexamples(fid, 1000, distinct=False)
	histo = bucketize(vals)
	return render_to_response('doit/pop_summary.html', {'histo': histo, 'attr_name': attr_name, 'source': sid, 'fid': fid, 'metadata': meta,})


def detail_examples(req, sid, dbname, fid):
	db = DoitDB(dbname)
	attr_name = db.fieldname(fid)
	egs = [{'name': attr_name, 'values': db.fieldexamples(fid, 10)}]
	matches = db.field_mappings(sid)[int(fid)]['matches'][:5]
	for match in matches:
		egs.append({'name': match['name'], 'values': db.globalfieldexamples(match['id'], 10)})

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

	return render_to_response('doit/pop_egs.html', {'examples': transpose, 'attr_name': attr_name, 'fid': fid,})


def detail_shared(req, sid, dbname, fid):
	db = DoitDB(dbname)
	attr_name = db.fieldname(fid)
	shared = []
	matches = db.field_mappings(sid)[int(fid)]['matches']
	for match in matches:
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
	return render_to_response('doit/pop_shared.html', {'shared': table, 'attr_name': attr_name, 'fid': fid,})


def detail_distro(req,  dbname, sid, fid):
	db = DoitDB(dbname)
	attr_name = db.fieldname(fid)
	vals = db.fieldexamples(fid, 1000, distinct=False)

	histos = [bucketize(vals)]
	histos[0]['name'] = attr_name

	matches = sorted(db.field_mappings(sid)[int(fid)]['matches'], key=itemgetter('score'), reverse=True)[:4]

	for match in matches:
		histo = bucketize(db.globalfieldexamples(int(match['id']), n=1000, distinct=False))
		histo['name'] = match['name']
		histos.append(histo)

	return render_to_response('doit/pop_distro.html', {'histos': histos, 'attr_name': attr_name, 'fid': fid,})


def detail_scoring(req, dbname, sid, fid):
	db = DoitDB(dbname)
	attr_name = db.fieldname(fid)

	matches = db.indivscores(fid)
	return render_to_response('doit/pop_scores.html', {'matches': matches, 'attr_name': attr_name, 'fid': fid,})



