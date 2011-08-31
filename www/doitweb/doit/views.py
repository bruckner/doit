import math

from doit.util import bucketize
from doit.dataaccess import DoitDB
from operator import itemgetter
from django.shortcuts import render_to_response
from django.http import HttpResponse

def source_index(req, dbname):
	db = DoitDB(dbname)
	return render_to_response('doit/source_index.html', {'source_list': db.sources(),})


def mapper(req, sid, dbname):
	db = DoitDB(dbname)
	matchscores = db.matches(sid)
	egs = db.examplevalues(sid)

	attr_list = []
	for name in matchscores:
		egs.setdefault(name, None)
		cand = sorted(matchscores[name], key=itemgetter(2), reverse=True)
		attr_list.append({'name': name, 'candidates': cand, 'example': (egs[name])})

	return render_to_response('doit/mapper.html', {'attr_list': attr_list, 'source_id': sid})

def mapper_results(req, sid, dbname):
	db = DoitDB(dbname)

	s = ''
	for local_att, global_att in req.POST.items():
		s = s + local_att + '/' + global_att + '\n'

	return HttpResponse(s)


def lowscoremapper(req, dbname):
	db = DoitDB(dbname)
	matchscores = db.lowscorers(25)

	attr_list = []
	for name in matchscores:
		cand = sorted(matchscores[name], key=itemgetter(2), reverse=True)
		attr_list.append({'name': name, 'candidates': cand})

	return render_to_response('doit/mapper.html', {'attr_list': attr_list})


def detail_summary(req, sid, dbname, attr_name):
	db = DoitDB(dbname)

	vals = db.fieldexamples(sid, attr_name, 1000, distinct=False)
	histo = bucketize(vals)

	#meta = db.fieldmeta(sid, attr_name)

	return render_to_response('doit/pop_summary.html', {'histo': histo, 'attr_name': attr_name, 'source': sid})


def detail_examples(req, sid, dbname, attr_name):
	db = DoitDB(dbname)

	egs = [{'name': attr_name, 'values': db.fieldexamples(sid, attr_name, 10)}]
	matches = db.matches(sid)[attr_name][:5]

	for match in matches:
		egs.append({'name': match[0], 'values': db.globalfieldexamples(match[1], 10)})

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

	return render_to_response('doit/pop_egs.html', {'examples': transpose, 'attr_name': attr_name,})


def detail_shared(req, sid, dbname, attr_name):
	db = DoitDB(dbname)

	shared = []
	matches = db.matches(sid)[attr_name]
	for match in matches:
		shared.append({'name': match[0], 'values': db.sharedvalues(sid, attr_name, match[1])})

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

	return render_to_response('doit/pop_shared.html', {'shared': table, 'attr_name': attr_name,})


def detail_distro(req,  dbname, sid, attr_name):
	db = DoitDB(dbname)
	vals = db.fieldexamples(sid, attr_name, 1000, distinct=False)

	histos = [bucketize(vals)]
	histos[0]['name'] = attr_name

	matches = sorted(db.matches(sid)[attr_name], key=lambda match: match[2], reverse=True)[:4]

	for match in matches:
		histo = bucketize(db.globalfieldexamples(match[1], n=1000, distinct=False))
		histo['name'] = match[0]
		histos.append(histo)

	return render_to_response('doit/pop_distro.html', {'histos': histos, 'attr_name': attr_name,})


def detail_scoring(req, dbname, sid, attr_name):
	db = DoitDB(dbname)
	matches = db.indivscores(sid, attr_name)
	return render_to_response('doit/pop_scores.html', {'matches': matches, 'attr_name': attr_name,})



