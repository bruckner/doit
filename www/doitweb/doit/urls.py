from django.conf.urls.defaults import *
from django.views.generic.simple import redirect_to

urlpatterns = patterns('doit.views',
    (r'^(?P<dbname>\w+)/$', 'source_index'),

    (r'^(?P<dbname>\w+)/lowscorers/$', 'lowscoremapper'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/$', 'mapper'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/save$', 'mapper_results'),

    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/(?P<fid>[^/]+)/$', redirect_to, {'url': 'summary',}),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/(?P<fid>[^/]+)/summary$', 'detail_summary'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/(?P<fid>[^/]+)/values/$', redirect_to, {'url': 'examples',}),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/(?P<fid>[^/]+)/values/examples$', 'detail_examples'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/(?P<fid>[^/]+)/values/shared$', 'detail_shared'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/(?P<fid>[^/]+)/values/distro$', 'detail_distro'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/(?P<fid>[^/]+)/scores$', 'detail_scoring'),

    (r'^(?P<dbname>\w+)/process/(?P<sid>\d+)/(?P<method_index>[0-4])/', 'source_processor'),
)
