from django.conf.urls.defaults import *
from django.views.generic.simple import redirect_to

urlpatterns = patterns('doit.views',
    (r'^(?P<dbname>\w+)/$', 'source_index'),

    (r'^(?P<dbname>\w+)/lowscorers/$', 'lowscoremapper'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/$', 'mapper'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/save$', 'mapper_results'),

    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/!(?P<attr_name>[^/]+)/$', redirect_to, {'url': 'summary',}),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/!(?P<attr_name>[^/]+)/summary$', 'detail_summary'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/!(?P<attr_name>[^/]+)/values/$', redirect_to, {'url': 'examples',}),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/!(?P<attr_name>[^/]+)/values/examples$', 'detail_examples'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/!(?P<attr_name>[^/]+)/values/shared$', 'detail_shared'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/!(?P<attr_name>[^/]+)/values/distro$', 'detail_distro'),
    (r'^(?P<dbname>\w+)/(?P<sid>\d+)/!(?P<attr_name>[^/]+)/scores$', 'detail_scoring'),
)
