from django.conf.urls.defaults import *
from django.views.generic.simple import redirect_to

urlpatterns = patterns('doit.views',
    (r'^templates/viewTable', 'viewTable_template'),
    (r'^(?P<dbname>\w+)/$', 'source_index'),

    (r'^(?P<dbname>\w+)/lowscorers/$', 'lowscoremapper'),
    (r'^(?P<dbname>\w+)/sources/(?P<sid>\d+)/map$', 'mapper'),
    (r'^(?P<dbname>\w+)/sources/(?P<sid>\d+)/data', 'source_data'),
    (r'^(?P<dbname>\w+)/sources/(?P<sid>\d+)/table', 'source_table'),
    (r'^(?P<dbname>\w+)/sources/(?P<sid>\d+)/entities', 'source_entities'),
    (r'^(?P<dbname>\w+)/sources/(?P<sid>\d+)/process/(?P<method_index>[0-4])/', 'source_processor'),
    (r'^(?P<dbname>\w+)/fields/(?P<comp_op>(like|named))/(?P<field_name>\w+)/map$', 'mapper_by_field_name'),
    (r'^(?P<dbname>\w+)/save$', 'mapper_results'),
    (r'^(?P<dbname>\w+)/suggest-new-attribute/$', 'suggest_new_attribute'),
    (r'^(?P<dbname>\w+)/suggest-new-attribute/form', 'suggest_new_attribute_form'),

    (r'^(?P<dbname>\w+)/fields/(?P<fid>\d+)/$', redirect_to, {'url': 'summary',}),
    (r'^(?P<dbname>\w+)/fields/(?P<fid>\d+)/candidates/$', 'field_candidates'),
    (r'^(?P<dbname>\w+)/fields/(?P<fid>\d+)/summary$', 'detail_summary'),
    (r'^(?P<dbname>\w+)/fields/(?P<fid>\d+)/values/$', redirect_to, {'url': 'examples',}),
    (r'^(?P<dbname>\w+)/fields/(?P<fid>\d+)/values/examples$', 'detail_examples'),
    (r'^(?P<dbname>\w+)/fields/(?P<fid>\d+)/values/shared$', 'detail_shared'),
    (r'^(?P<dbname>\w+)/fields/(?P<fid>\d+)/values/distro$', 'detail_distro'),
    (r'^(?P<dbname>\w+)/fields/(?P<fid>\d+)/scores$', 'detail_scoring'),
)
