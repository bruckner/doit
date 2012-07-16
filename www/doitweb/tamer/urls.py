from django.conf.urls.defaults import *
from django.views.generic.simple import redirect_to

urlpatterns = patterns('tamer.views',
    (r'^(?P<dbname>\w+)/$', 'main_console'),

    (r'^(?P<dbname>\w+)/import-console-(?P<import_object>\w+)', 'import_console'),

    (r'^(?P<dbname>\w+)/widgets/attribute-selector', 'widget_attr_labeller'),
    (r'^(?P<dbname>\w+)/widgets/attribute-radio', 'widget_attr_radio'),

    (r'^(?P<dbname>\w+)/import-table', 'run_import'),
    (r'^(?P<dbname>\w+)/import-auxiliary', 'import_auxiliary'),

    (r'^(?P<dbname>\w+)/source-(?P<sid>\d+)-map', 'schema_map_source'),
    (r'^(?P<dbname>\w+)/source-(?P<sid>\d+)-dedup', 'dedup_source'),
    (r'^(?P<dbname>\w+)/source-(?P<sid>\d+)', 'source_console'),
    (r'^(?P<dbname>\w+)/source-(?P<sid>\d+)-(?P<tab>\w+)', 'source_console'),

    (r'^(?P<dbname>\w+)/configure-(?P<model_name>\w+)/set', 'set_config'),
    (r'^(?P<dbname>\w+)/configure-(?P<model_name>\w+)', 'config_console'),

    (r'^(?P<dbname>\w+)/initialize-dedup$', 'init_dedup_console'),
    (r'^(?P<dbname>\w+)/initialize-dedup-submit', 'init_dedup_submit'),
    (r'^(?P<dbname>\w+)/train-dedup$', 'train_dedup'),
    (r'^(?P<dbname>\w+)/evaluate-dedup$', 'compare_entities'),


    (r'^(?P<dbname>\w+)/.*', 'main_console'),
)
