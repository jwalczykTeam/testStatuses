'''
Created on Sep 1, 2016

@author: michele
'''

from doi.util import log_util
import re, json, os
from abc import ABCMeta, abstractmethod


logger = log_util.get_logger("astro.util.kibana_util")


class Processor(object):
    __metaclass__ = ABCMeta

    @staticmethod
    def __beautify(name):
        return re.sub("(^| )([a-z])", lambda match: match.group(1) + match.group(2).upper(), re.sub("[_\-]+", ' ', name))

    def __init__(self, es_client, index_name=None, index_title=None):
        self.es_client = es_client
        self.index_name = index_name
        self.index_title = index_title

        if index_name and not index_title:
            self.index_title = Processor.__beautify(index_name)

    def add_objects(self, kobjs):
        for kobj in kobjs:
            self._add_object(kobj)

    def remove_objects(self, kobjs):
        for kobj in kobjs:
            self._remove_object(kobj)

    @abstractmethod
    def _add_object(self, obj):
        pass

    @abstractmethod
    def _remove_object(self, obj):
        pass


class KibanaProcessor(Processor):
    __METADATA_INDEX = '.kibana'

    def __init__(self, es_client, index_name=None, index_title=None):
        super(KibanaProcessor, self).__init__(es_client, index_name, index_title)

    def _add_object(self, obj):
        k_id = obj['_id']
        source = obj['_source']

        # Remove un-needed attributes
        source.pop('hits', None)
        source.pop('version', None)

        source = json.dumps(source)
        if self.index_name:
            k_id = re.sub("__index__", self.index_name, k_id)
            source = re.sub("__title__", self.index_title, re.sub("__index__", self.index_name, source))

        self.es_client.insert(self.__METADATA_INDEX, obj['_type'], k_id, source)
        logger.info('Added %s %s to Kibana for index %s', obj['_type'], k_id, self.index_name)

    def _remove_object(self, obj):
        k_id = obj['_id']
        if self.index_name:
            k_id = re.sub("__index__", self.index_name, k_id)

        self.es_client.delete(self.__METADATA_INDEX, obj['_type'], k_id)
        logger.info('Removed %s %s from Kibana for index %s', obj['_type'], k_id, self.index_name)


class DRAProcessor(Processor):
    __METADATA_INDEX = '.devriskanalysis'

    def __init__(self, es_client, index_name=None, index_title=None):
        super(DRAProcessor, self).__init__(es_client, index_name, index_title)

    def _add_object(self, dobj):
        d_id = dobj['_id']
        source = dobj['_source']

        source = json.dumps(source)
        if self.index_name:
            d_id = re.sub("__index__", self.index_name, d_id)
            source = re.sub("__title__", self.index_title, re.sub("__index__", self.index_name, source))

        self.es_client.insert(self.__METADATA_INDEX, dobj['_type'], d_id, source)
        logger.info('Added %s %s to DevRisk Analysis plugin for index %s', dobj['_type'], d_id, self.index_name)

    def _remove_object(self, dobj):
        d_id = dobj['_id']
        if self.index_name:
            d_id = re.sub("__index__", self.index_name, d_id)

        self.es_client.delete(self.__METADATA_INDEX, dobj['_type'], d_id)
        logger.info('Removed %s %s from DevRisk Analysis plugin for index %s', dobj['_type'], d_id, self.index_name)


def import_kibana_objects(kibana_objects_dir):
    kibana_objects_dir = kibana_objects_dir
    dashboards_file = os.path.join(kibana_objects_dir, 'dashboards.json')
    visualizations_file = os.path.join(kibana_objects_dir, 'visualizations.json')
    index_pattern_file = os.path.join(kibana_objects_dir, 'index-pattern.json')

    logger.info('Patching files %s, %s to include navigation widget ...', dashboards_file, dashboards_file)

    '''
    The Kibana visualization widget for navigating across all dashboards of the project
    '''
    all_dashboards_visualization = {
        "_id": "__index__-all-dashboards",
        "_type": "visualization",
        "_source": {
          "title": "__title__ Dashboards",
          "visState": {},
          "uiStateJSON": "{}",
          "description": "",
          "version": 1,
          "kibanaSavedObjectMeta": {
            "searchSourceJSON": "{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":[]}"
          }
        }
    }

    vis_state = {
        "title": "links",
        "type": "markdown",
        "params": {
            "markdown": "## All dashboards"
        },
        "aggs": [],
        "listeners": {}
    };

    '''
    Populate the navigation widget with project dashboards stored in dashboards.json template
    '''
    md_elem_template =  "  \n\n- [__dash_title__](/app/kibana#/dashboard/__dash_id__)  \n  __dash_desc__"

    with open(dashboards_file) as fp:
        dashboards = json.load(fp)

    markdown = vis_state['params']['markdown']
    for dashboard in dashboards:
        markdown += re.sub("__dash_desc__", dashboard['_source']['description'],
                           re.sub("__dash_title__", dashboard['_source']['title'],
                                  re.sub("__dash_id__", dashboard['_id'], md_elem_template)))

    vis_state['params']['markdown'] = markdown
    all_dashboards_visualization['_source']['visState'] = json.dumps(vis_state)

    '''
    Add the navigation widget to the visualizations read from visualizations.json file
    '''
    with open(visualizations_file) as fp:
        visualizations = json.load(fp)

    visualizations.append(all_dashboards_visualization)

    '''
    Add the navigation widget to all dashboards configured in the dashboards.json tempalte file.
    '''
    panel = {
        "id": "__index__-all-dashboards",
        "type": "visualization",
        "panelIndex": 200,
        "size_x": 12,
        "size_y": 4,
        "col": 1,
        "row": 200
    };

    for dashboard in dashboards:
        panels = json.loads(dashboard['_source']['panelsJSON'])
        panels.append(panel)
        dashboard['_source']['panelsJSON'] = json.dumps(panels)

    '''
    Get the index pattern objects
    '''
    with open(index_pattern_file) as fp:
        index_pattern = json.load(fp)

    logger.info('Done getting Kibana objects, visualizations and dashboards.')
    return index_pattern + visualizations + dashboards

def import_dra_objects(templates_dir):
    with open(os.path.join(templates_dir, 'dra-project.json')) as fp:
        return json.load(fp)
