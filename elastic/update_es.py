import os
import urllib2
import json
import ConfigParser
from elasticsearch import Elasticsearch,helpers

Config = ConfigParser.ConfigParser()
Config.read(os.path.realpath(__file__).rsplit(os.path.sep, 1)[0] + '/config.properties')
es = Elasticsearch([Config.get('es', 'es_server')])
experiments = []
experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-1'});
# experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-3'});
# experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-4'});
# experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-5'});
# experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-6'});
# experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-7'});
# experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-8'});
# experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-9'});
# experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-10'});
# experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-11'});
# experiments.append({'project': 'NEES-2015-1271', 'name': 'Experiment-12'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-10'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-11'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-12'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-13'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-3'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-4'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-6'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-7'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-8'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-9'});
# experiments.append({'project': 'NEES-2012-1164', 'name': 'Experiment-1'});
# experiments.append({'project': 'NEES-2012-1164', 'name': 'Experiment-2'});
# experiments.append({'project': 'NEES-2012-1164', 'name': 'Experiment-3'});

# experiments.append({'project': 'NEES-2012-1157', 'name': 'Experiment-21'});
# experiments.append({'project': 'NEES-2012-1157', 'name': 'Experiment-22'});
# experiments.append({'project': 'NEES-2012-1157', 'name': 'Experiment-24'});
# experiments.append({'project': 'NEES-2012-1157', 'name': 'Experiment-26'});
# experiments.append({'project': 'NEES-2012-1157', 'name': 'Experiment-27'});
# experiments.append({'project': 'NEES-2012-1157', 'name': 'Experiment-28'});
# experiments.append({'project': 'NEES-2012-1157', 'name': 'Experiment-29'});
# experiments.append({'project': 'NEES-2012-1157', 'name': 'Experiment-30'});
# experiments.append({'project': 'NEES-2012-1160', 'name': 'Experiment-6'});
# experiments.append({'project': 'NEES-2012-1160', 'name': 'Experiment-5'});
# experiments.append({'project': 'NEES-2012-1160', 'name': 'Experiment-7'});
# experiments.append({'project': 'NEES-2012-1160', 'name': 'Experiment-8'});
# experiments.append({'project': 'NEES-2012-1160', 'name': 'Experiment-9'});
# experiments.append({'project': 'NEES-2012-1160', 'name': 'Experiment-10'});
# experiments.append({'project': 'NEES-2012-1160', 'name': 'Experiment-11'});
# experiments.append({'project': 'NEES-2012-1160', 'name': 'Experiment-14'});
# experiments.append({'project': 'NEES-2012-1160', 'name': 'Experiment-12'});
# experiments.append({'project': 'NEES-2012-1160', 'name': 'Experiment-13'});
# experiments.append({'project': 'NEES-2011-1083', 'name': 'Experiment-3'});
# experiments.append({'project': 'NEES-2011-1083', 'name': 'Experiment-2'});
# experiments.append({'project': 'NEES-2011-1083', 'name': 'Experiment-1'});
# experiments.append({'project': 'NEES-2010-0976', 'name': 'Experiment-2'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-7'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-6'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-5'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-4'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-3'});
# experiments.append({'project': 'NEES-2012-1157', 'name': 'Experiment-3'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-2'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-19'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-18'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-17'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-16'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-15'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-14'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-13'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-11'});
# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-10'});

# experiments.append({'project': 'NEES-2013-1207', 'name': 'Experiment-1'});
# experiments.append({'project': 'NEES-2012-1157', 'name': 'Experiment-1'});
# experiments.append({'project': 'NEES-2010-0928', 'name': 'Experiment-8'});
# experiments.append({'project': 'NEES-2010-0928', 'name': 'Experiment-6'});
# experiments.append({'project': 'NEES-2010-0928', 'name': 'Experiment-4'});
# experiments.append({'project': 'NEES-2010-0928', 'name': 'Experiment-3'});
# experiments.append({'project': 'NEES-2010-0928', 'name': 'Experiment-2'});
# experiments.append({'project': 'NEES-2010-0928', 'name': 'Experiment-12'});
# experiments.append({'project': 'NEES-2010-0928', 'name': 'Experiment-10'});
# experiments.append({'project': 'NEES-2010-0928', 'name': 'Experiment-1'});
# experiments.append({'project': 'NEES-2012-1158', 'name': 'Experiment-4'});
# experiments.append({'project': 'NEES-2012-1158', 'name': 'Experiment-3'});
# experiments.append({'project': 'NEES-2012-1158', 'name': 'Experiment-2'});
# experiments.append({'project': 'NEES-2007-0424', 'name': 'Experiment-2'});
# experiments.append({'project': 'NEES-2012-1163', 'name': 'Experiment-3'});
# experiments.append({'project': 'NEES-2012-1163', 'name': 'Experiment-2'});
# experiments.append({'project': 'NEES-2012-1163', 'name': 'Experiment-1'});
# experiments.append({'project': 'NEES-2006-0122', 'name': 'Experiment-31'});


i = 0
for experiment in experiments:
    project = es.search(index='nees', q='_type: "experiment" AND project._exact:' + experiments[i]['project'] + ' AND name._exact: ' + experiments[i]['name'], size=3000)

    for item in project['hits']['hits']:
        opener = urllib2.build_opener()
        opener.addheaders = [('Accept','application/vnd.citationstyles.csl+json')]
        url = 'http://doi.org/' + item['_source']['doi']
        response = opener.open(url)
        doi = json.loads(response.read())

        authors = []
        for author in doi['author']:
            authors.append({'firstName': author['given'], 'lastName': author['family']})

        body = {}
        body['doc'] = {}
        body['doc']['creators'] = authors

        result = es.update(index='nees', doc_type='experiment', id=item['_id'], body=body)
    i = i + 1
