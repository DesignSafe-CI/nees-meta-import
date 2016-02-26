import ConfigParser
import cx_Oracle
from elasticsearch import Elasticsearch,helpers
import json
import logging
import os
import sys
import time
import urllib
import hashlib

def convert_rows_to_dict_list(cursor):
    columns = list()
    for column in cursor.description:
        if '_' in column[0]:
            columns.append(convert_to_camel_case(column[0].lower()))
        else :
            columns.append(column[0].lower())
    return [dict(zip(columns, row)) for row in cursor]

def convert_to_camel_case(snake_str):
    components = snake_str.split('_')
    return components[0] + "".join(x.title() for x in components[1:])

def get_dir_size(path):
    return sum( os.path.getsize(os.path.join(dirpath,filename)) for dirpath, dirnames, filenames in os.walk( path ) for filename in filenames )


def insert_project_metadata(root_dir, cursor, project_objects, logging, _index):
    logging.debug('insert_project_metadata')

    # get project name
    project_name = os.path.basename(os.path.normpath(root_dir))

    # find project id
    project_name = project_name.split('.')[0]

    logging.debug('insert_project_metadata - project_name: ' + project_name)
    cursor.execute("select projid from project where name = " + "\'" + str(project_name) + "\'")
    project_rows_dict_list = convert_rows_to_dict_list(cursor)
    project_id = project_rows_dict_list[0]['projid']

    cursor.execute("select projid, name, title, start_date, end_date, description_4k, fundorg, fundorgprojid  from project where projid = " + "\'" + str(project_id) + "\'")
    project_rows_dict_list = convert_rows_to_dict_list(cursor)
    for row_dict in project_rows_dict_list:

        # pis
        cursor.execute('select FIRST_NAME, LAST_NAME from PROJECT_GROUP a join PERSON b on a.person_id = b.id where a.projid = ' + str(row_dict['projid']));
        pis_dict_list = convert_rows_to_dict_list(cursor)
        row_dict['pis'] = pis_dict_list

        # organization
        cursor.execute('select b.name, b.state, b.country from project_organization a join organization b on a.orgid = b.orgid where a.projid = ' + str(row_dict['projid']))
        organization_dict_list = convert_rows_to_dict_list(cursor)
        row_dict['organization'] = organization_dict_list

        # equipment
        cursor.execute("select c.name as component, e.class_name as equipment_class, f.name as facility from experiment a join experiment_equipment b on a.expid = b.experiment_id join equipment c on b.equipment_id = c.equipment_id join equipment_model d on c.model_id = d.id join equipment_class e on d.equipment_class_id = e.equipment_class_id join organization f on c.orgid = f.orgid where a.projid = " + "\'" + str(project_id) + "\'")
        equipment_dict_list = convert_rows_to_dict_list(cursor)
        row_dict['equipment'] = equipment_dict_list

        # clean object
        if row_dict['startDate'] is not None:
            row_dict['startDate'] = row_dict['startDate'].strftime('%Y-%m-%d %H:%M:%S')

        if row_dict['endDate'] is not None:
            row_dict['endDate'] = row_dict['endDate'].strftime('%Y-%m-%d %H:%M:%S')

        if 'description4K' in row_dict:
            row_dict['description'] = row_dict['description4K']
            del row_dict['description4K']
        if 'projid' in row_dict:
            del row_dict['projid']

        # create and insert project metadata
        project_metadata = {}
        project_metadata = row_dict
        project_metadata['_index'] = _index
        project_metadata['_type'] = 'project'
        project_metadata['_id'] = hashlib.md5(row_dict['name']).hexdigest()
        project_metadata['deleted'] = "false"

        try:
            logging.debug('insert_project_metadata - project_metadata.append')
            project_objects.append(project_metadata)
            logging.debug('insert_project_metadata - project_metadata.append')
            return root_dir
        except Exception, e:
            logging.debug('insert_project_metadata - FAIL - project_metadata.append:')
            logging.debug(e)

def insert_experiment_metadata(root_dir, experiment_name, cursor, project_objects, project_metadata_id, _index):
    logging.debug('insert_experiment_metadata')
    logging.debug('project_metadata_id:')
    logging.debug(project_metadata_id)

    # get project name
    project_name = os.path.basename(os.path.normpath(root_dir))

    # parse name and get id
    project_name = project_name.split('.')[0]

    logging.debug('insert_experiment_metadata - project_name:')
    logging.debug(project_name)

    cursor.execute("select projid from project where name = " + "\'" + str(project_name) + "\'")
    project_rows_dict_list = convert_rows_to_dict_list(cursor)
    project_id = project_rows_dict_list[0]['projid']

    logging.debug('insert_experiment_metadata - project_id:')
    logging.debug( project_id )

    logging.debug('insert_experiment_metadataa - experiment_name:')
    logging.debug( experiment_name )

    #insert experiment metadata
    cursor.execute("select projid, name, title, start_date, end_date, description_4k from experiment where projid = " + "\'" + str(project_id) + "\'" + " and name = " + "\'" + str(experiment_name) + "\'" )
    project_rows_dict_list = convert_rows_to_dict_list(cursor)

    for row_dict in project_rows_dict_list:
        logging.debug('insert_experiment_metadata - projid:')
        logging.debug( row_dict['projid'] )

        # facility
        cursor.execute("select a.name, c.name, c.state, c.country from experiment a join experiment_organization b on a.expid = b.expid join organization c on b.orgid = c.orgid where a.projid = " + "\'" + str(row_dict['projid']) + "\'" + " and a.name = " + "\'" + str(experiment_name) + "\'" )
        facility_dict_list = convert_rows_to_dict_list(cursor)
        row_dict['facility'] = facility_dict_list

        # clean dates & description
        if row_dict['startDate'] is not None:
            row_dict['startDate'] = row_dict['startDate'].strftime('%Y-%m-%d %H:%M:%S')

        if row_dict['endDate'] is not None:
            row_dict['endDate'] = row_dict['endDate'].strftime('%Y-%m-%d %H:%M:%S')

        if 'description4K' in row_dict:
            row_dict['description'] = row_dict['description4K']
            del row_dict['description4K']
        del row_dict['projid']

        # create and insert experiment metadata
        experiment_metadata = {}
        experiment_metadata = row_dict
        experiment_metadata['_index'] = _index
        experiment_metadata['_type'] = 'experiment'
        experiment_metadata['_id'] = hashlib.md5(row_dict['name']).hexdigest()
        experiment_metadata['project'] = project_metadata_id.split('.')[0]
        experiment_metadata['deleted'] = "false"

        try:
            logging.debug('insert_experiment_metadata - before project_objects.append')
            project_objects.append(experiment_metadata)
            logging.debug('insert_experiment_metadata - after project_objects.append')
            return experiment_name
        except Exception, e:
            logging.debug('insert_experiment_metadata - FAIL - addMetadata:')
            logging.debug(e)

# TO-DO: refactor params to objects
def walk_project_directory(root_dir, project_objects, agave_system, cursor, project_metadata_id, logging, project_dir_size, _index):
    # insert project dir/files metadata
    for dir_name, sub_dir_list, file_list in os.walk(root_dir, topdown=False):
        logging.debug('walk_project_directory - Found directory: ' + dir_name)

        dir_size_sum = 0

        try:
            rel_path = dir_name.rsplit(os.path.sep, 1)[0]
            logging.debug('walk_project_directory - rel_path: ' + rel_path)

            # create experiment dir/files metadata
            if 'Experiment-' in dir_name:

                # if Experiment-* insert one time only experiment db metadata
                experiment_metadata_uuid = ''
                if 'Experiment-' in dir_name.split(os.path.sep)[-1]:
                    experiment_metadata_uuid = insert_experiment_metadata(root_dir, dir_name.split(os.path.sep)[-1], cursor, project_objects, project_metadata_id, _index)
                    logging.debug('walk_project_directory - experiment_metadata_uuid:')
                    logging.debug(experiment_metadata_uuid)

                ########################## insert exp dir ##################
                dir_size = get_dir_size(dir_name)
                logging.debug('walk_project_directory - dir_size:')
                logging.debug(dir_size)

                agave_path = 'agave://' + agave_system + '/' + rel_path + '/' + dir_name.split(os.path.sep)[-1]
                logging.debug('\twalk_project_directory experiment_dir_metadata agave_path:')
                logging.debug(agave_path)

                experiment_dir_metadata = {}
                experiment_dir_metadata['_index'] = 'nees'
                experiment_dir_metadata['_type'] = 'object'
                experiment_dir_metadata['_id'] = hashlib.md5(agave_path).hexdigest()
                experiment_dir_metadata['project'] = root_dir
                experiment_dir_metadata['format'] = 'folder'
                experiment_dir_metadata['length'] = dir_size
                experiment_dir_metadata['path'] = rel_path
                experiment_dir_metadata['name'] = dir_name.split(os.path.sep)[-1]
                experiment_dir_metadata['systemId'] = agave_system
                experiment_dir_metadata['type'] = 'dir'
                experiment_dir_metadata['deleted'] = 'false'
                experiment_dir_metadata['agavePath'] = agave_path

                logging.debug('walk_project_directory - before meta.addMetadata')
                project_objects.append(experiment_dir_metadata)
                logging.debug('walk_project_directory - after meta.addMetadata')
                ########################## end insert exp dir ##################

                for fname in file_list:
                    logging.debug('\twalk_project_directory - Inserting experiment file: ' +  fname)

                    file_path = os.path.join(dir_name, fname)
                    file_size = os.path.getsize(file_path)

                    logging.debug('\twalk_project_directory - file size:')
                    logging.debug(file_size)

                    agave_path = 'agave://' + agave_system + '/' + dir_name + '/' + fname
                    logging.debug('\twalk_project_directory - experiment_file_metadata agave_path:')
                    logging.debug(agave_path)

                    experiment_file_metadata = {}
                    experiment_file_metadata['_index'] = 'nees'
                    experiment_file_metadata['_type'] = 'object'
                    experiment_file_metadata['_id'] = hashlib.md5(agave_path).hexdigest()
                    experiment_file_metadata['project'] = root_dir
                    experiment_file_metadata['format'] = 'raw'
                    experiment_file_metadata['length'] = file_size
                    experiment_file_metadata['path'] = dir_name
                    experiment_file_metadata['name'] = fname
                    experiment_file_metadata['systemId'] = agave_system
                    experiment_file_metadata['type'] = 'file'
                    experiment_file_metadata['deleted'] = 'false'
                    experiment_file_metadata['agavePath'] = agave_path

                    logging.debug('\twalk_project_directory - before project_objects.append')
                    project_objects.append(experiment_file_metadata)
                    logging.debug('\twalk_project_directory - after project_objects.append')



            # create project dir/files metadata
            else:
                logging.debug('walk_project_directory - Inserting project dir metadata: ' + dir_name)
                ########################## insert project dir  ##################
                dir_size = get_dir_size(dir_name)
                logging.debug('walk_project_directory - dir_size:')
                logging.debug(dir_size)

                project_dir_metadata = {}
                project_dir_metadata['_index'] = 'nees'
                project_dir_metadata['_type'] = 'object'
                project_dir_metadata['project'] = root_dir
                project_dir_metadata['format'] = 'folder'

                # If creating NEES-####-####.groups dir, rel path from projects/ to /
                if '.groups' in dir_name.split(os.path.sep)[-1]:
                    project_dir_metadata['path'] = '/'
                    agave_path = 'agave://' + agave_system + '/' + dir_name.split(os.path.sep)[-1]
                    logging.debug('walk_project_directory - project_dir_metadata agave_path:')
                    logging.debug(agave_path)
                else:
                    project_dir_metadata['path'] = rel_path
                    agave_path = 'agave://' + agave_system + '/' + project_dir_metadata['path'] + '/' + dir_name.split(os.path.sep)[-1]
                    logging.debug('walk_project_directory - project_dir_metadata agave_path:')
                    logging.debug(agave_path)

                project_dir_metadata['_id'] = hashlib.md5(agave_path).hexdigest()
                project_dir_metadata['length'] = dir_size
                project_dir_metadata['name'] = dir_name.split(os.path.sep)[-1]
                project_dir_metadata['systemId'] = agave_system
                project_dir_metadata['type'] = 'dir'
                project_dir_metadata['deleted'] = 'false'
                project_dir_metadata['agavePath'] = agave_path

                logging.debug('walk_project_directory - before meta.addMetadata:')
                project_objects.append(project_dir_metadata)
                logging.debug('walk_project_directory - after meta.addMetadata')
                ########################## end insert project dir ##################

                for fname in file_list:
                    logging.debug('\twalk_project_directory - Inserting project file metadata ' + fname)
                    logging.debug('\twalk_project_directory - agave_system = ' + agave_system)

                    file_path = os.path.join(dir_name, fname)
                    file_size = os.path.getsize(file_path)

                    logging.debug('\twalk_project_directory - file size:')
                    logging.debug(file_size)

                    agave_path = 'agave://' + agave_system + '/' + dir_name + '/' + fname
                    logging.debug('\twalk_project_directory - project_file_metadata agave_path:')
                    logging.debug(agave_path)

                    # create project_dir_metadata
                    project_file_metadata = {}
                    project_file_metadata['_index'] = 'nees'
                    project_file_metadata['_type'] = 'object'
                    project_file_metadata['_id'] = hashlib.md5(agave_path).hexdigest()
                    project_file_metadata['project'] = root_dir.split('.')[0]
                    project_file_metadata['format'] = 'raw'
                    project_file_metadata['length'] = file_size
                    project_file_metadata['path'] = dir_name
                    project_file_metadata['name'] = fname
                    project_file_metadata['systemId'] = agave_system
                    project_file_metadata['type'] = 'file'
                    project_file_metadata['deleted'] = 'false'
                    project_file_metadata['agavePath'] = agave_path

                    logging.debug('\twalk_project_directory - before meta.addMetadata')
                    project_objects.append(project_file_metadata)
                    logging.debug('\twalk_project_directory - after meta.addMetadata')

        except Exception, e:
            logging.debug('walk_project_directory - Exception: ')
            logging.debug(e)

def main(args):
    Config = ConfigParser.ConfigParser()
    Config.read(os.path.realpath(__file__).rsplit(os.path.sep, 1)[0] + '/config.properties')

    # nees db auth
    user=Config.get('nees', 'user')
    pswd=Config.get('nees', 'pswd')
    host=Config.get('nees', 'host')
    port=Config.get('nees', 'port')
    sid=Config.get('nees', 'sid')
    dsn = cx_Oracle.makedsn(host, port, sid)
    db = cx_Oracle.connect(user, pswd, dsn)
    cursor = db.cursor()

    # agave system
    agave_system = Config.get('agave', 'system')

    root_dir = args[0]

    log_file = os.path.realpath(__file__).rsplit(os.path.sep, 1)[0] + '/logs/' + root_dir + '.log'

    es_tracer = logging.getLogger('elasticsearch.trace')
    es_tracer.setLevel(logging.INFO)
    es_tracer.addHandler(logging.FileHandler(os.path.realpath(__file__).rsplit(os.path.sep, 1)[0] + '/logs/' + root_dir + '_es.log'))


    FORMAT = "%(asctime)s.%(msecs)d %(message)s"
    logging.basicConfig(format=FORMAT, filename=log_file,level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
    logging.basicConfig(filename=log_file,level=logging.DEBUG)

    _index = Config.get('es', '_index')

    project_objects = []
    project_metadata_id = insert_project_metadata(root_dir, cursor, project_objects, logging, _index)

    if not project_metadata_id:
        logging.debug('main - could not insert project metadata, skipping this project')
    else:
        logging.debug('main - before inserting project: ' + root_dir)
        project_dir_size = 0
        walk_project_directory(root_dir, project_objects, agave_system, cursor, project_metadata_id, logging, project_dir_size, _index)
        logging.debug('main - after inserting project: ' + root_dir)
        project_objects_tuple = tuple(project_objects)
        es = Elasticsearch([Config.get('es', 'es_server')])
        project_objects_inserted = helpers.bulk(es, project_objects_tuple)


if len(sys.argv) < 2:
    # TO-DO: fix this so you can feed paths instead of names
    print 'Usage: $ python metaes.py <NEES-####-####.groups>'
    print 'e.g. $ python metaes.py NEES-2005-0086.groups'
else:
    main(sys.argv[1:])
