from agavepy.agave import Agave, AgaveException
import ConfigParser
import cx_Oracle
import json
import logging
import os
import sys
import time
import urllib

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


def set_metadata_public_permissions(agave, metadata_uuid, username, permission):
    set_metadata_public_permissions
    body = {}
    body['username'] = username
    body['permission'] = permission
    body_json = json.dumps(body)
    try:
        logging.debug('set_metadata_public_permissions - before meta.updateMetadataPermissionsForUser')
        metadata_permission = agave.meta.updateMetadataPermissionsForUser(uuid=metadata_uuid, username=username, body=body_json)
        logging.debug('set_metadata_public_permissions - after meta.updateMetadataPermissionsForUser - metadata permission: ')
        logging.debug(metadata_permission)
    except Exception, e:
        logging.debug('set_metadata_public_permissions - FAIL - updateMetadataPermissionsForUser: ')
        logging.debug('metadata_uuid:' + metadata_uuid + ', username: ' + username + ', permission: ' + permission)
        logging.debug(e)


def insert_project_metadata(root_dir, agave_system, cursor, agave, logging):
    logging.debug('insert_project_metadata')

    # get project name
    project_name = os.path.basename(os.path.normpath(root_dir))
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
        cursor.execute("select distinct c.name as component, e.class_name as equipment_class, f.name as facility from experiment a join experiment_equipment b on a.expid = b.experiment_id join equipment c on b.equipment_id = c.equipment_id join equipment_model d on c.model_id = d.id join equipment_class e on d.equipment_class_id = e.equipment_class_id join organization f on c.orgid = f.orgid where a.projid = " + "\'" + str(project_id) + "\'")
        equipment_dict_list = convert_rows_to_dict_list(cursor)
        row_dict['equipment'] = equipment_dict_list

        # add path and change name
        row_dict['path'] = '/'

        # clean dates & description
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
        project_metadata['name'] = 'object'
        project_metadata['value'] = {}
        project_metadata['value'] = row_dict
        project_metadata['value']['deleted'] = 'false'
        project_metadata['value']['systemId'] = agave_system
        project_metadata['value']['projectPath'] = os.path.basename(os.path.normpath(root_dir))
        project_metadata_json = json.dumps(project_metadata)
        logging.debug('insert_project_metadata - project_metadata_json: ')
        logging.debug(project_metadata_json)

        try:
            logging.debug('insert_project_metadata - before meta.addMetadata')
            project_metadata = agave.meta.addMetadata(body=project_metadata_json)
            logging.debug('insert_project_metadata - after meta.addMetadata - project_metadata uuid:')
            logging.debug( project_metadata['uuid'])

            # set public permissions
            set_metadata_public_permissions(agave, project_metadata['uuid'], 'world', 'READ')
            return project_metadata['uuid']
        except Exception, e:
            logging.debug('insert_project_metadata - FAIL - addMetadata:')
            logging.debug(e)


def insert_experiment_metadata(root_dir, agave_system, experiment_name, cursor, agave, project_metadata_uuid):
    logging.debug('insert_experiment_metadata')
    logging.debug('project_metadata_uuid:')
    logging.debug(project_metadata_uuid)

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
        if 'projid' in row_dict:
            del row_dict['projid']

        # create and insert experiment metadata
        # experiment_dir_path = hashlib.md5(NEES-####-####.groups/Experiment-#)
        experiment_dir_path = os.path.basename(os.path.normpath(root_dir)) + '/' + row_dict['name']
        experiment_metadata = {}
        experiment_metadata['name'] = 'object'
        experiment_metadata['associationIds'] = project_metadata_uuid
        experiment_metadata['value'] = {}
        experiment_metadata['value'] = row_dict
        experiment_metadata['value']['deleted'] = 'false'
        experiment_metadata['value']['systemId'] = agave_system
        experiment_metadata['value']['experimentPath'] = experiment_dir_path
        experiment_metadata_json = json.dumps(experiment_metadata)

        logging.debug('insert_experiment_metadata - experiment_metadata_json:')
        logging.debug( experiment_metadata_json )

        try:
            logging.debug('insert_experiment_metadata - before meta.addMetadata')
            experiment_metadata = agave.meta.addMetadata(body=experiment_metadata_json)
            logging.debug('insert_experiment_metadata - after meta.addMetadata - experiment_metadata uuid:')
            logging.debug( experiment_metadata['uuid'] )

            # set public permissions
            set_metadata_public_permissions(agave, experiment_metadata['uuid'], 'world', 'READ')

            return experiment_metadata['uuid']
        except Exception, e:
            logging.debug('insert_experiment_metadata - FAIL - addMetadata:')
            logging.debug(e)


def walk_project_directory(root_dir, agave, agave_system, cursor, project_metadata_uuid, logging, project_dir_size):
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
                    experiment_metadata_uuid = insert_experiment_metadata(root_dir, agave_system, dir_name.split(os.path.sep)[-1], cursor, agave, project_metadata_uuid)
                    logging.debug('walk_project_directory - experiment_metadata_uuid:')
                    logging.debug(experiment_metadata_uuid)

                ########################## insert exp dir ##################
                dir_size = get_dir_size(dir_name)
                logging.debug('walk_project_directory - dir_size:')
                logging.debug(dir_size)

                experiment_dir_metadata = {}
                # experiment_dir_metadata['name'] = dir_name
                experiment_dir_metadata['name'] = 'object'
                experiment_dir_metadata['associationIds'] = [experiment_metadata_uuid]
                experiment_dir_metadata['value'] = {}
                experiment_dir_metadata['value']['format'] = 'folder'
                experiment_dir_metadata['value']['length'] = dir_size
                experiment_dir_metadata['value']['path'] = rel_path
                experiment_dir_metadata['value']['name'] = dir_name.split(os.path.sep)[-1]
                experiment_dir_metadata['value']['permissions'] = 'READ'
                experiment_dir_metadata['value']['systemId'] = agave_system
                experiment_dir_metadata['value']['type'] = 'dir'
                experiment_dir_metadata['value']['legacy'] = 'true'
                experiment_dir_metadata['value']['deleted'] = 'false'

                experiment_dir_metadata_json = json.dumps(experiment_dir_metadata)
                logging.debug('walk_project_directory - experiment_dir_metadata_json:')
                logging.debug(experiment_dir_metadata_json)
                logging.debug('walk_project_directory - before meta.addMetadata')
                experiment_dir_metadata_uuid = agave.meta.addMetadata(body=experiment_dir_metadata_json)
                logging.debug('walk_project_directory - after meta.addMetadata - experiment_dir_metadata_uuid:')
                logging.debug(experiment_dir_metadata_uuid['uuid'])

                # set public permissions
                set_metadata_public_permissions(agave, experiment_dir_metadata_uuid['uuid'], 'world', 'READ')
                ########################## end insert exp dir ##################

                for fname in file_list:
                    logging.debug('\twalk_project_directory - Inserting experiment file: ' +  fname)

                    file_path = os.path.join(dir_name, fname)
                    file_size = os.path.getsize(file_path)

                    logging.debug('\twalk_project_directory - file size:')
                    logging.debug(file_size)

                    experiment_file_metadata = {}
                    experiment_file_metadata['name'] = 'object'
                    experiment_file_metadata['associationIds'] = [experiment_metadata_uuid]
                    experiment_file_metadata['value'] = {}
                    experiment_file_metadata['value']['format'] = 'raw'
                    experiment_file_metadata['value']['length'] = file_size
                    experiment_file_metadata['value']['path'] = dir_name
                    experiment_file_metadata['value']['name'] = fname
                    experiment_file_metadata['value']['permissions'] = 'READ'
                    experiment_file_metadata['value']['systemId'] = agave_system
                    experiment_file_metadata['value']['type'] = 'file'
                    experiment_file_metadata['value']['legacy'] = 'true'
                    experiment_file_metadata['value']['deleted'] = 'false'

                    experiment_file_metadata_json = json.dumps(experiment_file_metadata)
                    logging.debug('\twalk_project_directory - experiment_file_metadata_json:')
                    logging.debug( experiment_file_metadata_json )
                    logging.debug('\twalk_project_directory - before meta.addMetadata')
                    experiment_file_metadata_uuid = agave.meta.addMetadata(body=experiment_file_metadata_json)
                    logging.debug('\twalk_project_directory - after meta.addMetadata - experiment_file_metadata_uuid:')
                    logging.debug( experiment_file_metadata_uuid['uuid'] )

                    # set public permissions
                    set_metadata_public_permissions(agave, experiment_file_metadata_uuid['uuid'], 'world', 'READ')



            # create project dir/files metadata
            else:
                logging.debug('walk_project_directory - Inserting project dir metadata: ' + dir_name)
                ########################## insert project dir  ##################
                dir_size = get_dir_size(dir_name)
                logging.debug('walk_project_directory - dir_size:')
                logging.debug(dir_size)

                project_dir_metadata = {}
                project_dir_metadata['name'] = 'object'
                project_dir_metadata['associationIds'] = [project_metadata_uuid]
                project_dir_metadata['value'] = {}
                project_dir_metadata['value']['format'] = 'folder'

                # If creating NEES-####-####.groups dir, rel path from projects/ to /
                if '.groups' in dir_name.split(os.path.sep)[-1]:
                    project_dir_metadata['value']['path'] = '/'
                else:
                    project_dir_metadata['value']['path'] = rel_path

                project_dir_metadata['value']['length'] = dir_size
                project_dir_metadata['value']['name'] = dir_name.split(os.path.sep)[-1]
                project_dir_metadata['value']['permissions'] = 'READ'
                project_dir_metadata['value']['systemId'] = agave_system
                project_dir_metadata['value']['type'] = 'dir'
                project_dir_metadata['value']['legacy'] = 'true'
                project_dir_metadata['value']['deleted'] = 'false'

                project_dir_metadata_json = json.dumps(project_dir_metadata)
                logging.debug('walk_project_directory - project_dir_metadata_json:')
                logging.debug(project_dir_metadata_json)
                logging.debug('walk_project_directory - before meta.addMetadata - project_dir_metadata_json:')
                project_dir_metadata_uuid = agave.meta.addMetadata(body=project_dir_metadata_json)
                logging.debug('walk_project_directory - after meta.addMetadata - project_dir_metadata_uuid:')
                logging.debug(project_dir_metadata_uuid['uuid'])

                # set public permissions
                set_metadata_public_permissions(agave, project_dir_metadata_uuid['uuid'], 'world', 'READ')
                ########################## end insert project dir ##################

                for fname in file_list:
                    logging.debug('\twalk_project_directory - Inserting project file metadata ' + fname)
                    logging.debug('\twalk_project_directory - agave_system = ' + agave_system)

                    file_path = os.path.join(dir_name, fname)
                    file_size = os.path.getsize(file_path)

                    logging.debug('\twalk_project_directory - file size:')
                    logging.debug(file_size)

                    # create project_dir_metadata
                    project_file_metadata = {}
                    project_file_metadata['name'] = 'object'
                    project_file_metadata['associationIds'] = [project_metadata_uuid]
                    project_file_metadata['value'] = {}
                    project_file_metadata['value']['format'] = 'raw'
                    project_file_metadata['value']['length'] = file_size
                    project_file_metadata['value']['path'] = dir_name
                    project_file_metadata['value']['name'] = fname
                    project_file_metadata['value']['permissions'] = 'READ'
                    project_file_metadata['value']['systemId'] = agave_system
                    project_file_metadata['value']['type'] = 'file'
                    project_file_metadata['value']['legacy'] = 'true'
                    project_file_metadata['value']['deleted'] = 'false'

                    project_file_metadata_json = json.dumps(project_file_metadata)
                    logging.debug('\twalk_project_directory - project_file_metadata_json:')
                    logging.debug( project_file_metadata_json )
                    logging.debug('\twalk_project_directory - before meta.addMetadata')
                    project_file_metadata_uuid = agave.meta.addMetadata(body=project_file_metadata_json)
                    logging.debug('\twalk_project_directory - after meta.addMetadata - project_file_metadata_uuid:')
                    logging.debug( project_file_metadata_uuid['uuid'])

                    # set public permissions
                    set_metadata_public_permissions(agave, project_file_metadata_uuid['uuid'], 'world', 'READ')


        except Exception, e:
            logging.debug('walk_project_directory - Exception: ')
            logging.debug(e)

def main(args):
    Config = ConfigParser.ConfigParser()
    Config.read(os.path.realpath(__file__).rsplit(os.path.sep, 1)[0] + '/config.properties')

    # nees db auth
    user = Config.get('nees', 'user')
    pswd=Config.get('nees', 'pswd')
    host=Config.get('nees', 'host')
    port=Config.get('nees', 'port')
    sid=Config.get('nees', 'sid')
    dsn = cx_Oracle.makedsn(host, port, sid)
    db = cx_Oracle.connect(user, pswd, dsn)
    cursor = db.cursor()

    # agave auth
    api_server = Config.get('agave', 'api_server')
    agave_system = Config.get('agave', 'system')
    agave_user = Config.get('agave', 'user')
    token=Config.get('agave', 'token')

    # start agave
    agave = Agave(api_server=api_server, token=token)

    # insert project metadata
    root_dir = args[0]


    log_file = os.path.realpath(__file__).rsplit(os.path.sep, 1)[0] + '/logs/' + root_dir + '.log'
    FORMAT = "%(asctime)s.%(msecs)d %(message)s"
    logging.basicConfig(format=FORMAT, filename=log_file,level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
    logging.basicConfig(filename=log_file,level=logging.DEBUG)

    # check if project metadata has already been created, if so, do nothing
    # metadata_object = {}
    # metadata_object['name'] = 'object'
    # metadata_object['value.name'] = root_dir
    #
    # logging.debug('main - metadata_object:')
    # logging.debug(metadata_object)
    #
    # metadata_in_system = agave.meta.listMetadata(q=json.dumps(metadata_object))
    #
    # if not metadata_in_system:
    project_metadata_uuid = insert_project_metadata(root_dir, agave_system, cursor, agave, logging)
    if not project_metadata_uuid:
        logging.debug('main - could not insert project metadata, skipping this project')
    else:
        logging.debug('main - inserting project: ' + root_dir)
        project_dir_size = 0
        walk_project_directory(root_dir, agave, agave_system, cursor, project_metadata_uuid, logging, project_dir_size)
    # else:
    #     walk_project_directory(root_dir, agave, cursor, metadata_in_system[0]['uuid'], logging)


if len(sys.argv) < 2:
    # TO-DO: fix this so you can feed paths instead of names
    print 'Usage: $ python meta.py <NEES-####-####.groups>'
    print 'e.g. $ python meta.py NEES-2005-0086.groups'
else:
    main(sys.argv[1:])
