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
import MySQLdb

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


def insert_project_metadata(root_dir, agave_system, central_cursor, neeshub_cursor, project_objects, logging, _index):
    logging.debug('insert_project_metadata')

    # get project name
    project_name = os.path.basename(os.path.normpath(root_dir))
    project_name = project_name.split('.')[0]

    logging.debug('insert_project_metadata - project_name: ' + project_name)
    central_cursor.execute("select projid from project where name = " + "\'" + str(project_name) + "\'")
    project_rows_dict_list = convert_rows_to_dict_list(central_cursor)
    project_id = project_rows_dict_list[0]['projid']

    central_cursor.execute("select projid, name, title, start_date, end_date, description_4k from project where projid = " + "\'" + str(project_id) + "\'")
    project_rows_dict_list = convert_rows_to_dict_list(central_cursor)
    for row_dict in project_rows_dict_list:

        # pis
        central_cursor.execute('select distinct p.last_name, p.first_name from person p, authorization a, person_entity_role per, project pr where p.id = a.person_id and p.deleted = 0 and a.entity_type_id = 1 and p.id = per.person_id and a.entity_type_id = per.entity_type_id and a.entity_id = per.entity_id and pr.projid = per.entity_id  and (per.role_id = 1 or per.role_id = 2) and pr.projid = ' + str(row_dict['projid']));
        pis_dict_list = convert_rows_to_dict_list(central_cursor)
        row_dict['pis'] = pis_dict_list

        # organization
        central_cursor.execute('select b.name, b.state, b.country from project_organization a join organization b on a.orgid = b.orgid where a.projid = ' + str(row_dict['projid']))
        organization_dict_list = convert_rows_to_dict_list(central_cursor)
        if (bool(organization_dict_list) != False):
            row_dict['organization'] = organization_dict_list

        # sponsor
        central_cursor.execute('select fund_org, award_num, award_url from project_grant where projid = ' + str(row_dict['projid']))
        sponsor_rows_dict_list = convert_rows_to_dict_list(central_cursor)
        if (bool(sponsor_rows_dict_list) != False):
            row_dict['sponsor'] = []
            for sponsor_dict in sponsor_rows_dict_list:
                sponsor_object = {}
                sponsor_object['name'] = str(sponsor_dict.get('fundOrg')) + '-' + str(sponsor_dict.get('awardNum'))
                sponsor_object['url'] = str(sponsor_dict.get('awardUrl'))
                row_dict['sponsor'].append(sponsor_object)

        # facility
        central_cursor.execute('select distinct c.name, c.state, c.country from experiment a join experiment_facility b on a.expid = b.expid join organization c on b.facilityid = c.facilityid where a.projid = ' + str(row_dict['projid']))
        facility_rows_dict_list = convert_rows_to_dict_list(central_cursor)
        if (bool(facility_rows_dict_list) != False):
            row_dict['facility'] = facility_rows_dict_list

        # equipment
        central_cursor.execute("select distinct  pe.name as equipment, e.name as component, ec.class_name as equipment_class, org.name as facility from equipment e inner join experiment_equipment ee on e.equipment_id = ee.equipment_id inner join experiment ex on ee.experiment_id = ex.expid inner join equipment_model em on e.model_id = em.id inner join equipment_class ec on em.equipment_class_id = ec.equipment_class_id inner join organization org on e.orgid = org.orgid left outer join equipment pe on pe.equipment_id = e.parent_id where ex.projid = " + "\'" + str(project_id) + "\'" + " order by pe.name, e.name")
        equipment_dict_list = convert_rows_to_dict_list(central_cursor)
        row_dict['equipment'] = equipment_dict_list

        # publications
        project_owner = project_name.replace("-", "_").lower()
        neeshub_cursor.execute("select distinct r.title, r.id from jos_resources r inner join jos_xgroups g on g.cn = r.group_owner left outer join jos_xgroups_members gm on gm.gidNumber = g.gidNumber left outer join jos_users u on u.id = gm.uidNumber where r.group_owner = " + "\'" + str(project_owner) + "\'" + " group by r.title")
        publications_rows_dict_list = neeshub_cursor.fetchall()
        if (bool(publications_rows_dict_list) != False):
            row_dict['publications'] = []
            for publication_dict in publications_rows_dict_list:
                publication_object = {}
                publication_object['title'] = publication_dict['title']
                neeshub_cursor.execute("select distinct n.name as xname from jos_author_assoc a left outer join jos_users u on u.id = a.authorid left outer join jos_xprofiles n on a.authorid = n.uidnumber where a.subtable='resources' and a.subid = " + "\'" + str(publication_dict['id']) + "\'" + " order by ordering, surname, givenname, middlename")
                publication_authors_dict = neeshub_cursor.fetchall()
                if (bool(publication_authors_dict) != False):
                    publication_object['authors'] = []
                    for publication_author in publication_authors_dict:
                        publication_object['authors'].append(publication_author['xname'])
                row_dict['publications'].append(publication_object)

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
        project_metadata['deleted'] = 'false'
        project_metadata['systemId'] = agave_system
        project_metadata['projectPath'] = os.path.basename(os.path.normpath(root_dir))

        try:
            logging.debug('insert_project_metadata - project_metadata.append')
            project_objects.append(project_metadata)
            logging.debug('insert_project_metadata - project_metadata.append')
            return root_dir
        except Exception, e:
            logging.debug('insert_project_metadata - FAIL - project_metadata.append:')
            logging.debug(e)

def insert_experiment_metadata(root_dir, agave_system, experiment_name, central_cursor, neeshub_cursor, project_objects, project_metadata_id, _index):
    logging.debug('insert_experiment_metadata')
    logging.debug('project_metadata_id:')
    logging.debug(project_metadata_id)

    # get project name
    project_name = os.path.basename(os.path.normpath(root_dir))
    project_name = project_name.split('.')[0]

    logging.debug('insert_experiment_metadata - project_name:')
    logging.debug(project_name)

    central_cursor.execute("select projid from project where name = " + "\'" + str(project_name) + "\'")
    project_rows_dict_list = convert_rows_to_dict_list(central_cursor)
    project_id = project_rows_dict_list[0]['projid']

    logging.debug('insert_experiment_metadata - project_id:')
    logging.debug( project_id )

    logging.debug('insert_experiment_metadataa - experiment_name:')
    logging.debug( experiment_name )

    #insert experiment metadata
    central_cursor.execute("select projid, expid, name, title, start_date, end_date, description_4k from experiment where projid = " + "\'" + str(project_id) + "\'" + " and name = " + "\'" + str(experiment_name) + "\'" + "order by name" )
    project_rows_dict_list = convert_rows_to_dict_list(central_cursor)

    for row_dict in project_rows_dict_list:
        logging.debug('insert_experiment_metadata - projid:')
        logging.debug( row_dict['projid'] )

        # doi
        central_cursor.execute("select b.doi from experiment a join contribution b on a.expid = b.entity_id where expid = " + "\'" + str(row_dict['expid']) + "\'")
        experiment_doi_rows_dict_list = convert_rows_to_dict_list(central_cursor)
        if (bool(experiment_doi_rows_dict_list) != False):
            row_dict['doi'] = experiment_doi_rows_dict_list[0]['doi']

        # experiment type
        central_cursor.execute("select b.display_name from experiment a join experiment_domain b on a.experiment_domain_id = b.id where a.projid = " + "\'" + str(project_id) + "\'" + " and a.name = " + "\'" + str(experiment_name) + "\'" )
        experiment_type_rows_dict_list = convert_rows_to_dict_list(central_cursor)
        if (bool(experiment_type_rows_dict_list) != False):
            row_dict['type'] = experiment_type_rows_dict_list[0].get('displayName')

        # equipment
        central_cursor.execute("select distinct  pe.name as equipment, e.name as component, ec.class_name as equipment_class, org.name as facility from equipment e inner join experiment_equipment ee on e.equipment_id = ee.equipment_id inner join experiment ex on ee.experiment_id = ex.expid inner join equipment_model em on e.model_id = em.id inner join equipment_class ec on em.equipment_class_id = ec.equipment_class_id inner join organization org on e.orgid = org.orgid left outer join equipment pe on pe.equipment_id = e.parent_id where ex.projid = " + "\'" + str(project_id) + "\'" + " and ex.expid = " + "\'" + str(row_dict['expid']) + "\'"  + " order by pe.name, e.name")
        equipment_dict_list = convert_rows_to_dict_list(central_cursor)
        row_dict['equipment'] = equipment_dict_list

        # specimen type
        central_cursor.execute("select distinct b.title as name, b.description as description from experiment a join specimen b on a.expid = b.expid join specimen_component c on b.id = c.specimen_id where a.projid = " + "\'" + str(project_id) + "\'" + " and a.name = " + "\'" + str(experiment_name) + "\'" )
        specimen_type_rows_dict_list = convert_rows_to_dict_list(central_cursor)
        specimen_type_list = []
        for specimen_row_dict in specimen_type_rows_dict_list:
            specimen_type_list.append(specimen_row_dict)
        if len(specimen_type_list) > 0:
            row_dict['specimenType'] = specimen_type_list

        # facility
        central_cursor.execute("select distinct c.name, c.state, c.country from experiment a join experiment_facility b on a.expid = b.expid join organization c on b.facilityid = c.facilityid where a.projid = " + "\'" + str(row_dict['projid']) + "\'" + " and a.name = " + "\'" + str(experiment_name) + "\'" )
        facility_dict_list = convert_rows_to_dict_list(central_cursor)
        if (bool(facility_dict_list) != False):
            row_dict['facility'] = facility_dict_list

        # material
        central_cursor.execute("select distinct c.id, c.title from experiment a join specimen b on a.expid = b.expid join specimen_component c on b.id = c.specimen_id where a.projid = " + "\'" + str(row_dict['projid']) + "\'" + " and a.expid = " + "\'" + str(row_dict['expid']) + "\'" + " order by c.title" )
        materials_rows_dict_list = convert_rows_to_dict_list(central_cursor)
        if (bool(materials_rows_dict_list) != False):
            row_dict['material'] = []
            for materials_dict in materials_rows_dict_list:
                central_cursor.execute("select distinct d.title from experiment a join specimen b on a.expid = b.expid join specimen_component c on b.id = c.specimen_id join speccomp_material d on c.id = d.specimen_component_id where a.projid = " + "\'" + str(row_dict['projid']) + "\'" + " and c.id = " + "\'" + str(materials_dict.get('id')) + "\'" )
                material_dict_list = convert_rows_to_dict_list(central_cursor)

                component_material = {}
                component_material['component'] = materials_dict.get('title')
                component_material_list = []
                for material in material_dict_list:
                    component_material_list.append(material.get('title'))
                component_material['materials'] = component_material_list
                row_dict['material'].append(component_material)

        # sensors
        central_cursor.execute("select name from location_plan where expid = " + "\'" + str(row_dict['expid']) + "\'" + " order by name")
        sensors_rows_dict_list = convert_rows_to_dict_list(central_cursor)

        if (bool(sensors_rows_dict_list) != False):
            row_dict['sensors'] = []
            for sensor in sensors_rows_dict_list:
                row_dict['sensors'].append(sensor.get('name'))

        # clean dates & description
        if row_dict['startDate'] is not None:
            row_dict['startDate'] = row_dict['startDate'].strftime('%Y-%m-%d %H:%M:%S')

        if row_dict['endDate'] is not None:
            row_dict['endDate'] = row_dict['endDate'].strftime('%Y-%m-%d %H:%M:%S')

        if 'description4K' in row_dict:
            if row_dict['description4K'] is None:
                del row_dict['description4K']
            else :
                row_dict['description'] = row_dict['description4K']
                del row_dict['description4K']
        if 'projid' in row_dict:
            del row_dict['projid']
        if 'expid' in row_dict:
            del row_dict['expid']


        # create and insert experiment metadata
        # experiment_dir_path = hashlib.md5(NEES-####-####.groups/Experiment-#)
        experiment_dir_path = os.path.basename(os.path.normpath(root_dir)) + '/' + row_dict['name']
        experiment_metadata = {}
        experiment_metadata = row_dict
        experiment_metadata['_index'] = _index
        experiment_metadata['_type'] = 'experiment'
        experiment_metadata['_id'] = hashlib.md5(experiment_dir_path).hexdigest()
        experiment_metadata['project'] = project_metadata_id.split('.')[0]
        experiment_metadata['deleted'] = 'false'
        experiment_metadata['systemId'] = agave_system
        experiment_metadata['experimentPath'] = experiment_dir_path

        try:
            logging.debug('insert_experiment_metadata - before project_objects.append')
            project_objects.append(experiment_metadata)
            logging.debug('insert_experiment_metadata - after project_objects.append')
            return experiment_name
        except Exception, e:
            logging.debug('insert_experiment_metadata - FAIL - addMetadata:')
            logging.debug(e)

# TO-DO: refactor params to objects
def walk_project_directory(root_dir, project_objects, agave_system, central_cursor, neeshub_cursor, project_metadata_id, logging, project_dir_size, _index):
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
                    experiment_metadata_uuid = insert_experiment_metadata(root_dir, agave_system, dir_name.split(os.path.sep)[-1], central_cursor, neeshub_cursor, project_objects, project_metadata_id, _index)
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
                experiment_dir_metadata['_index'] = _index
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
                    experiment_file_metadata['_index'] = _index
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
                project_dir_metadata['_index'] = _index
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
                    project_file_metadata['_index'] = _index
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

    # nees central db auth
    central_user=Config.get('nees-central', 'user')
    central_pswd=Config.get('nees-central', 'pswd')
    central_host=Config.get('nees-central', 'host')
    central_port=Config.get('nees-central', 'port')
    central_sid=Config.get('nees-central', 'sid')
    central_dsn = cx_Oracle.makedsn(central_host, central_port, central_sid)
    central_db = cx_Oracle.connect(central_user, central_pswd, central_dsn)
    central_cursor = central_db.cursor()

    # nees neeshub db auth
    neeshub_host = Config.get('nees-neeshub', 'host')
    neeshub_username = Config.get('nees-neeshub', 'username')
    neeshub_password = Config.get('nees-neeshub', 'password')
    neeshub_dbname = Config.get('nees-neeshub', 'dbname')
    neeshub_db = MySQLdb.connect(neeshub_host, neeshub_username, neeshub_password, neeshub_dbname)
    neeshub_cursor = neeshub_db.cursor(MySQLdb.cursors.DictCursor);

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
    project_metadata_id = insert_project_metadata(root_dir, agave_system, central_cursor, neeshub_cursor, project_objects, logging, _index)

    if not project_metadata_id:
        logging.debug('main - could not insert project metadata, skipping this project')
    else:
        logging.debug('main - before inserting project: ' + root_dir)
        project_dir_size = 0
        walk_project_directory(root_dir, project_objects, agave_system, central_cursor, neeshub_cursor, project_metadata_id, logging, project_dir_size, _index)
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
