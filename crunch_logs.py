#!/usr/bin/env python

import datetime
import time
import logging
import argparse
import sys

import boto3
import botocore
import yaml
import psycopg2
import pymysql


def get_logger():
    logger = logging.getLogger(__file__)
    # Neccesary for enabling lower level logging
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(__file__ + '.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

logger = get_logger()


def get_mysql_conn(host, db, user, password):
    mysql_connect_params = {'host': host,
                            'db': db,
                            'user': user,
                            'password': password,
                            'charset': 'utf8mb4',
                            'cursorclass': pymysql.cursors.DictCursor}
    conn = pymysql.connect(**mysql_connect_params)
    conn.autocommit_mode = True
    return conn


def execute_many_mysql_queries(mysql_conn, query, params_list):
    cur = mysql_conn.cursor()
    try:
        cur.executemany(query, params_list)
        mysql_conn.commit()
        mysql_conn.close()
        return cur.rowcount
    except(pymysql.IntegrityError, pymysql.ProgrammingError) as e:
        logger.error(repr(e))
        return None


def get_rs_conn(host, port, db, user, password):
    rs_connect_params = {'host': host,
                         'port': port,
                         'database': db,
                         'user': user,
                         'password': password}
    conn = psycopg2.connect(**rs_connect_params)
    conn.autocommit = True
    return conn


def execute_rs_query(rs_conn, query, params=()):
    cur = rs_conn.cursor()
    try:
        if not params:
            cur.execute(query)
        else:
            cur.execute(query, params)
        rs_conn.commit()
        return cur
    except (psycopg2.InternalError, psycopg2.ProgrammingError) as e:
        logger.error(e.message)
        rs_conn.close()
        return None


def run_rs_queries(rs_conn, query_list):
    '''Expects a list of tuples of ('query_name', 'query_string')
    Will run all the queries in the list and return the results.
    '''
    for q in query_list:
        logger.info('executing: ' + q[0])
        cursor = execute_rs_query(rs_conn, q[1])
    return cursor


def get_conf(bucket, key):
    '''If the specified bucket name is 'local', load config from a local file.
    Else load config from S3'''
    if bucket == 'local':
        with open(key) as fh:
            return yaml.load(fh)
    else:
        client = boto3.client('s3')
        obj = client.get_object(Bucket=bucket, Key=key)
        body = obj['Body']
    return yaml.load(body)


def get_query(base_path, q):
    '''
    This function supports both local files and s3 objects.
    If the `base_path` starts with "s3://", the function tries to bring the
    query from s3, otherwise the base_path is treated as a local directory.
    In the latter case the path could be either absolute or relative.
    '''
    delimeter = 's3://'
    if base_path.startswith(delimeter):
        stripped = base_path.split(delimeter)[1]
        slash_index = stripped.index('/')
        bucket, directory = stripped[:slash_index], stripped[slash_index + 1:]
        key = directory + '/' + q
        client = boto3.client('s3')
        obj = client.get_object(Bucket=bucket, Key=key)
        body = obj['Body']
        return body.read()
    else:
        with open(base_path + '/' + q) as fh:
            return fh.read()


def prepare_rs_queries(conf, mode='daily'):
    rv = []
    log_path = get_log_path(conf['base_log_path'], mode)
    for q in conf['prep_query_list']:
        raw_query = get_query(conf['queries_path'], q)
        if q == 'copy':
            for day_path in log_path:
                q_name = 'copy_' + '/'.join(day_path.split('/')[-4:])
                query = raw_query % (
                    day_path, conf['s3_access_key'], conf['s3_secret_key'])
                rv.append((q_name, query))
        else:
            rv.append((q, raw_query))
    return rv


def get_yesterday():
    return datetime.datetime.now().date() - datetime.timedelta(days=1)


def get_log_path(base_path, mode):
    today = datetime.datetime.now().date()
    rv = []
    day_range = {
            'daily': range(1, 2),
            'weekly': range(1, 8),
            'monthly': range(1, 31)
            }
    for i in day_range[mode]:
        past_date = today - datetime.timedelta(days=i)
        path = '{}/{:04d}/{:02d}/{:02d}/'.format(
            base_path, past_date.year, past_date.month, past_date.day)
        rv.append(path)
    return rv


def start_cluster(cluster_config):
    client = boto3.client('redshift')
    response = client.create_cluster(**cluster_config)
    return response


def delete_cluster(cluster_id):
    '''A wrapper around delete_cluster().
    Returns a response compatible with wait_for()'''
    client = boto3.client('redshift')
    try:
        response = client.delete_cluster(ClusterIdentifier=cluster_id,
                                         SkipFinalClusterSnapshot=True
                                         )
        return response['Cluster']
    except(botocore.exceptions.ClientError) as e:
        return {'ClusterStatus': e.response['Error']['Code']}


def get_cluster(cluster_id):
    client = boto3.client('redshift')
    response = client.describe_clusters()
    clusters = response['Clusters']
    if not clusters:
        raise ValueError('No RedShift clusters found!')
    else:
        cluster_ids = [c['ClusterIdentifier'] for c in clusters]
        if cluster_id not in cluster_ids:
            raise ValueError('Cluster {} not found'.format(cluster_id))
        else:
            response = client.describe_clusters(ClusterIdentifier=cluster_id)
            return response['Clusters'][0]


def wait_for(callable, params, result_key, good_response, interval=30,
             timeout=5400, debug=False):
    started = datetime.datetime.now()
    while True:
        running_time = datetime.datetime.now() - started
        if running_time.seconds >= timeout:
            logger.info('Timeout reached')
            return None
        else:
            resp_dict = callable(*params)
            if resp_dict[result_key] == good_response:
                return resp_dict
            else:
                time.sleep(interval)
                if debug:
                    logger.debug(
                        'Running for {} seconds. The response is "{}".'.format(
                            running_time.seconds, resp_dict[result_key]))


def date_rows(date, rows):
    dated_rows = []
    for row in rows:
        dated_rows.append((date,) + row)
    return dated_rows


def blocking_kill_cluster(c_name):
    return (wait_for(delete_cluster, [c_name], 'ClusterStatus', 'deleting') and
            wait_for(delete_cluster, [c_name], 'ClusterStatus', 'ClusterNotFound'))


def parse_args():
    parser = argparse.ArgumentParser('Crunch the access logs from ELB')
    parser.add_argument('-m',
                        '--mode',
                        choices=['daily', 'weekly', 'monthly'],
                        help='A kind of aggregation to perform',
                        default="daily")
    parser.add_argument('-d', '--dry-run', action='store_true')
    return parser.parse_args()

def blocking_start_cluster(conf, mode):
    cluster_name = '{}-log-cruncher'.format(mode)
    nodes_number = conf['{}_rs_size'.format(mode)]
    started_by = '{} log cruncher'.format(mode)
    delete_by = (datetime.datetime.now() +
                 datetime.timedelta(hours=12)).strftime('%Y-%m-%d %H:%M')
    cluster_config = {'ClusterIdentifier': cluster_name,
                      'NodeType': conf['rs_instance_type'],
                      'NumberOfNodes': nodes_number,
                      'MasterUsername': conf['rs_user'],
                      'MasterUserPassword': conf['rs_password'],
                      'Tags': [{'Key': 'Delete by date', 'Value': delete_by},
                               {'Key': 'Started by', 'Value': started_by}
                               ]
                      }

    try:
        logger.info('Starting cluster {}'.format(cluster_name))
        started = start_cluster(cluster_config)
        logger.info('Waiting for {} to become available'.format(cluster_name))
        result = wait_for(get_cluster, [cluster_name], 'ClusterStatus',
                          'available')
        if result:
            rs_host = result['Endpoint']['Address']
            rs_port = int(result['Endpoint']['Port'])
            return rs_host, rs_port
        else:
            e = 'Error creating the cluster - check RedShift console!'
            logger.error(e)
            logger.info('Trying to terminate the cluster')
            delete_initiated = blocking_kill_cluster(cluster_name)
            logger.info('Done')
    except Exception as e:
        exc_tb = sys.exc_info()[2]
        logger.error('Got {} exception in line {}.'.format(
                                repr(e), exc_tb.tb_lineno))
        logger.info('Terminating the cluster')
        delete_initiated = blocking_kill_cluster(cluster_name)
        logger.info('Done')

def print_queries(queries):
    for q in queries:
        print 'Query name: {}'.format(q[0])
        print '_' * 25
        print 'Query body:\n' + '_' * 25
        print q[1]

def dry_run(prep, select, insert):
    print 'We were going to execute the following queries:'
    print 'Prepare cluster:'
    print_queries(prep)
    print 'Select queries:'
    print_queries(select)
    print 'Insert queries:'
    print_queries(insert)


if __name__ == '__main__':
    conf = get_conf('local', 'config.yml')
    args = parse_args()
    mode = args.mode
    # Generate all the queries - cluster preparation, select and insert
    prep_rs_queries = prepare_rs_queries(conf, mode)
    query_kinds = conf['data_query_kinds']
    data_queries = {'insert': [], 'select': []}
    for q_kind in query_kinds:
        select_q_body = get_query(conf['queries_path'], '{}_select'.format(q_kind))
        data_queries['select'].append((q_kind, select_q_body))
        insert_q_body = get_query(conf['queries_path'], '{}_{}_insert'.format(q_kind, mode))
        data_queries['insert'].append((q_kind, insert_q_body))
    if args.dry_run:
        # Print out the queries for the given mode and exit
        dry_run(prep_rs_queries, data_queries['select'], data_queries['insert'])
        print 'Exiting...'
        sys.exit(0)
    try:
        # Start the cluster
        cluster_name = '{}-log-cruncher'.format(mode)
        logger.info('Starting {} cluster...'.format(cluster_name))
        rs_host, rs_port = blocking_start_cluster(conf, mode)
        logger.info('Connecting to {}'.format(rs_host))
        logger.info('Connecting to {}'.format(rs_host))
        rs_db = 'dev'
        rs_conn = get_rs_conn(rs_host,
                              rs_port,
                              rs_db,
                              conf['rs_user'],
                              conf['rs_password'])

        logger.info('Preparing the cluster...')
        # Prepare the cluster - create table, copy the data, etc...
        cluster_prepared = run_rs_queries(rs_conn, prep_rs_queries)
        if cluster_prepared:
            for i, q_kind in enumerate(query_kinds):
                logger.info('Executing {} query on redshift'.format(q_kind))
                q = data_queries['select'][i][1]
                results = execute_rs_query(rs_conn, q).fetchall()
                logger.debug('Adding date to results')
                yesterday = get_yesterday()
                dated_results = date_rows(yesterday, results)
                logger.info('Saving the results to the target db')
                target_conn = get_mysql_conn(conf['target_host'],
                                             conf['target_db'],
                                             conf['target_user'],
                                             conf['target_password'])

                saved = execute_many_mysql_queries(target_conn,
                                                   data_queries['insert'][i][1],
                                                   dated_results)
                if not saved:
                    logger.error('An error occured while saving the data')
                else:
                    logger.info('Done')
        else:
            e = 'Error occured while communicating with redshift cluster'
            logger.error(e)
        m = 'Cluster delete initiated. Waiting for cluster terminatation'
        logger.info(m)
        delete_initiated = blocking_kill_cluster(cluster_name)
        logger.info('Done')
    except Exception as e:
        exc_tb = sys.exc_info()[2]
        logger.error('Got {} exception in line {}.'.format(
                                repr(e), exc_tb.tb_lineno))
        logger.info('Terminating the cluster')
        delete_initiated = blocking_kill_cluster(cluster_name)
        logger.info('Done')
