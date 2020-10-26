# encoding: utf-8

import logging
import sqlparse
import pyathena
from pyathena.async_cursor import AsyncCursor
import pymysql
import os
import signal
import click
import time
import requests
import json
import boto3

from athenacli.packages import special
import re

logger = logging.getLogger(__name__)

def keyboardInterruptHandler(signal, frame):
    raise KeyboardInterrupt

signal.signal(signal.SIGINT, keyboardInterruptHandler)

def is_update_query(sql):
    """
    create table이나 drop table같은 udpate성 쿼리를 선별한다.
    """
    create_stmt = re.match("\s*create\s+(external\s+|)table\s+.*", sql, re.IGNORECASE)
    drop_stat = re.match("\s*drop\s+table\s+.*", sql, re.IGNORECASE)
    insert_stmt = re.match("\s*insert\s+(into|overwrite)\s+.*", sql, re.IGNORECASE)
    alter_stmt = re.match("\s*alter\s+(table|database)\s+.*", sql, re.IGNORECASE)
    if all([create_stmt==None,drop_stat==None,insert_stmt==None,alter_stmt==None]) == True:
        return False
    return True

def get_parameter_value(Name, default=None, WithDecryption=False):
    try:
        session = boto3.Session(region_name='ap-northeast-2')
        ssm = session.client('ssm')
        return ssm.get_parameter(Name=Name, WithDecryption=WithDecryption)['Parameter']['Value']
    except:
        return default

parameter_store_name_path = os.getenv('PARAMETER_STORE_NAME_PATH', '/skinet/key-value')
QUERY_COST_SERVICE_URL = os.getenv('QUERY_COST_SERVICE_URL', 'None')
QUERY_SERVICE_URL = get_parameter_value(parameter_store_name_path + "/apigateway/skn_athena")

class SQLExecute(object):
    DATABASES_QUERY = 'SHOW DATABASES'
    TABLES_QUERY = 'SHOW TABLES'
    TABLE_COLUMNS_QUERY = '''
        SELECT table_name, column_name FROM information_schema.columns
        WHERE table_schema = '%s'
        ORDER BY table_name, ordinal_position
    '''

    def __init__(
        self,
        aws_access_key_id,
        aws_secret_access_key,
        aws_session_token,
        region_name,
        s3_staging_dir,
        database,
        query_db_info = None
    ):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.region_name = region_name
        self.s3_staging_dir = get_parameter_value(parameter_store_name_path + '/athena/target_output', s3_staging_dir)
        self.database = database

        self.connect()
    #    self.query_db_connect(query_db_info)

    def connect(self, database=None):
        conn = pyathena.connect(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            region_name=self.region_name,
            s3_staging_dir=self.s3_staging_dir,
            schema_name=database or self.database,
            poll_interval=0.2 # 200ms
        )
        self.database = database or self.database

        if hasattr(self, 'conn'):
            self.conn.close()
        self.conn = conn

    #def query_db_connect(self, query_db_info):
    #    if hasattr(self, 'query_db_conn'):
    #        self.query_db_conn.close()
    #    if query_db_info == None:
    #        self.query_db_conn = None
    #        return
    #    db_conn = pymysql.connect(
    #              host     = query_db_info['host'],
    #              port     = int(query_db_info['port']),
    #              user     = query_db_info['user'],
    #              password = query_db_info['password'],
    #              db       = query_db_info['db'],
    #              charset  = query_db_info['charset']
    #              )
    #    self.query_db_conn = db_conn

    #def insert_query_db_old(self, user, query_id, query, state, state_change_reason, output_path, scan_size, running_cost, running_time, mod_date, reg_date):
    #    if self.query_db_conn == None:
    #        logger.debug('query_db_conn is None')
    #        return
    #    try:
    #        run_query = '''insert into athena_query(user, query_id, query, state, state_change_reason, output_path, scan_size, running_cost, running_time, mod_date, reg_date)
    #                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''' 
    #        # % (user, query_id, query, state, state_change_reason, output_path, scan_size, running_cost, running_time,mod_date, reg_date)
    #        logger.debug(query)
    #        with self.query_db_conn.cursor(pymysql.cursors.DictCursor) as curs:
    #            rs = curs.execute(run_query, (user, query_id, query, state, state_change_reason, output_path, scan_size, running_cost, running_time, mod_date, reg_date))
    #            self.query_db_conn.commit()
    #    except pymysql.InternalError as error:
    #        code, message = error.args
    #        logger.debug("pymysql error: {}, {}".format(code, message))
    #    return rs

    def insert_query_db(self, user, query_id, query, state, state_change_reason, output_path, scan_size, running_cost, running_time, mod_date, reg_date):
        headers = {"Content-Type" : "application/json"}
        query_db_data = {
            "user_id": user,
            "query_id": query_id,
            "query": query, 
            "state": state,
            "state_change_reason": state_change_reason,
            "output_path": output_path,
            "scan_size": int(scan_size),
            "running_cost": float(running_cost),
            "running_time": float(running_time) 
        }
        query_db_res = json.loads(requests.post( QUERY_SERVICE_URL+"/put_query_log", json=query_db_data, headers=headers ).text)
        if (query_db_res is not None) and ('result' in query_db_res) and (query_db_res['result'] == 'success'):
            click.echo("query history insert success", err=True)
            rs = 0
        else:
            click.echo("query history insert fail", err=True)
            rs = -1
        return rs


    def AthenaSendUpdate(self, qry):
        user_id = os.getenv('user_id')
        if user_id == None:
            click.echo("query Failed, plaese set user_id", err=True)
            return (0, 0)
        headers = {"Content-Type": "application/json"}
        try:
            data = {'user_id': user_id, 'database': self.database, 'query': qry}
            appData = json.loads(requests.post( self.query_service+"/run_query", json=data, headers=headers).text)

            if appData['result'] != "success":
                click.echo("query failed, result is not success", err=True)
                click.echo(appData['data']['query_id'], err=True)
                return (0, 0)

            while(True):
                data2 = {'user_id': user_id, 'query_id': appData['data']['query_id']}
                query_stat = json.loads(requests.post( self.query_service+"/get_query_status", json=data2, headers=headers).text)

                if query_stat['data']['state'] == "SUCCEEDED":
                    click.echo("query succeeded", err=True)
                    break

                if query_stat['data']['state'] == "FAILED":
                    click.echo("query failed!", err=True)
                    click.echo(query_stat['data']['state_change_reason'], err=True)
                    return (0, 0)
                click.echo(".", err=True, nl=False)
                time.sleep(1)
        except KeyboardInterrupt:
            data3 = {'user_id': user_id, 'query_id': appData['data']['query_id']}
            stop_query = json.loads(requests.post( self.query_service+"/stop_query", json=data3, headers=headers).text)
            if stop_query['result'] == 'success':
                click.echo("Keboard Interrupt. Query " + stop_query['data']['state'], err=True)
                return (stop_query['data']['running_time'], stop_query['data']['scan_size'])
            else:
                click.echo("cancel query failed", err=True)
                return (0, 0)
        except:
            traceback.print_exc()
        return (query_stat['data']['running_time'], query_stat['data']['scan_size'])


    def run(self, statement, is_part=True):
        '''Execute the sql in the database and return the results.

        The results are a list of tuples. Each tuple has 4 values
        (title, rows, headers, status).
        '''
        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            yield (None, None, None, None, None, None)

        # Split the sql into separate queries and run each one.
        components = sqlparse.split(statement)
        headers = {"Content-Type" : "application/json"}

        for sql in components:
            # Remove spaces, eol and semi-colons.
            sql = sql.rstrip(';')

            # \G is treated specially since we have to set the expanded output.
            if sql.endswith('\\G'):
                special.set_expanded_output(True)
                sql = sql[:-2].strip()

            cur = self.conn.cursor()

            try:
                for result in special.execute(cur, sql):
                    res_info = self.get_info(cur._query_id)
                    yield result + res_info
            except special.CommandNotFound:  # Regular SQL
                query_est_data = { "query": sql }
                if QUERY_COST_SERVICE_URL not in ['None', 'TODO']:
                    query_est_res = json.loads(requests.post( QUERY_COST_SERVICE_URL+"/prediction", json=query_est_data, headers=headers ).text)
                    if (query_est_res is not None) and ('status' in query_est_res) and (query_est_res['status'] == 200):
                        if query_est_res['result']['prediction'] == 'Low':
                            click.echo("estimated query cost is {}".format(query_est_res['result']['prediction']), err=True)
                        else:
                            click.secho("estimated query cost is {}".format(query_est_res['result']['prediction']), err=True, fg='red')
                    else:
                        click.echo("Can't estimate query cost...", err=True)
                else:
                    click.echo("Can't service query cost... please check QUERY_COST_SERVICE_URL", err=True)
                try:
                    if is_update_query(sql) == True:
                        res_info = self.AthenaSendUpdate(sql)
                        if res_info != None:
                            res_result = (None, None, None, "Success")
                    else:
                        cur = self.conn.cursor(AsyncCursor) # add, for cancel query
                        query_id, future = cur.execute(sql)
                        res_result = self.get_result(query_id, future, is_part)
                        res_info = self.get_info(query_id)
                except Exception as e:
                    res_result = (None, None, None, None)
                    res_info = (0, 0)
                yield res_result + res_info


    def get_info(self, query_id):
        if query_id == None:
            return (0, 0)
        stats = self.conn._client.get_query_execution(QueryExecutionId=query_id)
        logger.debug(stats)
        user = os.getenv('user_id', 'None')
        query_id = stats['QueryExecution']['QueryExecutionId']
        query = stats['QueryExecution']['Query']
        state = stats['QueryExecution']['Status']['State']
        if state != 'SUCCEEDED':
            return (0, 0)
        state_change_reason = ''
        output_path = stats['QueryExecution']['ResultConfiguration']['OutputLocation']
        execution_time = stats['QueryExecution']['Statistics']['EngineExecutionTimeInMillis'] / 1000.0
        scanned_data = stats['QueryExecution']['Statistics']['DataScannedInBytes']
        running_cost = scanned_data / 1000000000000.0 * 5.0
        mod_date = stats['QueryExecution']['Status']['CompletionDateTime']
        reg_date = stats['QueryExecution']['Status']['SubmissionDateTime'] 
        self.insert_query_db(user, query_id, query, state, state_change_reason, output_path, scanned_data, running_cost, execution_time, mod_date, reg_date)
        return (execution_time, scanned_data)

    def get_result(self, query_id, future, is_part=True):
        '''Get the current result's data from the cursor.'''
        title = headers = None

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT or SHOW.
        try:
            while future.running():
                click.echo(".", err=True, nl=False)
                time.sleep(1)
            result_set = future.result()
            logger.debug(result_set.state)
            logger.debug(result_set.state_change_reason)
            logger.debug(result_set.completion_date_time)
            logger.debug(result_set.submission_date_time)
            logger.debug(result_set.data_scanned_in_bytes)
            logger.debug(result_set.execution_time_in_millis)
            logger.debug(result_set.output_location)
            logger.debug(result_set.description) 
            if (result_set.state == 'SUCCEEDED') and (result_set.description is not None):
                headers = [x[0] for x in result_set.description]
                click.secho("\n[Download] aws s3 cp " + result_set.output_location + " .", err=True, fg='cyan')
                if is_part == True:
                    rows = result_set.fetchmany()
                else:
                    #rows = result_set.fetchall()
                    rows = []
                    for i, row in enumerate(result_set):
                        rows.append(row)
                        if i % 10000 == 0:
                            click.echo("*", err=True, nl=False)
                    click.secho("\n", err=True, fg='cyan')
                status = '%d row%s in display. Max is 1000' % (len(rows), '' if len(rows) == 1 else 's')
            else:
                click.echo("", err=True);
                click.secho(result_set.state_change_reason, err=True, fg='red')
                rows = None
                status = None # result_set.state_change_reason 
            return (title, rows, headers, status)
        except KeyboardInterrupt:
            cur = self.conn.cursor(AsyncCursor)
            cur.cancel(query_id)
            click.secho('Keyboard Interrupt', err=True, fg='red')
            return (None, None, None, None)

    def tables(self):
        '''Yields table names.'''
        with self.conn.cursor() as cur:
            cur.execute(self.TABLES_QUERY)
            for row in cur:
                yield row

    def table_columns(self):
        '''Yields column names.'''
        with self.conn.cursor() as cur:
            cur.execute(self.TABLE_COLUMNS_QUERY % self.database)
            for row in cur:
                yield row

    def databases(self):
        with self.conn.cursor() as cur:
            cur.execute(self.DATABASES_QUERY)
            return [x[0] for x in cur.fetchall()]
