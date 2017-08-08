import asyncio
import json
import logging
import time
import uuid

import copy
from google.cloud import bigquery
from google.cloud.bigquery.job import DestinationFormat, WriteDisposition, CreateDisposition
from google.cloud.exceptions import NotFound, Conflict

from gcp_tools.gcs_tools import get_bucket
from gcp_tools.file_utils import strip_extension
from gcp_tools.filters import filter_table_name
from gcp_tools.error_handling import retry_on_error

logger = logging.getLogger(__name__)


def execute_export_job(bq_project_id, bq_table, gcs_project_id, gcs_bucket_name, gcs_filename,
                       destination_format=DestinationFormat.CSV):
    bucket = get_bucket(gcs_project_id, gcs_bucket_name)
    if '*' not in gcs_filename:
        gcs_filename = strip_extension(gcs_filename, ['json', 'csv', 'gz', 'avro'])
        if destination_format == DestinationFormat.CSV:
            gcs_uri = 'gs://{}/{}_*.csv.gz'.format(bucket.name, gcs_filename)
        elif destination_format == DestinationFormat.NEWLINE_DELIMITED_JSON:
            gcs_uri = 'gs://{}/{}_*.json.gz'.format(bucket.name, gcs_filename)
        elif destination_format == DestinationFormat.AVRO:
            gcs_uri = 'gs://{}/{}_*.avro'.format(bucket.name, gcs_filename)
        else:
            raise ValueError('Unknown destination format: {}'.format(destination_format))
    else:
        gcs_uri = 'gs://{}/{}'.format(bucket.name, gcs_filename)

    bigquery_client = bigquery.Client(project=bq_project_id)

    job_name = str(uuid.uuid4())

    job = bigquery_client.extract_table_to_storage(
        job_name, bq_table, gcs_uri)
    job.destination_format = destination_format
    if destination_format != DestinationFormat.AVRO:
        job.compression = 'GZIP'
    job.begin()

    return job


def get_bq_table(bq_project_id, bq_dataset_id, table_name):
    bigquery_client = bigquery.Client(project=bq_project_id)
    dataset = bigquery_client.dataset(bq_dataset_id)

    table = dataset.table(table_name)

    if table.exists():
        return table
    else:
        raise NotFound('Table {} not found.'.format(table_name))


def get_bq_tables(bq_project_id, bq_dataset_id, bq_table_pattern='.*', only_date_shards=False):
    bigquery_client = bigquery.Client(project=bq_project_id)
    dataset = bigquery_client.dataset(bq_dataset_id)

    def filter_fun(table_name):
        return filter_table_name(table_name, bq_table_pattern, only_date_shards)

    start_time = time.time()
    tables = [table for table in dataset.list_tables() if filter_fun(table.name)]
    logger.debug("Listing bq tables: {} s".format(time.time() - start_time))

    return tables


def load_csvs_from_gcs(gcs_uri, bq_project_id, bq_dataset_id, bq_table_name, bq_schema,
                       bq_create_disposition='CREATE_IF_NEEDED', bq_write_disposition='WRITE_APPEND'):
    bigquery_client = bigquery.Client(project=bq_project_id)

    dataset = bigquery_client.dataset(bq_dataset_id)
    table = dataset.table(bq_table_name, bq_schema)
    if not table.exists():
        table.create()

    job_name = str(uuid.uuid4())

    job = bigquery_client.load_table_from_storage(
        job_name, table, gcs_uri)
    job.create_disposition = bq_create_disposition
    job.write_disposition = bq_write_disposition

    start_time = time.time()
    job.begin()
    wait_for_job(job)
    logger.debug("File(s) {} imported into bq: {} s".format(gcs_uri, time.time() - start_time))


def query_into_table(query, dest_bq_project_id, dest_bq_dataset_id, dest_bq_table_name,
                     bq_write_disposition='WRITE_APPEND', use_legacy_sql=False):
    bigquery_client = bigquery.Client(project=dest_bq_project_id)

    dataset = bigquery_client.dataset(dest_bq_dataset_id)
    table = dataset.table(dest_bq_table_name)

    job_name = str(uuid.uuid4())

    job = bigquery_client.run_async_query(job_name, query)
    job.use_legacy_sql = use_legacy_sql
    job.destination = table

    job.write_disposition = bq_write_disposition

    start_time = time.time()
    job.begin()
    wait_for_job(job)
    logger.debug("Query job {} finished: {} s".format(job_name, time.time() - start_time))


def wait_for_job(job):
    while True:
        job_reloader(job)
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


def execute_sync_query(project_id, query_str):
    bq_client = bigquery.Client(project_id)
    query = bq_client.run_sync_query(query_str)
    query.use_legacy_sql = False
    run_query(query)

    if not query.complete:
        job = query.job
        wait_for_job(job)

    def result_generator(query, token=None):
        while True:
            rows, total_count, token = fetch_query_results(query, token)
            yield rows
            if token is None:
                break

    result = []
    for rows in result_generator(query):
        result.extend(rows)

    return result


def start_batch_query(client, query, dest_table,
                      create_dispostion=CreateDisposition.CREATE_IF_NEEDED,
                      write_disposition=WriteDisposition.WRITE_TRUNCATE, use_legacy_sql=False):
    job_name = dest_table.name.replace('$', '-DOLLAR-') + '_' + str(uuid.uuid4())
    job = client.run_async_query(job_name, query)
    job.destination = dest_table
    job.priority = 'BATCH'
    job.create_disposition = create_dispostion
    job.write_disposition = write_disposition
    job.use_legacy_sql = use_legacy_sql
    logger.debug('Submitting job {}'.format(job_name))
    job_starter(job)

    return job_name


def execute_async_jobs(job_list, num_workers=50, poll_period=5, description='', completion_callback=None):
    description = description or uuid.uuid4()
    logger.debug('Job list: {} started.'.format(description))
    start_time = time.time()
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()
    q = asyncio.Queue(loop=loop)

    for job in job_list:
        q.put_nowait(job)

    workers = [
        asyncio.ensure_future(
            run_async_worker(q, poll_period=poll_period, completion_callback=completion_callback),
            loop=loop)
        for _ in range(num_workers)]

    loop.run_until_complete(asyncio.wait(workers))
    loop.close()
    logger.debug('Job list: {} finished: {} s'.format(description, time.time() - start_time))


async def run_async_worker(work_queue, poll_period, completion_callback):
    if completion_callback is None:
        def completion_callback(_): pass

    while not work_queue.empty():
        queue_item = await work_queue.get()

        if len(queue_item) == 1:
            job, job_description = queue_item, uuid.uuid4()
        else:
            job, job_description = queue_item

        start_time = time.time()
        job_starter(job)
        logger.debug('Job: {} started.'.format(job_description))

        while not job.state.lower() == 'done' and job.state:
            await asyncio.sleep(poll_period)
            job_reloader(job)

        if job.error_result is None:
            completion_callback(job_description)
            logger.debug('Job: {} finished: {} s'.format(job_description, time.time() - start_time))
        else:
            logger.debug('Job: {} finished with error {}: {} s'.format(job_description, job.error_result,
                                                                       time.time() - start_time))


@retry_on_error(number_of_retries=4, init_wait=5)
def job_starter(job):
    try:
        job.begin()
    except Conflict as e:
        if not e.message.startswith('Already Exists'):
            raise Conflict(e.message)
        else:
            # do nothing, job already started
            job_tmp = copy.deepcopy(job)
            job_reloader(job)
            if job.query != job_tmp.query or \
                    (job.destination is None and job_tmp.destination is not None) or \
                    (job.destination is not None and job_tmp.destination is None) or \
                    ((job.destination is not None and job_tmp.destination is not None) and
                         (job.destination.project != job_tmp.destination.project or
                                  job.destination.dataset_name != job_tmp.destination.dataset_name or
                                  job.destination.name != job_tmp.destination.name)):
                raise ValueError("Job {} already exists but with different properties than: "
                                 "query ({}), dest. project ({}), dest. dataset ({}), dest. name ({})".
                                 format(job_tmp.name,
                                        job_tmp.query,
                                        job_tmp.destination.project if job_tmp.destination is not None else None,
                                        job_tmp.destination.dataset_name if job_tmp.destination is not None else None,
                                        job_tmp.destination.name if job_tmp.destination is not None else None))


@retry_on_error(number_of_retries=4, init_wait=10)
def job_reloader(job):
    job.reload()


def get_table_schema_from_json(json_file_name):
    with open(json_file_name, 'r') as ifile:
        schema_dict = json.load(ifile)

    return get_table_schema_from_json_str(schema_dict['schema']['fields'])


def get_table_schema_from_json_str(schema_list):
    schema = []
    for field in schema_list:
        if field['type'] in ('RECORD', 'STRUCT'):
            fields = get_table_schema_from_json_str(field['fields'])
            schema_field = bigquery.SchemaField(name=field['name'], field_type=field['type'], mode=field['mode'],
                                                fields=fields)
        else:
            schema_field = bigquery.SchemaField(name=field['name'], field_type=field['type'], mode=field['mode'])
        schema.append(schema_field)
    return schema


def create_partitioned_table(bq_client, dataset_id, table_name, schema):
    dataset = bq_client.dataset(dataset_id)
    new_table = dataset.table(table_name)

    if check_table_exists(new_table):
        reload_table(new_table)

        assert schema_dict_creator(new_table.schema) == schema_dict_creator(schema), \
            'Table {} already exists with mismatched schema'.format(new_table.name)
        assert new_table.partitioning_type == 'DAY', \
            'Partitioning for table {} not set up correctly'.format(new_table.name)
    else:
        new_table.schema = schema
        new_table.partitioning_type = 'DAY'
        create_new_table(new_table)

    return new_table


def schema_dict_creator(schema):
    schema_dict = {}
    type_dict = {'INTEGER': 'INT64', 'FLOAT': 'FLOAT64', 'RECORD': 'STRUCT'}
    for schema_field in schema:
        if schema_field.field_type in type_dict:
            type = type_dict[schema_field.field_type]
        else:
            type = schema_field.field_type
        if type == 'STRUCT':
            fields = schema_dict_creator(schema_field.fields)
        else:
            fields = None
        schema_dict[schema_field.name] = (type, schema_field.mode, fields)
    return schema_dict


@retry_on_error()
def check_table_exists(table):
    return table.exists()


@retry_on_error()
def reload_table(table):
    table.reload()


@retry_on_error()
def update_table(table):
    table.update()


@retry_on_error()
def delete_table(table):
    table.delete()


@retry_on_error()
def create_new_table(table):
    table.create()


@retry_on_error()
def fetch_query_results(query, token=None):
    return query.fetch_data(page_token=token)


@retry_on_error()
def run_query(query):
    query.run()
