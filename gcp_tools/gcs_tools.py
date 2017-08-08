import os
import time
from google.cloud import storage

import logging

from gcp_tools.error_handling import retry_on_error

logger = logging.getLogger(__name__)


@retry_on_error()
def upload_file_to_gcs(file_path, gcs_project_id, gcs_bucket_name, gcs_file_name=None):
    if not gcs_file_name:
        gcs_file_name = os.path.basename(file_path)

    bucket = get_bucket(gcs_project_id, gcs_bucket_name)

    start_time = time.time()
    blob = storage.Blob(gcs_file_name, bucket)
    blob.upload_from_filename(file_path)
    logger.debug("File {} uploaded to gcs: {} s".format(file_path, time.time() - start_time))


def list_blobs_in_bucket(gcs_project_id, gcs_bucket_name, prefix=''):
    logger.debug('Listing blobs')
    storage_client = storage.Client(project=gcs_project_id)
    if storage_client.lookup_bucket(gcs_bucket_name):
        bucket = storage_client.get_bucket(gcs_bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))
        logger.debug('Blobs listed: {}'.format([blob.name for blob in blobs]))
        return blobs
    else:
        raise ValueError('Bucket not found: {}'.format(gcs_bucket_name))


@retry_on_error()
def get_bucket(gcs_project_id, gcs_bucket_name):
    storage_client = storage.Client(project=gcs_project_id)

    bucket = storage_client.lookup_bucket(gcs_bucket_name)
    if not bucket:
        bucket = storage_client.bucket(gcs_bucket_name)
        bucket.location = 'EUROPE-WEST1'
        bucket.storage_class = 'REGIONAL'
        bucket.create()

    return bucket


def get_blob(gcs_project_id, gcs_bucket_name, gcs_blob_name):
    storage_client = storage.Client(project=gcs_project_id)

    if storage_client.lookup_bucket(gcs_bucket_name):
        bucket = storage_client.get_bucket(gcs_bucket_name)
        blob = bucket.get_blob(gcs_blob_name)
        if blob:
            return blob
        else:
            raise ValueError('Blob not found: {}'.format(gcs_blob_name))
    else:
        raise ValueError('Bucket not found: {}'.format(gcs_bucket_name))


def download_blob(blob, local_output_path):
    blob_file_name = blob.name.split('/')[-1]
    output_file_name = os.path.join(local_output_path, blob_file_name)

    logger.debug('Downloading blob {}'.format(blob.name))
    blob.download_to_filename(output_file_name)
    logger.debug('Download complete {}'.format(blob.name))

    return blob_file_name, output_file_name


def move_to_gcs(blob_dir, file_name_w_path, project_id, target_bucket):
    logger.debug('Uploading file {}'.format(file_name_w_path))
    upload_file_to_gcs(file_name_w_path, project_id, target_bucket,
                                 '{}/{}'.format(blob_dir, os.path.basename(file_name_w_path)))
    logger.debug('Upload complete {}, removing file'.format(file_name_w_path))
    os.remove(file_name_w_path)
