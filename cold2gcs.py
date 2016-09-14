#!/usr/bin/python

"""
    File name: cold2gcs.py
    Author: Andrew Hill
    Python Version: 2.7
    Description: This script is defined in indexes.conf and is ran by Splunk just
    before the indexers erase frozen data from an index, as defined by our retention
    policies. If the script returns without errors, the data will be deleted from
    disk. Otherwise the indexer will refuse to index additional data and retain
    the frozen data until the issue is addressed.
"""

import gzip
import logging
import os
import random
import shutil
import subprocess
import sys
from time import strftime

# https://console.cloud.google.com/storage/browser?project=PROJECT_NAME
FROZEN_BUCKET = 'BUCKET_NAME'
LOG_LEVEL = 'INFO'
LOG_LOCATION = '/opt/splunk/var/log/splunk/cold2gcs.log'

def log_and_exit(error_message):
  """Writes error to log and exits with errors"""
  logging.error(error_message)
  sys.exit(error_message)

def enable_logging():
  """Enables custom logging for cold2froezen operation"""
  logging.basicConfig(filename=LOG_LOCATION,level=LOG_LEVEL,format='%(asctime)s %(message)s',datefmt='%m/%d/%Y %I:%M:%S %p')

def get_args():
  """Checks arg count and gets bucket path."""
  # script name and bucket directory
  if len(sys.argv) != 2:
    log_and_exit('usage: python cold2gcs.py <bucket_dir_to_archive>')

  return sys.argv[1]

def get_bucket_files(bucket):
  """Checks filesystem and bucket information."""
  if not os.path.isdir(bucket):
    log_and_exit('Given bucket is not a valid directory: ' + bucket)

  rawdatadir = os.path.join(bucket, 'rawdata')
  if not os.path.isdir(rawdatadir):
    log_and_exit('No rawdata directory, given bucket is likely invalid: ' + bucket) 

  return os.listdir(bucket)

def delete_metadata(bucket, files):
  """Remove all files except for the rawdata. Splunk will rebuild all metadata and tsidx on thaw."""
  logging.info('Archiving bucket: ' + bucket)

  for bucket_file in files:
    full_path = os.path.join(bucket, bucket_file)
    if os.path.isfile(full_path):
      os.remove(full_path)

def get_index_name(bucket):
  """Gets the index name from the bucket path."""
  if bucket.endswith('/'):
    bucket = bucket[:-1]

  # eg. /opt/splunk/var/lib/splunk/INDEX/db/db_12345678_123456789_12
  index_name = os.path.basename(os.path.dirname(os.path.dirname(bucket)))
  return index_name

def create_folder(index_name):
  """Creates index folder if it doesn't exist yet as well as todays date folder."""
  today = strftime("%Y-%m-%d")
  right_now = strftime("%Y-%m-%d_%H:%M:%S")

  # https://cloud.google.com/storage/docs/gsutil/addlhelp/HowSubdirectoriesWork
  init_file = "init_" + right_now

  f = open('/tmp/' + init_file, 'w')
  f.write(right_now + '\n')
  f.close()

  dest_path = 'gs://' + FROZEN_BUCKET + '/' + index_name + '/' + today

  try:
    os.system('gsutil -m cp -n /tmp/' + init_file + ' ' + dest_path + '/init')
  except:
    log_and_exit('Failed to create the destination bucket ' + dest_path)

  logging.info('Folder created: ' + dest_path)
  return dest_path

def move_files(bucket, dest_path):
  """Move the files to GCS."""
  try:
    os.system('gsutil -m cp -r ' + bucket + ' ' + dest_path)
    logging.info('Success: ' + bucket + ' copied to GCS.')
  except:
    log_and_exit('Failed to copy ' + bucket)

def main():
  """Executes the bucket transfer process."""
  enable_logging()

  # prepare to move the files
  bucket = get_args()
  files = get_bucket_files(bucket)
  delete_metadata(bucket, files)
  index_name = get_index_name(bucket)
  dest_path = create_folder(index_name)

  # move the files
  # if we exit with no errors, Splunk will delete the cold bucket
  move_files(bucket, dest_path)
  logging.info('Completed copying ' + bucket + ' to GCS without errors.')

if __name__ == '__main__':
  main()
