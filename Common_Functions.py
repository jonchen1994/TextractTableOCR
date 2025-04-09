# Last updated 3/14/2025

import boto3
from dotenv import load_dotenv
import os
import shutil


load_dotenv()

def isolate_batch(files_list, orig_dir, target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for f in files_list:
        temp_path = os.path.join(orig_dir,f)
        try:
            shutil.copy(temp_path, target_dir)
        except e as Exception:
            print(f'file {f} had the problem: {e}'})


def get_uploaded_docs(client):
    bucket_name = os.getenv("bucket_name")
    prefix = os.getenv("bucket_path_destination") + "/" + os.getenv("traveler_extension")

    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket = bucket_name, Prefix = prefix)

    franch_list = []
    for page in pages:
        page_list = [c['Key'].split('/')[-1] for c in page['Contents']]
        franch_list.extend(page_list)

    # response = client.list_objects_v2(Bucket = bucket_name, Prefix = prefix, Delimiter = '/')
    return franch_list

def get_docs_in_S3(client, bucket_name, prefix):

    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket = bucket_name, Prefix = prefix)

    franch_list = []
    for page in pages:
        page_list = [c['Key'].split('/')[-1] for c in page['Contents']]
        franch_list.extend(page_list)

    # response = client.list_objects_v2(Bucket = bucket_name, Prefix = prefix, Delimiter = '/')
    return franch_list