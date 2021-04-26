'''
This script borrows nearly exactly from code provided by David Bruce Borenstein at Applied Nonprofit Research.
Information about the script as well as the original code can be found at: https://appliednonprofitresearch.com/posts/2020/06/skip-the-irs-990-efile-indices/
I do not have David Bruce Borenstein's expressed permission to post this script publicly, but assume this is in line with the explicit intentions of the blog post.
If citing this script please cite him.
'''

import time
import datetime
from collections import deque
from typing import List, Deque, Iterable, Dict
import logging
import boto3
from botocore.config import Config
from botocore import UNSIGNED
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed, Future
import pandas as pd

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

BUCKET = "irs-form-990"
EARLIEST_YEAR = 2015
cur_year = datetime.datetime.now().year

first_prefix = EARLIEST_YEAR * 100
last_prefix = (cur_year + 1) * 100

def get_keys_for_prefix(prefix):

    my_config = Config( region_name = 'us-east-1', signature_version=UNSIGNED )
    client = boto3.client('s3', config=my_config)
    
    # See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

	# A deque is a collection with O(1) appends and O(n) iteration
    results = deque()
    i = 0
    for i, page in enumerate(page_iterator):
        if "Contents" not in page:
            continue
        
        # You could also capture, e.g., the timestamp or checksum here
        page_keys = (element["Key"] for element in page["Contents"])
        results.extend(page_keys)
    logging.info("Scanned {} page(s) with prefix {}.".format(i+1, prefix))
    return results

if __name__ == '__main__':

    start = time.time()

    # ProcessPoolExecutor starts a completely separate copy of Python for each worker
    with ProcessPoolExecutor() as executor:
        futures = deque()
        for prefix in range(first_prefix, last_prefix):
            future = executor.submit(get_keys_for_prefix, str(prefix))
            futures.append(future)
    
    n = 0
    file_lst = []

    # as_completed ignores submission order to prevent unnecessary waiting
    for future in as_completed(futures):
        keys = future.result()
        for key in keys:
            file_lst.append( str( key ) )
            n += 1

    with open( 'file_list.csv', 'w' ) as f:
        f.write( "file_name\n" )
        for filename in file_lst:
            f.write( filename + '\n' )

    elapsed = time.time() - start
    logging.info("Discovered {:,} keys in {:,.1f} seconds.".format(n, elapsed))

