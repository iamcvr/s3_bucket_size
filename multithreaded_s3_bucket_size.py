import boto3
import concurrent.futures
from botocore.config import Config

#vars
bucket_name = 'BUCKET_NAME_HERE'
#prefix needs to end with: /
prefix_name = 'PREFIX_IF_YOU_WANT'

s3 = boto3.client('s3')
s3_config = Config(read_timeout=10,connect_timeout=10,retries={"max_attempts": 10,'mode': 'adaptive'})
s3_client = boto3.client('s3', config=s3_config)
paginator = s3_client.get_paginator('list_objects_v2')
pages_to_scan = paginator.paginate(Bucket=bucket_name, Prefix=prefix_name, Delimiter="/")
futures = []
total_size = 0

def humansize(nbytes):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    i = 0
    while nbytes >= 1000 and i < len(suffixes)-1:
        nbytes /= 1000.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])


def item_scanner(page):
    scanned_size = 0
    if 'Contents' in page:
        for item in page['Contents']:
            if 'Size' in item:
                scanned_size += item['Size']

    return scanned_size

def pool_executor(pages_to_scan):
    global total_size
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for page in pages_to_scan:
            futures.append(executor.submit(item_scanner, page))
            if 'CommonPrefixes' in page:
                for item in page['CommonPrefixes']:
                    prefix_name = item['Prefix']
                    pages_to_scan = paginator.paginate(Bucket=bucket_name, Prefix=prefix_name, Delimiter="/")
                    pool_executor(pages_to_scan)
        results = [f.result() for f in futures]
    return results

results = pool_executor(pages_to_scan)
for item in results:
        total_size += item
print(results)
print(f"Total Bucket Size: {humansize(total_size)}")