import logging
import os

import boto3


s3 = boto3.resource(
    's3',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
)


class S3DownloadLogger(object):
    def __init__(self, file_size, filename):
        self._filename = filename
        self._size = file_size
        self._seen_so_far = 0
        self._seen_percentages = dict.fromkeys([25, 50, 75], False)  # Define intervals here.

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        percentage = round((self._seen_so_far / self._size) * 100)
        if percentage in self._seen_percentages.keys() and not self._seen_percentages[percentage]:
            self._seen_percentages[percentage] = True
            logging.info(f"Download progress for '{self._filename}': {percentage}%")


def download_file_from_s3(bucket, key, download_path):
    remote_file = s3.Bucket(bucket).Object(key)
    logging.info(f"Starting download for '{remote_file.key}'")
    download_logger = S3DownloadLogger(remote_file.content_length, remote_file.key)
    remote_file.download_file(download_path, Callback=download_logger)
    logging.info(f"Finished download for '{remote_file.key}'")


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    download_file_from_s3('logging-example', 'example-file.mp4', '/tmp/downloaded-file.mp4')
