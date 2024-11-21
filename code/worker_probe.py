import json
import logging
import os
import sys
from pathlib import Path
from time import sleep

import boto3
import commons
import requests
import settings
from wavinfo import WavInfoReader

logger = commons.log()

if settings.STACK_URL == "":
    logger.error("STACK_URL is not defined")
    sys.exit(1)
if settings.SQS_INGEST == "":
    logger.error("SQS_INGEST is not defined")
    sys.exit(1)

sqs = boto3.resource("sqs", endpoint_url=settings.STACK_URL)
s3 = boto3.client("s3", endpoint_url=settings.STACK_URL)

queue = sqs.get_queue_by_name(QueueName=settings.SQS_INGEST)
logger.info(f"Listening on {queue.url}")

ASSET_BUCKET = "asset"


def probe_wave(file, bucket):
    file_path = Path(file)
    if file_path.suffix == ".wav":
        try:
            probe = WavInfoReader(file)
        except Exception as e:
            logger.error(e)
            return
        logger.info(f"Probe {file_path.name} {probe}")
        # TODO PART IV B test probe.sample_rate and probe.bits_per_sample else return none
        return {
            "file": file_path.name,
            "source_bucket": bucket,
            "bucket": ASSET_BUCKET,
            # TODO PART IV C add probe information
        }


while True:
    logger.info("Looking for messages")
    for messages in queue.receive_messages():
        data = json.loads(messages.body)
        if "Records" in data.keys():
            for message in data["Records"]:
                if "s3" in message.keys():
                    logger.info(f"New S3 message {message}")
                    try:
                        path = f"tmp/{message['s3']['object']['key']}"
                        # TODO PART IV A s3.download_file from ingest bucket to local storage tmp
                        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
                        logger.info(
                            f"bucket: {message['s3']['bucket']['name']}\nobject key: {message['s3']['object']['key']}\nfile destination: {path}"
                        )
                    except Exception as e:
                        logger.error(f"{e} File not downloaded")
                        path = None
                    if path:
                        probe = probe_wave(path, message["s3"]["bucket"]["name"])
                        if probe:
                            logger.info(f"{settings.ASSET_URL}/asset : {probe}")
                            # probe is valid
                            # TODO PART IV D POST probe to asset API
                            # https://www.w3schools.com/python/module_requests.asp
                            # TODO PART IV E s3 copy to ASSET_BUCKET
                        else:
                            # probe is not valid
                            logger.error("Probe failed, file skipped")
                        try:
                            # always remove file in /tmp
                            os.remove(path)
                        except Exception as e:
                            logger.error(f"{e} File not deleted")
        messages.delete()
    sleep(10)
