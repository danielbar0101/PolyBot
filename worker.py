import json
import time
import boto3
import botocore
from loguru import logger
from utils import search_download_youtube_video


def process_msg(msg):
    search_download_youtube_video(msg)

    # TODO upload the downloaded video to your S3 bucket
    s3 = boto3.resource('s3')
    BUCKET = "firstaccesspoint-rnxo4y7g6jia1k6ki81yrdj1z5ccoeuw2a-s3alias"

    s3.Bucket(BUCKET).upload_file(process_msg(), "dump/file")

def main():
    while True:
        try:
            messages = queue.receive_messages(
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )
            for msg in messages:
                logger.info(f'processing message {msg}')
                process_msg(msg.body)

                # delete message from the queue after is was handled
                response = queue.delete_messages(Entries=[{
                    'Id': msg.message_id,
                    'ReceiptHandle': msg.receipt_handle
                }])
                if 'Successful' in response:
                    logger.info(f'msg {msg} has been handled successfully')

        except botocore.exceptions.ClientError as err:
            logger.exception(f"Couldn't receive messages {err}")
            time.sleep(10)


if __name__ == '__main__':
    with open('config.json') as f:
        config = json.load(f)

    sqs = boto3.resource('sqs', region_name=config.get('aws_region'))
    queue = sqs.get_queue_by_name(QueueName=config.get('bot_to_worker_queue_name'))

    main()
