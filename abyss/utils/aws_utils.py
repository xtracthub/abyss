import json
import os
from configparser import ConfigParser
from typing import Dict, List

import boto3
from flask import Flask


PROJECT_ROOT = os.path.realpath(os.path.dirname(__file__)) + "/"


def read_aws_config_file(config_file=os.path.join(PROJECT_ROOT,
                                                 "sqs.ini"),
                         section="sqs") -> dict:
    """Reads AWS credentials from a .ini file.

    Parameters
    ----------
    config_file : str
        .ini config file to read database configuration from.
    section : str
        Section in .ini file to read credentials from.

    Returns
    -------
    credentials : dict
        Dictionary with credentials.
    """
    parser = ConfigParser()
    parser.read(config_file)

    credentials = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            credentials[param[0]] = param[1]

    else:
        raise Exception(
            f"Section {section} not found in the {config_file} file")

    return credentials


def read_flask_aws_config(app: Flask) -> dict:
    """Reads AWS credentials from a Flask app configuration.

    Parameters
    ----------
    app : Flask
        Flask app to read database configuration from.

    Returns
    -------
    credentials : dict
        Dictionary with credentials.
    """
    credentials = dict()

    credentials["aws_access"] = app.config.get("AWS_ACCESS")
    credentials["aws_secret"] = app.config.get("AWS_SECRET")
    credentials["aws_region"] = app.config.get("AWS_REGION")

    return credentials


def create_sqs_connection(aws_access: str, aws_secret: str,
                          region_name: str):
    """Creates connection to SQS.

    Parameters
    ----------
    aws_access : str
        AWS access key ID.
    aws_secret : str
        AWS secret access key.
    region_name : str
        Region that SQS queue is located in.

    Returns
    -------
        boto3 SQS resource.
    """
    sqs = boto3.resource("sqs",
                         aws_access_key_id=aws_access,
                         aws_secret_access_key=aws_secret,
                         region_name=region_name)

    return sqs


def make_queue(sqs_conn, queue_name: str):
    """Creates SQS queue.

    Parameters
    ----------
    sqs_conn
        boto3 SQS resource.
    queue_name : str
        Name of queue to create.

    Returns
    -------
    str
        URL of created queue.
    """
    queue = sqs_conn.create_queue(QueueName=queue_name)
    return queue.url


def get_message(sqs_conn, queue_name: str, max_messages=1) -> List[Dict]:
    """Receives messages from an SQS queue.

    Parameters
    ----------
    sqs_conn
        boto3 SQS resource.
    queue_name : str
        Name of queue to create.
    max_messages : int
        Max number of messages to pull from SQS.

    Returns
    -------
    messages : list(dict)
        Messages pulled from SQS.
    """
    queue = sqs_conn.get_queue_by_name(QueueName=queue_name)
    responses = queue.receive_messages(MaxNumberOfMessages=max_messages)

    messages = []

    if len(responses) > 0:
        for response in responses:
            message = json.loads(response.body)
            messages.append(message)
            response.delete()

    return messages


def put_message(sqs_conn, message: Dict, queue_name: str):
    """Places message onto SQS queue.

    Parameters
    ----------
    sqs_conn
        boto3 SQS resource.
    message : dict
        Message to place onto queue.
    queue_name : str
        Name of queue to place messages on.

    Returns
    -------
    response
        Response returned by SQS.
    """
    message = json.dumps(message)
    queue = sqs_conn.get_queue_by_name(QueueName=queue_name)
    response = queue.send_message(MessageBody=message)

    return response


def put_messages(sqs_conn, messages: List[Dict], queue_name: str):
    """Places batch of messages onto SQS queue.

    Parameters
    ----------
    sqs_conn
        boto3 SQS resource.
    messages : list(dict)
        Messages to place onto queue.
    queue_name : str
        Name of queue to place messages on.

    Returns
    -------
    response
        Response returned by SQS.
    """
    sqs_messages = []
    for idx, message in enumerate(messages):
        if idx >= 10:
            raise ValueError(f"Can not batch more than 10 messages onto SQS queue")

        sqs_messages.append({"Id": str(idx),
                             "MessageBody": json.dumps(message)})

    queue = sqs_conn.get_queue_by_name(QueueName=queue_name)
    response = queue.send_messages(Entries=sqs_messages)

    return response


def create_s3_connection(aws_access: str, aws_secret: str,
                         region_name: str):
    """Creates connection to S3.

    Parameters
    ----------
    aws_access : str
        AWS access key ID.
    aws_secret : str
        AWS secret access key.
    region_name : str
        Region for S3.

    Returns
    -------
        boto3 S3 resource.
    """
    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access,
                      aws_secret_access_key=aws_secret,
                      region_name=region_name)

    return s3


def s3_upload_file(s3_conn, s3_bucket: str,
                   file_path: str, s3_file_path: str):
    """Uploads a file to an s3 bucket.

    Parameters
    ----------
    s3_conn
        Connection client to S3.
    s3_bucket : str
        S3 bucket to upload to.
    file_path : str
        File path to upload to S3.
    s3_file_path : str
        Path on S3 to upload to.

    Returns
    -------

    """
    with open(file_path, "rb") as f:
        s3_conn.upload_fileobj(f, s3_bucket, s3_file_path)


