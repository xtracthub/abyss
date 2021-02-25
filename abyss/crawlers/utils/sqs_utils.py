import json
import boto3
from typing import Dict, List


def create_sqs_connection(aws_access: str, aws_secret: str,
                          region_name: str):
    """Creates SQS queue.

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

