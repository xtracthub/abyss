import json
import boto3
from typing import Dict, List


def get_message(queue_name, aws_access, aws_secret, region_name,
                max_messages=1) -> List[Dict]:
    """Receives messages from an SQS queue.

    Parameters
    ----------
    queue_name : str
        Name of queue to receive messages from.
    aws_access : str
        AWS access key ID.
    aws_secret : str
        AWS secret access key.
    region_name : str
        Region that SQS queue is located in.
    max_messages : int
        Max number of messages to pull from SQS.

    Returns
    -------
    messages : list(dict)
        Messages pulled from SQS.
    """
    sqs = boto3.resource("sqs",
                         aws_access_key_id=aws_access,
                         aws_secret_access_key=aws_secret,
                         region_name=region_name)
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    responses = queue.receive_messages(MaxNumberOfMessages=max_messages)

    messages = []

    if len(responses) > 0:
        for response in responses:
            message = json.loads(response.body)
            messages.append(message)
            response.delete()

    return messages


def put_message(message: Dict, queue_name: str, aws_access: str,
                aws_secret: str, region_name: str):
    """Places message onto SQS queue.

    Parameters
    ----------
    message : dict
        Message to place onto queue.
    queue_name : str
        Name of queue to place messages on.
    aws_access : str
        AWS access key ID.
    aws_secret : str
        AWS secret access key.
    region_name : str
        Region that SQS queue is located in.

    Returns
    -------
    response
        Response returned by SQS.
    """
    message = json.dumps(message)
    sqs = boto3.resource("sqs",
                         aws_access_key_id=aws_access,
                         aws_secret_access_key=aws_secret,
                         region_name=region_name)
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    response = queue.send_message(MessageBody=message)

    return response
