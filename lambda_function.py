import json
import os
from typing import Dict, Optional, Tuple, Any
import boto3
import logging
from elasticsearch import Elasticsearch

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

es = Elasticsearch(
    os.getenv("host"), http_auth=(os.getenv("userName"), os.getenv("password"))
)
dynamodb_client = boto3.client("dynamodb")


def get_message_details(event: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    message_id: str = event["Records"][0]["Sns"]["MessageId"]
    sns_message: str = event["Records"][0]["Sns"]["Message"]
    sns_message_json: Dict[str, Any] = json.loads(sns_message)
    return message_id, sns_message_json


def check_and_add_event_id(tableName: Optional[str], message_id: str) -> None:
    if tableName is not None:
        response: Dict[str, Any] = dynamodb_client.get_item(
            TableName=tableName, Key={"eventId": {"S": message_id}}
        )
        if "Items" not in response:
            dynamodb_client.put_item(
                TableName=tableName, Item={"eventId": {"S": message_id}}
            )
    else:
        logger.error("Table name is not defined")
        raise Exception("Table name is not defined")


def index_to_elasticsearch(
    index_name: Optional[str], id: str, body: Dict[str, Any]
) -> None:
    if index_name is not None:
        es.index(index=index_name, id=id, body=body)  # type: ignore[call-arg]
    else:
        logger.error("Index name is not defined")
        raise Exception("Index name is not defined")


def handler(event: Dict[str, Any], context: Dict[str, Any]) -> None:
    try:
        message_id, sns_message_json = get_message_details(event)
        check_and_add_event_id(os.getenv("hotelCreatedEventIdsTable"), message_id)
        index_to_elasticsearch(
            os.getenv("indexName"), sns_message_json["Id"], sns_message_json
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e
