import base64
import datetime
import decimal
import json
import os
import re
import unicodedata
import uuid

import requests
from google.auth import jwt
from google.cloud import pubsub_v1

"""
This dictionary keeps track of all table names and their corresponding data. 

Format:
{<table_name>: [{<data_1>, <data_2>, ...], ...}}
"""
db_saves = {}
"""
This dictionary keeps track of all secrets injected at runtime.

Format:
{<key>: <value>, ...}
"""
secrets = {}

"""
This dictionary keeps track of all the table names and their corresponding BiqQuery DDL schemas.

Format:
{<key>: <{dict}>, ...}
"""
ddl_schema = {}

"""
This dictionary keeps track of all the key-value pairs defined in the user's code.

Format:
{<key>: <value>, ...}
"""
kv_saves = {}


def set_value(key, value):
    """
    User callable function to set a key-value pair.

    Args:
        key (str): The key to set.
        value (Any): The value to set.
    """
    if not isinstance(key, str):
        raise TypeError("Key must be a string")
    if not isinstance(value, str):
        raise TypeError("Value must be a string")

    global kv_saves
    kv_saves[key] = value


def get_value(key):
    """
    User callable function to get a key-value pair.

    Args:
        key (str): The key to get.

    Returns:
        Any: The value associated with the key.
    """
    global kv_saves
    return kv_saves.get(key)


def unset_value(key):
    """
    User callable function to unset a key-value pair.

    Args:
        key (str): The key to unset.
    """
    global kv_saves
    kv_saves.pop(key, None)


def validate_table_name(table_name):
    """
    Validate the table name for BigQuery.

    Args:
        table_name (str): The table name to be validated.

    Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating whether the table name is valid, and an error message if it's not valid.
    """
    # Check for spaces
    if " " in table_name:
        return False, "Table name cannot contain spaces."

    # Check for dashes
    if "-" in table_name:
        return False, "Table name cannot contain dashes."

    # Check the length of the table name
    if len(table_name.encode("utf-8")) > 1024:
        return False, "Table name cannot exceed 1,024 UTF-8 bytes."

    # Check for valid characters
    allowed_categories = ["L", "M", "N", "P"]
    for char in table_name:
        char_category = unicodedata.category(char)
        if char_category[0] not in allowed_categories:
            return (
                False,
                "Table name can only contain characters like: letter, mark, number, underscore.",
            )

    return True, "Table name is valid."


def normalize_column_name(column_name):
    """
    Normalize the column name to follow BigQuery rules.

    Args:
        column_name (str): The column name to be normalized.

    Returns:
        str: The normalized column name.
    """
    # Remove leading/trailing whitespace
    column_name = column_name.strip()

    # Replace invalid characters with underscores
    column_name = re.sub(r"[^a-zA-Z0-9_]", "_", column_name)

    # Ensure the column name starts with a letter or underscore
    if not column_name[0].isalpha() and column_name[0] != "_":
        column_name = "_" + column_name

    # Truncate the column name to 300 characters
    column_name = column_name[:300]

    # Check for reserved prefixes and add an underscore if necessary
    reserved_prefixes = [
        "_TABLE_",
        "_FILE_",
        "_PARTITION",
        "_ROW_TIMESTAMP",
        "__ROOT__",
        "_COLIDENTIFIER",
    ]
    for prefix in reserved_prefixes:
        if column_name.startswith(prefix) and column_name != prefix:
            column_name = "_" + column_name

    return column_name


def custom_json_serializer(obj):
    """
    Custom JSON serializer that handles various Python data types.

    Args:
        obj (Any): The object to be serialized.

    Returns:
        str: The serialized object.
    """
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return str(obj)
    elif isinstance(obj, float):
        return str(obj)
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode("utf-8")
    elif isinstance(obj, dict):
        return {
            normalize_column_name(k): custom_json_serializer(v) for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [custom_json_serializer(item) for item in obj]
    elif isinstance(obj, (set, tuple)):
        return [custom_json_serializer(item) for item in obj]
    else:
        return str(obj)


def flatten_and_stringify(obj):
    """
    Flatten the object to 1 level deep and stringify deeper nested structures.

    Args:
        obj (dict): The input object to be flattened and stringified.

    Returns:
        dict: The flattened and stringified object.
    """
    flattened = {}
    for key, value in obj.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                # Check if the sub_value is still a nested structure
                col_name = normalize_column_name(f"{key}_{sub_key}")
                if isinstance(sub_value, (dict, list)):
                    flattened[col_name] = json.dumps(
                        sub_value, default=custom_json_serializer, ensure_ascii=False
                    )
                else:
                    flattened[col_name] = custom_json_serializer(sub_value)
        elif isinstance(value, list):
            # Directly stringify lists to avoid complex structures
            flattened[key] = json.dumps(
                value, default=custom_json_serializer, ensure_ascii=False
            )
        elif isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            flattened[normalize_column_name(key)] = (
                value  # Exempt datetime objs from custom serializer
            )
        else:
            flattened[normalize_column_name(key)] = custom_json_serializer(value)

    # Add the livedocs_date field as a string
    flattened["livedocs_date"] = datetime.datetime.now().isoformat()
    return flattened


def generate_ddl(schema_dict, table_name):
    """
    Generate BigQuery DDL from the schema dictionary.

    Args:
        schema_dict (dict): The dictionary containing the schema information.
        table_name (str): The name of the table.

    Returns:
        str: The BigQuery DDL statement.
    """
    type_mapping = {
        "str": "STRING",
        "int": "INT64",
        "float": "FLOAT64",
        "Decimal": "NUMERIC",
        "bool": "BOOL",
        "datetime": "TIMESTAMP",
        "date": "DATE",
        "time": "TIME",
        "list": "JSON",
        "tuple": "STRING",
        "set": "STRING",
        "dict": "JSON",
        "NoneType": "STRING",
        "bytes": "STRING",  # Base64 encoded string
        "uuid.UUID": "STRING",
    }

    ddl_parts = []
    for column_name, column_type in schema_dict.items():
        if column_type in type_mapping:
            ddl_parts.append(f"{column_name} {type_mapping[column_type]}")
        else:
            ddl_parts.append(
                f"{column_name} STRING"
            )  # Default to STRING for unknown types

    # Always append the livedocs_date field of datetime type
    ddl_parts.append("livedocs_date TIMESTAMP")

    return f"CREATE TABLE {table_name} ({', '.join(ddl_parts)});"


def is_uniform_dicts(lst: list):
    """
    Check if all dicts in the list have the same set of keys after flattening.

    Args:
        lst (list): The list of dictionaries to be checked.

    Returns:
        Tuple[bool, dict]: A tuple containing a boolean indicating whether the dicts are uniform, and a dictionary representing the schema.
    """
    if not lst:
        return True, None  # Empty list considered uniform
    standardized_lst = [flatten_and_stringify(item) for item in lst]
    first_item_keys = set(standardized_lst[0].keys())
    schema = {
        normalize_column_name(key): type(value).__name__
        for key, value in lst[0].items()
    }
    return all(set(item.keys()) == first_item_keys for item in standardized_lst), schema


def save_to_db(table_name, data):
    """
    Save the data to the database and generate the DDL schema.

    Args:
        table_name (str): The name of the table.
        data (list): The data to be saved.
    """
    global db_saves, ddl_schema

    # Validate the table name
    is_valid, error_message = validate_table_name(table_name)
    if not is_valid:
        raise ValueError(error_message)

    # Initialize table if not present
    if table_name not in db_saves:
        db_saves[table_name] = []

    # Check for INVALID marker
    if db_saves.get(table_name) == "INVALID":
        return

    # Validate and process data
    if isinstance(data, list):
        uniform, schema = is_uniform_dicts(data)
        if uniform:
            # Flatten and stringify each item in data before extending db_saves
            flattened_data = [flatten_and_stringify(item) for item in data]
            db_saves[table_name].extend(flattened_data)
            ddl_schema[table_name] = generate_ddl(
                schema,
                table_name,
            )  # Update global DDL schema variable
        else:
            db_saves[table_name] = "INVALID"
            ddl_schema[table_name] = "INVALID"

    else:
        db_saves[table_name] = "INVALID"
        ddl_schema[table_name] = "INVALID"


def push_to_pubsub(livedocs_secrets, error=None, execution_time=0.0):
    """
    Publishes the given main_result to a Pub/Sub topic, with the appropriate status
    and message.

    Args:
        livedocs_secrets (Dict[str, str]): Secrets required by livedocs to execute cf.
        error (str, optional): Indicates whether an error occurred. Defaults to None.
        execution_time (float, optional): The execution time of the function in milliseconds.

    Returns:
        str: A message indicating the result of the Pub/Sub publication.
    """
    INGESTOR_ID = os.getenv("INGESTOR_ID")
    INGESTOR_REQUEST_ID = os.getenv("INGESTOR_REQUEST_ID")
    PROJECT_ID = livedocs_secrets["GCP_PROJECT_ID"]
    TOPIC_NAME = livedocs_secrets["INGESTOR_CF_RESULTS_TOPIC"]
    CF_SERVICE_ACCOUNT_INFO = livedocs_secrets["CF_SERVICE_ACCOUNT_INFO"]
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"

    try:
        # Load the CF_SERVICE_ACCOUNT_INFO as a dictionary
        service_account_info = json.loads(CF_SERVICE_ACCOUNT_INFO)
        credentials = jwt.Credentials.from_service_account_info(
            service_account_info, audience=audience
        )

        publisher = pubsub_v1.PublisherClient(credentials=credentials)

        # References an existing topic
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

        # This is the status of the user's code execution
        status = 200 if not error else 500
        message = "success" if not error else f"failed: {error}"

        message_json = json.dumps(
            {
                "data": {
                    "status": status,
                    "message": message,
                    "ingestor_id": INGESTOR_ID,
                    "ingestor_request_id": INGESTOR_REQUEST_ID,
                    "database_writes": db_saves,
                    "execution_time": execution_time,
                    "kv": kv_saves,
                },
            },
            default=custom_json_serializer,
            ensure_ascii=False,
        )
        message_bytes = message_json.encode("utf-8")

        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()
        return f"Results published to {TOPIC_NAME} for {INGESTOR_REQUEST_ID}"
    except json.JSONDecodeError as e:
        print("JSON decoding error:", e)
        return (e, 500)
    except Exception as e:
        print(f"Failed to publish results for {INGESTOR_REQUEST_ID}", e)
        return (e, 500)


def get_secrets():
    """
    Retrieves decrypted secrets, KV store from @ingestor and updates the global dictionaries.
    """
    INGESTOR_REQUEST_ID = os.getenv("INGESTOR_REQUEST_ID")
    INGESTOR_URL = os.getenv("INGESTOR_URL")
    global secrets, kv_saves

    response = requests.get(f"{INGESTOR_URL}/get-secrets/{INGESTOR_REQUEST_ID}")
    if response.status_code == 200:
        data = response.json()
        # Update KV store
        kv = json.loads(data["kv"])
        kv_saves.update(kv)
        # Only user secrets are globally accessible
        secrets.update(data["user_secrets"])
        return data["livedocs_secrets"]
    else:
        print(f"Failed to retrieve secrets: {response.status_code} - {response.text}")
