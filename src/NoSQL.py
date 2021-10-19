import boto3
import csv
import requests
from boto3.dynamodb.conditions import Key
import hashlib

ACCESS_KEY_ID = "XX"
SECRET_ACCESS_KEY = "XX"
REGION = "us-east-2"

# Files
MASTER = "data/master.csv"
EXPERIMENT_FILES = ["data/exp1.csv", "data/exp2.csv", "data/exp3.csv"]


"""
VALIDATE TABLE CONTENTS
"""


def get_item(table: boto3.resource, partition: str) -> dict:
    response = table.query(KeyConditionExpression=Key("PartitionKey").eq(partition))
    try:
        return response["Items"][0]
    except:
        return None


def download_file_hash(url: str) -> str:
    ses = requests.Session()
    download = ses.get(url)

    # File contents
    decoded_content = download.content.decode("utf-8")

    file_hash = hashlib.sha256(download.content).hexdigest()
    return file_hash


def validate_result(table: boto3.resource, bucket_name: str) -> bool:
    for file in EXPERIMENT_FILES:
        record = get_item(table, file.split("/")[-1])

        with open(file, "rb") as f:
            bytes = f.read()  # read entire file as bytes
            local_hash = hashlib.sha256(bytes).hexdigest()

        if record == None:
            return False
        else:
            remote_hash = download_file_hash(record["url"])
            try:
                assert remote_hash == local_hash
                print(f"Passed validation for {file}")
            except:
                print(f"Failed validation for {file}")
    return True


"""
CREATE BUCKET AND UPLOAD FILES
"""


def upload_files(bucket_name: str) -> bool:
    for file in EXPERIMENT_FILES:
        try:
            # upload a new object into the bucket
            body = open(file, "rb")

            filename = file.split("/")[-1]

            o = s3.Object(bucket_name, filename).put(Body=body)
            # Make it publicly available
            s3.Object(bucket_name, filename).Acl().put(ACL="public-read")
        except Exception as e:
            print(f"Exception occured while uploading {file} to {bucket_name}. {e}")
            return False
    return True


def create_s3_bucket(s3, bucket_name: str) -> boto3.resources.factory:
    bucket = None
    try:
        # Create bucket.
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": REGION})

    except Exception as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            pass
        else:
            print(f"Exception creating bucket. {e}")
            return

    # Make the bucket publicly available.
    bucket = s3.Bucket(bucket_name)
    bucket.Acl().put(ACL="public-read")

    return bucket


def get_s3():
    # Connect to AWS, s3 resource
    try:
        s3 = boto3.resource("s3", aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)
        return s3
    except Exception as e:
        print(f"Exception getting s3 resource. {e}")
        return None


"""
CREATE AND POPULATE TABLE
"""


def update_table(table: boto3.resources.factory, bucket_name: str):
    with open(MASTER, "r") as csvfile:
        csvf = csv.reader(csvfile, delimiter=",", quotechar="|")
        for item in csvf:
            if str.isnumeric(item[0]):
                url = " https://s3-" + REGION + ".amazonaws.com/" + bucket_name + "/" + item[4]
                metadata_item = {
                    "PartitionKey": item[4],
                    "RowKey": item[0],
                    "Temp": item[1],
                    "Conductivity": item[2],
                    "Concentration": item[3],
                    "url": url,
                }

                try:
                    table.put_item(Item=metadata_item)
                except Exception as e:
                    print(f"Exception occured while uploading {item[4]}")


def create_table(dyndb: boto3.resources.factory):
    try:
        table = dyndb.create_table(
            TableName="DataTable",
            KeySchema=[
                {"AttributeName": "PartitionKey", "KeyType": "HASH"},
                {"AttributeName": "RowKey", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PartitionKey", "AttributeType": "S"},
                {"AttributeName": "RowKey", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
    except Exception as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            # Table already exists.
            pass
        else:
            print(f"Exception creating table. {e.resource}")
            return
        # If there is an exception, the table may already exist. if so...
        table = dyndb.Table("DataTable")

    # Wait for the table to be created
    table.meta.client.get_waiter("table_exists").wait(TableName="DataTable")
    return table


def create_dynamodb() -> boto3.resources.factory:
    try:
        dyndb = boto3.resource(
            "dynamodb", region_name=REGION, aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY
        )
        return dyndb
    except Exception as e:
        print(f"Exception creating dynamoDB. {e}")
        return None


if __name__ == "__main__":
    bucket_name = "saurpath-nosql"

    s3 = get_s3()
    bucket = create_s3_bucket(s3, bucket_name)
    upload_files(bucket_name)

    dyndb = create_dynamodb()
    table = create_table(dyndb)
    update_table(table, bucket_name)

    if validate_result(table, bucket_name):
        print("Experiment Completed successfully")
    else:
        print("Experiment Failed")
