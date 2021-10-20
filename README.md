# NoSQL

#### Author: Saurabh Pathak (spathak2@andrew.cmu.edu)

### Steps Taken:
  1. Connect to AWS S3 resource and create a storage bucket, named 'saurpath-nosql'.
  2. Upload the experiment files, i.e. exp1.csv, exp2.csv and exp3.csv, to the storage bucket.
  3. Create a DynamoDB NoSQL Database.
  4. Create a table named 'DataTable' in the database.
  5. Upload and populate the master record in the table.
  6. Link the corresponding row in the table with the URL of the experiment file.
  7. Validate the files are linked successfully and the contents are not changed by:
      * Connect to the DynamoDB, read the URL record and download the file directly from the URL.
      * Calculate a hash using sha256 of the file contents stored locally on the system.
      * Calculate a hash using sha256 of the file contents downloaded using the URL.
      * Ensures both these hashes match to confirm the file integrity is maintained.
   
   
 ###  Contents of DynamoDB
 #### The query scans the database and displays all the records containing the extension .csv
   ```ruby
   saurpath@Saurabhs-MacBook-Pro NoSQL % aws dynamodb scan --table-name DataTable --filter-expression "contains(#n0 , :v0)" --expression-attribute-names "#n0 = PartitionKey" --expression-attribute-values ":v0 = {S = .csv}"
   
   {
    "Items": [
        {
            "Temp": {
                "S": "-2.93"
            },
            "RowKey": {
                "S": "3"
            },
            "Conductivity": {
                "S": "57.1"
            },
            "Concentration": {
                "S": "3.7"
            },
            "PartitionKey": {
                "S": "exp3.csv"
            },
            "url": {
                "S": " https://s3-us-east-2.amazonaws.com/saurpath-nosql/exp3.csv"
            }
        },
        {
            "Temp": {
                "S": "-2"
            },
            "RowKey": {
                "S": "2"
            },
            "Conductivity": {
                "S": "52.1"
            },
            "Concentration": {
                "S": "3.4"
            },
            "PartitionKey": {
                "S": "exp2.csv"
            },
            "url": {
                "S": " https://s3-us-east-2.amazonaws.com/saurpath-nosql/exp2.csv"
            }
        },
        {
            "Temp": {
                "S": "-1"
            },
            "RowKey": {
                "S": "1"
            },
            "Conductivity": {
                "S": "52"
            },
            "Concentration": {
                "S": "3.4"
            },
            "PartitionKey": {
                "S": "exp1.csv"
            },
            "url": {
                "S": " https://s3-us-east-2.amazonaws.com/saurpath-nosql/exp1.csv"
            }
        }
    ],
    "Count": 3,
    "ScannedCount": 3,
    "ConsumedCapacity": null
}
   ```
