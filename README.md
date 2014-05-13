# CouchDB-2-s3

Export a CouchDB database as a line oriented JSON file, then upload that to S3.
Then later on s32couchdb that same file to get it into CouchDB instance.

## Install

    npm install -g couchdb2s3

## Usage

```
couchdb2s3 \
  --outputBucket my-bucket \
  --database http://localhost:5984/my-database
  --gzip
```

and

```
s32couchdb \
  --inputBucket my-bucket \
  --database http://localhost:5984/my-database
```

The import script will not create a new database for you, you'll have to do that on your own.

AWS credentials for uploading and retrieving exports can be provided either; as a IAM role assigned to an EC2, or a `~/.couchdb2s3rc` configuration file.

## Caveats

* Attached documents are not currently supported.
* As written these scripts expect to be used as part of a export system where only the most recent backups are meaningful.
