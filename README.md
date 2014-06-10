# CouchDB-2-s3

Export a CouchDB database as a line oriented JSON file, then upload that to S3.
Then later on s32couchdb that same file to get it into CouchDB instance.

## Install

    npm install -g couchdb2s3

## Usage

```
couchdb2s3 \
  --bucket my-bucket \
  --database http://localhost:5984/my-database
  [--gzip]                                      # Optionally gzip the export
```

and

```
s32couchdb \
  --bucket my-bucket \
  --database http://localhost:5984/my-database
  [--prefix `db/my-database`]                   # Optionally specify a s3 prefix,
                                                #   defaults to `db/[name of database]`.
  [--marker `db/my-database-2010-12-31`]        # Optionally specify a s3 marker,
                                                #   defaults to `db/[name of database]-[yyyy]-[mm]-[dd]`
                                                #   for yesterday's date.
```

## Configuration

AWS credentials for uploading and retrieving exports can be provided by;

1. IAM role assigned to an EC2
2. Environment variables; AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY
3. A [dotenv](https://www.npmjs.org/package/dotenv) configuration file

## Caveats

* `s32couchdb` will not create a new database for you, you'll have to do that on your own.
* Attached documents are not currently supported.
* As written these scripts expect to be used as part of a export system where only the most recent backups are meaningful.
