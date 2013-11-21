# CouchDB-2-s3

Export a CouchDB database as a line oriented JSON file, then upload that to S3.
Then later on s32couchdb that same file to get it into CouchDB instance.
Or just use that line-oriented version of your CouchDB database for something else.

## Install

    npm install -g couchdb2s3

## Usage

```
couchdb2s3 --config config.json \
  --outputBucket my-bucket \
  --database http://localhost:5984/my-database
```

and

```
couchdb2file --config config.json \
  --outputFile myfile.json \
  --database http://localhost:5984/my-database
```

and

```
s32couchdb --config config.json \
  --inputBucket my-bucket \
  --database http://localhost:5984/my-database
```

The import script will not create a new database for you, you'll have to do that on your own.

## Caveats

* Attached documents are not currently supported.
* As written these scripts expect to be used as part of a export system where only the most recent backups are meaningful.
