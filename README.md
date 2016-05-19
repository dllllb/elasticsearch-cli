elasticsearch-cli
=================

ElasticSearch CLI tool for batch searches and uploads

# Usage

## Search for the documents having field value in specified file

    cat INPUT_FILE | java -jar elasticsearch-cli.jar --endpoint localhost --index INDEX \
    search-by-input --search-field FIELD_TO_MATCH

## Search by Lucene query

    java -jar elasticsearch-cli.jar --endpoint localhost --index INDEX scan --query LUCENE_QUERY

## Upload a list of JSON documents in batches

    cat INPUT_FILE | java -jar elasticsearch-cli.jar --endpoint localhost --index INDEX batch-upload

## Retrieve documents by its key

    cat INPUT_FILE | java -jar elasticsearch-cli.jar --endpoint localhost --index INDEX multi-get
