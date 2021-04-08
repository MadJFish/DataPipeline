#!/bin/bash

#  https://asia-southeast2-cxplot2.cloudfunctions.net/hello_http

if [ "$1" == "" ]; then
    echo "Error!"
    echo "Format: $0 function_name"
    exit 1
fi

gcloud functions deploy $1 \
    --trigger-http \
    --runtime python38 \
    --allow-unauthenticated \
    --region=asia-southeast2

# gcloud functions delete hello_http --quiet

echo "https://asia-southeast2-cxplot2.cloudfunctions.net/$1"