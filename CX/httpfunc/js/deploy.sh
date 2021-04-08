#!/bin/bash

#  https://asia-southeast2-cxplot.cloudfunctions.net/http_function

gcloud functions deploy http_function \
    --trigger-http \
    --runtime nodejs10 \
    --allow-unauthenticated \
    --region=asia-southeast2

# gcloud functions delete http_function --quiet