#!/bin/bash

rm -f flows/etl_flow.zip
mkdir -p flows
zip -r flows/etl_flow.zip etl_pipeline requirements.txt
echo "Zipped flow code into flows/etl_flow.zip"

