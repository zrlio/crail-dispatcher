#!/bin/sh

# Script to package lambda code for ZIP file upload to AWS
# Assumes you have directories bin, conf, and jars for crail

rm *.zip
zip -9r java.zip lambda_java.py crail.py bin conf jars __init__.py

# To update AWS Lambda function code with this zip file, run:
#aws lambda update-function-code --function-name [FUNCTION_NAME] --zip-file fileb:///path/to/crail-dispatcher/python-client/java.zip
