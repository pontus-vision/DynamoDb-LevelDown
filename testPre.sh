#!/bin/bash

if [ "$CI" == "true" ]; then echo "Testing under CI"; fi
source ./testFuncs.sh

readDockerImageState 'dynamo'
case "$DYNAMO_STATUS" in
"")
  id=$(docker run --name dynamodb-local -t -d -p 4567:4567 -u dynamodblocal amazon/dynamodb-local:latest "-Djava.library.path=./DynamoDBLocal_lib" "-jar" "DynamoDBLocal.jar" "-inMemory" "-port" "4567" "-delayTransientStatuses")
  echo "Created '${id:0:12}' running as 'dynamodb-local'"
  ;;
"exited")
  id=$(docker start $DYNAMO_NAME)
  echo "Started '${DYNAMO_ID:0:12}' running as 'dynamodb-local'"
  ;;
"up")
  id=$(docker restart $DYNAMO_NAME)
  echo "Restarted '${DYNAMO_ID:0:12}' running as 'dynamodb-local'"
  ;;
*) echo "Found '${DYNAMO_ID:0:12}' running as '$DYNAMO_NAME'" ;;
esac
