#!/bin/bash

source ./testFuncs.sh

function startDockerContainers() {
  readDockerContainerState 'localstack'

  if [ "$LOCALSTACK_NAME" == "" ]; then
    LOCALSTACK_NAME='localstack-level-dynamo'
  fi

  # Limit services to startup faster
  LOCALSTACK_SERVICES='s3,dynamodb'

  case "$LOCALSTACK_STATUS" in
  "")
    LOCALSTACK_ID=$(docker run --name $LOCALSTACK_NAME -e LOCALSTACK_SERVICES="$LOCALSTACK_SERVICES" -t -d -P localstack/localstack:latest)
    echo "Created '${LOCALSTACK_ID:0:12}' running as '$LOCALSTACK_NAME'"
    ;;
  "exited")
    LOCALSTACK_NAME=$(docker start $LOCALSTACK_NAME)
    echo "Started '${LOCALSTACK_ID:0:12}' running as '$LOCALSTACK_NAME'"
    ;;
  "up")
    LOCALSTACK_NAME=$(docker restart $LOCALSTACK_NAME)
    echo "Restarted '${LOCALSTACK_ID:0:12}' running as '$LOCALSTACK_NAME'"
    ;;
  *) echo "Found '${LOCALSTACK_ID:0:12}' running as '$LOCALSTACK_NAME'" ;;
  esac
}

if [ "$CI" == "true" ]; then
  echo "Testing under CI"
  export S3_PORT=$LOCALSTACK_EDGE_PORT
  export DYNAMODB_PORT=$LOCALSTACK_EDGE_PORT
else
  echo "NOT testing under CI"
  startDockerContainers
  EDGE_PORT=$(docker inspect --format='{{(index (index .NetworkSettings.Ports "4566/tcp") 0).HostPort}}' $LOCALSTACK_ID)
  export S3_PORT=$EDGE_PORT
  export DYNAMODB_PORT=$EDGE_PORT
fi

echo "S3 is at http://localhost:$S3_PORT"
echo "DynamoDb is at http://localhost:$DYNAMODB_PORT"

runTests
