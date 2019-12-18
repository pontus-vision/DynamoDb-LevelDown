#!/bin/bash

source ./testFuncs.sh

readDockerImageState 'localstack'

function startDockerContainers() {
  if [ "$LOCALSTACK_NAME" == "" ]; then
    LOCALSTACK_NAME='localstack-level-dynamo'
  fi

  # Limit services to startup faster
  LOCALSTACK_SERVICES='s3,dynamodb'

  case "$LOCALSTACK_STATUS" in
  "")
    LOCALSTACK_ID=$(docker run --name $LOCALSTACK_NAME -t -d -P localstack/localstack:latest)
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
else
  startDockerContainers
fi

export S3_PORT=$(docker inspect --format='{{(index (index .NetworkSettings.Ports "4572/tcp") 0).HostPort}}' $LOCALSTACK_ID)
export DYNAMODB_PORT=$(docker inspect --format='{{(index (index .NetworkSettings.Ports "4569/tcp") 0).HostPort}}' $LOCALSTACK_ID)

echo "S3 is at http://localhost:$S3_PORT"
echo "DynamoDb is at http://localhost:$DYNAMODB_PORT"

runTests
