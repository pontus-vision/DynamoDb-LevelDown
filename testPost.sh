#!/bin/bash

source ./testFuncs.sh

if [ "$CI" == "true" ]; then
  echo "Tested under CI"
else
  echo "NOT tested under CI"
  readDockerContainerState 'localstack'
  case "$LOCALSTACK_STATUS" in
  "up")
    id=$(docker stop $LOCALSTACK_NAME)
    echo "Stopped '${LOCALSTACK_ID:0:12}' running as '$LOCALSTACK_NAME'"
    ;;
  esac
fi
