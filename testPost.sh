#!/bin/sh
if [ "$CI" == "true" ]; then exit 0; fi

source testFuncs.sh

readDockerImageState 'dynamo'
case "$DYNAMO_STATUS" in
  "up") 
    id=$(docker stop $DYNAMO_NAME)
    echo "Stopped '${DYNAMO_ID:0:12}' running as 'dynamodb-local'"
    ;;
esac
