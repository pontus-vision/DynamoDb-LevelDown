#!/bin/sh
if [ "$CI" != "true" ]; then
  ts-node node_modules/tape/bin/tape ./test/** | faucet || true
else
  tsc && nyc ts-node node_modules/tape/bin/tape ./test/** | tap-junit -s 'DynamoDBDown All Tests' -o build/junit -n results.xml || true
fi