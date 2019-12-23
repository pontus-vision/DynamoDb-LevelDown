#!/bin/bash
CC_PATH=$(which codeclimate)
if [ "$CC_PATH" == "" ]; then
  echo "Setup CodeClimate via https://github.com/codeclimate/codeclimate"
else
  echo "Using Code Climate from $CC_PATH"
  mkdir -p ./build
  set -o | grep noclobber
  codeclimate analyze -f html ./src >|./build/climate.html
fi
