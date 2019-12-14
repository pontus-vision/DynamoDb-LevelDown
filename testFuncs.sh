#!/bin/sh

IFS=''
DOCKER_PATH=$(which docker)
if [ "$DOCKER_PATH" == "" ]; then
  echo "Cannot find docker" >&2
  exit 1
fi

function readDockerImageState {
  local data
  local name=$1
  local prefix=$(printf '%s\n' "$1" | awk '{ print toupper($0) }')
  read -ra data <<< $(docker container ls -a --filter name=$name --format='"{{.ID}}" "{{.Names}}" "{{.Status}}""' | awk '{print $1" "$2" "$3 "\""}')
  command eval "local data=($data)"
  if [ "$data" != "" ]; then
    command eval "${prefix}_ID=\"${data[0]}\""
    command eval "${prefix}_NAME=\"${data[1]}\""
    local status=$(printf '%s\n' "${data[2]}" | awk '{ print tolower($0) }')
    command eval "${prefix}_STATUS=\"${status}\""
  fi
}
