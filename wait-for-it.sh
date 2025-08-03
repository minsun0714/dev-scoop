#!/usr/bin/env sh
host="$1"
shift
cmd="$@"

until nc -z $(echo $host | cut -d: -f1) $(echo $host | cut -d: -f2); do
  echo "Waiting for $host..."
  sleep 2
done

exec $cmd
