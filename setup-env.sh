#!/bin/bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f /usr/bin/java)))
export PATH=$PATH:$JAVA_HOME/bin:/opt/pig-0.17.0/bin

# Start the main process
exec "$@"

# Start ollama 
