#!/bin/bash
# Helper script to initialize a local Apache ActiveMQ Artemis broker instance

usage() {
    echo "Usage: $0 <port> <directory>"
    echo "  <port>      Port number where Artemis broker will listen (e.g. 61616)"
    echo "  <directory> Target directory path to instantiate the broker"
    echo "Example:"
    echo "  $0 61616 ./artemis-broker"
    exit 1
}

if [ $# -ne 2 ]; then
    usage
fi

PORT=$1
DIR=$2

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ]; then
    echo "Error: Port must be a numeric value greater than 1024."
    exit 1
fi

if [ -d "$DIR" ]; then
    echo "Error: Directory '$DIR' already exists. Please remove it before re-initializing."
    exit 1
fi

mkdir -p "$DIR" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Error: Cannot create directory '$DIR'. Please verify write permissions."
    exit 1
fi

WPORT=$((PORT + 1))

echo "Initializing Artemis broker in $DIR (Broker Port: $PORT, Web Console: $WPORT)..."

./artemis create \
    --default-port $PORT \
    --http-port $WPORT \
    --allow-anonymous \
    --user admin \
    --password admin \
    --no-autocreate \
    --relax-jolokia \
    --no-autotune \
    --no-amqp-acceptor \
    --no-hornetq-acceptor \
    --no-mqtt-acceptor \
    --no-stomp-acceptor \
    "$DIR"
