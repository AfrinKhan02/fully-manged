#!/bin/bash

# Configuration
export BOOTSTRAP_SERVERS="pkc-4ywp7.us-west-2.aws.confluent.cloud:9092"
export CLUSTER_API_KEY="<YOUR_API_KEY>"
export CLUSTER_API_SECRET="<YOUR_API_SECRET>"
export TOPIC_NAME="confluent-connect-log-events"

# Run the application
java -jar target/fully-managed-connectors-consumer-1.0-SNAPSHOT.jar
