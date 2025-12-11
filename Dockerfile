FROM docker.artifactory.michelin.com/michelin/hub/eclipse-temurin21-jre-alpine:bib-1.4 as run

WORKDIR /home/k8s

# Copy application jar
COPY ./target/fully-managed-connectors-consumer-1.0-SNAPSHOT.jar /home/k8s/app.jar

# Copy OTel agent
# Ensure opentelemetry-javaagent.jar is in target/ directory before building (handled by CI or manual copy)
COPY ./target/opentelemetry-javaagent.jar /home/k8s/opentelemetry-javaagent.jar

# Configuration file (optional, if you want to override via file)
# COPY ./src/main/resources/application.yaml /home/k8s/application.yaml

# Environment variables for OTel are expected to be passed via deployment config
ENTRYPOINT exec java $JAVA_OPTS_OTEL -jar $JAVA_OPTS ./app.jar

