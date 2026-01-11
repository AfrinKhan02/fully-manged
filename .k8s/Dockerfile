FROM docker.artifactory.michelin.com/michelin/hub/eclipse-temurin21-jre-alpine as run

WORKDIR /home/k8s

COPY ./target/fully-managed-connectors-consumer-1.0-SNAPSHOT.jar /home/k8s/fully-managed-connectors-consumer.jar
COPY ./.otel/opentelemetry-javaagent.jar opentelemetry-javaagent.jar

USER appuser

ENTRYPOINT exec java -javaagent:/home/k8s/opentelemetry-javaagent.jar \
    -XX:MaxRAMPercentage=75 \
    -XX:+UseParallelGC \
    -XX:ActiveProcessorCount=1 \
    -Djdk.virtualThreadScheduler.parallelism=20 \
    --add-opens java.base/java.time=ALL-UNNAMED \
    -Djava.security.egd=file:/dev/./urandom \
    -Dotel.instrumentation.logback-appender.enabled=false \
    -Dotel.instrumentation.log4j-appender.enabled=false \
    -Dotel.instrumentation.jul.enabled=false \
    -Dotel.logs.exporter=otlp \
    -Dotel.metrics.exporter=otlp \
    -Dotel.traces.exporter=otlp \
    -Dotel.exporter.otlp.protocol=grpc \
    -Dotel.exporter.otlp.endpoint=https://otel-eur.michelin.com:443 \
    -Dotel.exporter.otlp.headers=x-scope-orgid=default \
    -Dotel.resource.attributes=service.name=fully-managed-connectors-consumer,service.namespace=kfe,environment=dev \
    -Dotel.javaagent.debug=false \
    -Dotel.blrp.schedule.delay=1000 \
    -Dotel.blrp.max.queue.size=8192 \
    -Dotel.blrp.max.export.batch.size=512 \
    -jar /home/k8s/fully-managed-connectors-consumer.jar
