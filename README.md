# Connect Log Consumer (Spring Boot)

This Spring Boot application consumes logs from the Confluent Cloud `confluent-connect-log-events` topic and logs them locally.

## Prerequisites

- Java 21
- Maven
- Confluent Cloud API Key and Secret

## Build

Build the project using Maven:

```bash
mvn package
```

This will create an executable Spring Boot jar in `target/fully-managed-connectors-consumer-1.0-SNAPSHOT.jar`.

## Configuration

The application is configured via environment variables. You can provide these variables in a `secret.env` file in the project root.

### Using secret.env (Recommended)

1. Create a `secret.env` file in the project root (you can copy `secret.env.example`):

   ```bash
   cp secret.env.example secret.env
   ```

2. Edit `secret.env` and add your API keys:

   ```properties
   CLUSTER_API_KEY=your_key
   CLUSTER_API_SECRET=your_secret
   ```

The application will automatically load this file if it exists. `secret.env` is ignored by git.

### Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `BOOTSTRAP_SERVERS` | Kafka Bootstrap Servers | `pkc-4ywp7.us-west-2.aws.confluent.cloud:9092` |
| `CLUSTER_API_KEY` | Confluent Cloud API Key | **Required** |
| `CLUSTER_API_SECRET` | Confluent Cloud API Secret | **Required** |
| `TOPIC_NAME` | Topic to consume | `confluent-connect-log-events` |
| `GROUP_ID` | Consumer Group ID | `connect-log-consumer-group` |
| `CONSUME_FROM_START` | Consume from beginning | `false` |

## Run Locally

You can run the application using `java -jar` or `mvn spring-boot:run`.

The application is configured to use the OpenTelemetry Java Agent (`.otel/opentelemetry-javaagent.jar`) for sending logs, metrics, and traces to Grafana.

### Using Maven (Recommended)

The Maven plugin is pre-configured with the necessary JVM arguments and OpenTelemetry configuration.

```bash
export CLUSTER_API_KEY="<YOUR_API_KEY>"
export CLUSTER_API_SECRET="<YOUR_API_SECRET>"
# If using secret.env, just run:
mvn spring-boot:run
```

### Using Java

If you are running the JAR directly, you need to provide the JVM arguments manually:

```bash
export CLUSTER_API_KEY="<YOUR_API_KEY>"
export CLUSTER_API_SECRET="<YOUR_API_SECRET>"

JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseParallelGC -XX:ActiveProcessorCount=1 -Djdk.virtualThreadScheduler.parallelism=20 --add-opens java.base/java.time=ALL-UNNAMED -Djava.security.egd=file:/dev/./urandom"
OTEL_OPTS="-javaagent:.otel/opentelemetry-javaagent.jar -Dotel.logs.exporter=otlp -Dotel.metrics.exporter=otlp -Dotel.traces.exporter=otlp -Dotel.exporter.otlp.protocol=grpc -Dotel.exporter.otlp.endpoint=https://otel-eur.michelin.com:443 -Dotel.exporter.otlp.headers=x-scope-orgid=default -Dotel.resource.attributes=service.name=fully-managed-connectors-consumer,service.namespace=kfe,environment=dev -Dotel.javaagent.debug=false"

java $JAVA_OPTS $OTEL_OPTS -jar target/fully-managed-connectors-consumer-1.0-SNAPSHOT.jar
```
