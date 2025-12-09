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

This will create an executable Spring Boot jar in `target/connect-log-consumer-1.0-SNAPSHOT.jar`.

## Configuration

The application is configured via environment variables, mapped in `src/main/resources/application.yaml`.

| Variable | Description | Default |
|----------|-------------|---------|
| `BOOTSTRAP_SERVERS` | Kafka Bootstrap Servers | `pkc-4ywp7.us-west-2.aws.confluent.cloud:9092` |
| `CLUSTER_API_KEY` | Confluent Cloud API Key | **Required** |
| `CLUSTER_API_SECRET` | Confluent Cloud API Secret | **Required** |
| `TOPIC_NAME` | Topic to consume | `confluent-connect-log-events` |
| `GROUP_ID` | Consumer Group ID | `connect-log-consumer-group` |

## Run Locally

Use the provided script `run.sh` (Linux/Mac).

**Important**: Edit the script to set your `CLUSTER_API_KEY` and `CLUSTER_API_SECRET` before running.

```bash
./run.sh
```

Or manually:

```bash
java -jar target/fully-managed-connectors-consumer-1.0-SNAPSHOT.jar
```
