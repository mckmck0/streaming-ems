# 0010. Apache Kafka 4.3 (KRaft, JVM image) as the broker

- Status: accepted
- Date: 2026-10-20
- Spec: specs/001-data-pipeline-core

## Context
The brief left the dev broker OPEN (Redpanda or Apache Kafka in KRaft mode). Plan 1 chose `apache/kafka`.
Verified 2026-10-20: 4.3.1 (2026-06-25) is the newest pinnable 4.x; librdkafka 2.15.1 CI tests against 4.3;
Restate 1.7.13 bundles librdkafka 2.12.1, which handles the protocol versions removed by KIP-896.

## Decision
Use `apache/kafka:4.3.1@sha256:77e3df90…` as a single combined broker+controller in `infra/docker-compose.yml`.
Not `apache/kafka-native`: experimental, no CLI tools for healthchecks, zstd broken until 4.4 (KAFKA-19839).

## Consequences
Kafka 4.x timestamp rules (1 h future limit, retention by record time) shape the topic profiles (05, 09).
Kubernetes broker (Strimzi vs Redpanda) stays OPEN for the Helm spec.

## Alternatives considered
Redpanda (no reason to prefer for a Java-protocol thesis stack); Kafka 4.2.2 (kept as fallback for kafka-ui).
