# 003: KEDA over Kubernetes HPA for autoscaling

Date: 2026-04-18

## Status

Accepted

## Context

Workers should scale based on queue depth. Kubernetes HPA scales on CPU/memory, but workers are I/O bound, not CPU bound.

## Decision

Use KEDA with RabbitMQ queue length triggers.

## Consequences

### Pros
- Scales on actual signal: queue depth
- No custom metrics server needed
- Integrates with existing RabbitMQ
- Supports multiple triggers (high + normal queues)

### Cons
- Additional dependency: KEDA operator
- More complex than basic HPA

For job queues, queue depth is the right metric.