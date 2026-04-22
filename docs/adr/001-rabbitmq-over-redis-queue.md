# 001: RabbitMQ over Redis as the queue broker

Date: 2026-04-18

## Status

Accepted

## Context

We need a message broker for the job queue. Common options are Redis (Streams or Lists) and RabbitMQ.

## Decision

Use RabbitMQ.

## Consequences

### Pros
- Message-level acknowledgements: crashed worker doesn't lose job
- Dead-letter exchange routing: first-class DLQ, not an afterthought
- Per-queue consumer priority: enforce high > normal > low
- Management UI: non-engineers can inspect queue depth

### Cons
- More complex setup than Redis
- Higher resource usage

For a job queue specifically, RabbitMQ's model fits better than Redis's.