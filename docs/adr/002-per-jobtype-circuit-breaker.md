# 002: Per-job-type circuit breakers

Date: 2026-04-18

## Status

Accepted

## Context

We need circuit breakers to prevent cascading failures. Should we have one circuit breaker per worker or per job type?

## Decision

Per job type.

## Consequences

### Pros
- Isolation: A broken `send_email` handler doesn't affect `generate_report`
- Granular control: Different failure thresholds per job type
- Better observability: Per-type metrics

### Cons
- More state: Map of circuit breakers instead of single instance
- Complexity: Managing multiple breakers

The isolation benefit outweighs the complexity.