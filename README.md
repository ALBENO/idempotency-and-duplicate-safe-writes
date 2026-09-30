## Design Decisions

### 1. Database uniqueness

The idempotency key is scoped by authenticated tenant and operation.

The database enforces:

`UNIQUE (tenant_id, operation, key)`

This is required because an application-level check followed by an insert has a race condition. Concurrent requests can both observe that a key does not exist and both create an incident. The database uniqueness constraint provides the atomic ownership boundary.
