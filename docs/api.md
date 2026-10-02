# equalix-go HTTP API

Extracted from Java Equalix Spring controllers and `docs/api-reference.md`.
This document defines the REST contract equalix-go must preserve.

Sources:

- Java REST: `src/main/java/org/synanton/equalix/adapter/in/rest/` (`TaskIngestionController`, `TaskCompletionController`, `TaskManagementController`, `SystemStatusController`), `adapter/in/rest/dto/`, `config/ApiKeyAuthFilter.java`, `config/SecurityConfig.java`, `GlobalExceptionHandler.java`
- Java Kafka: `adapter/in/kafka/TaskIngestionKafkaConsumer.java`
- Java docs: `docs/api-reference.md`

---

## Conventions

- **Base path:** `/api/v1` (from `@RequestMapping("/api/v1/tasks")`, `@RequestMapping("/api/v1/status")`).
- **Content type:** `application/json` for all `POST` with body; `Accept: application/json`.
- **Auth:** every `/api/v1/**` request must include header `X-API-Key` matching `app.security.api-key` (default `${EQUALIX_API_KEY:changeme}`). Missing or wrong key → `401 Unauthorized` with an **empty servlet body** (Spring `sendError(401)`, *not* the error envelope below). Only `/actuator/health`, `/actuator/health/**`, `/actuator/info` are public.
- **Error shape:** custom envelope from `GlobalExceptionHandler` (not RFC 7807):

```json
{
  "code": "NOT_FOUND | BAD_REQUEST | VALIDATION_FAILED | INTERNAL_ERROR",
  "message": "string",
  "timestamp": "2026-01-01T12:00:00Z",
  "fieldErrors": null
}
```

`fieldErrors` is populated only for `VALIDATION_FAILED`:

```json
{
  "code": "VALIDATION_FAILED",
  "message": "Request validation failed",
  "timestamp": "2026-01-01T12:00:00Z",
  "fieldErrors": [{"field": "fairnessKey", "message": "must not be blank"}]
}
```

| Java exception | HTTP | `code` | `message` | `fieldErrors` |
|---|---|---|---|---|
| `EntityNotFoundException`, `TaskNotFoundException` | 404 | `NOT_FOUND` | `ex.getMessage()` (e.g. `Task not found: <id>`) | `null` |
| `IllegalArgumentException` | 400 | `BAD_REQUEST` | `ex.getMessage()` | `null` |
| `MethodArgumentNotValidException` | 400 | `VALIDATION_FAILED` | `Request validation failed` | `[{"field","message"}]` per field error |
| any other `Exception` | 500 | `INTERNAL_ERROR` | `Internal server error` | `null` |

- **Binary fields** (`payload`, `result`, `previous_result`): JSON base64 strings mapping to Java `byte[]`.
- **Timestamps**: ISO-8601 instants (`createdAt`, `completedAt`, `timestamp`).
- **Idempotency:** completion of an already-terminal task is ignored (success, no state change). Ingestion always creates a new task (fresh UUID); there is no client-supplied idempotency key.

---

## Endpoints

### `POST /api/v1/tasks`

**Purpose:** ingest a single task. Source: `TaskIngestionController.createTask` (`@ResponseStatus(CREATED)`).

**Request** (`CreateTaskRequest`):

| JSON field | Type | Required | Validation |
|---|---|---|---|
| `fairnessKey` | string | yes | `@NotBlank` |
| `weight` | number | no, default `1.0` | `@Positive` |
| `payload` | base64 | yes | `@NotNull`; size cap `max-payload-bytes` (default 1048576) enforced in `CreateTaskUseCase` → `IllegalArgumentException` (`400 BAD_REQUEST`) when exceeded |
| `sequential` | boolean | no, default `false` | — |
| `sequenceNumber` | integer | conditional: required when `sequential=true` (`@AssertTrue`: `!sequential \|\| sequenceNumber != null`, message `sequenceNumber is required when sequential is true`) | — |
| `dependsOnTaskId` | UUID | no | — |
| `requiresPreviousResult` | boolean | no, default `false` | — |

Example:

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: changeme" \
  -d '{
    "fairnessKey": "tenant-123",
    "weight": 1.0,
    "payload": "aGVsbG8=",
    "sequential": false,
    "requiresPreviousResult": false
  }'
```

**Response:** `201 Created`, body is the created task's UUID as a JSON string, e.g. `"3fa85f64-5717-4562-b3fc-2c963f66afa6"`.

**Errors:** `400 VALIDATION_FAILED` (blank key, non-positive weight, null payload, sequential without sequenceNumber); `400 BAD_REQUEST` (payload over cap); `401` (no/wrong key); `500`.

**Side effects:** persists task as `RECEIVED` with no priority (priority deferred to calculator); `retry_count = 0`. Sequential tasks also `findOrCreate` a `client_sequence_state` row so the first task of a key can dispatch.

### `POST /api/v1/tasks/{taskId}/complete`

**Purpose:** completion webhook from the remote executor. Source: `TaskCompletionController.completeTask` (returns `void` → `200 OK`).

**Request** (`CompleteTaskRequest`):

| JSON field | Type | Required | Validation |
|---|---|---|---|
| `success` | boolean | no, default `false` | — |
| `result` | base64 | no | — |
| `error` | string | conditional: required non-blank when `success=false` (`@AssertTrue`, message `error must be provided when success is false`) | — |

Example:

```bash
curl -X POST http://localhost:8080/api/v1/tasks/550e8400-e29b-41d4-a716-446655440000/complete \
  -H "Content-Type: application/json" \
  -H "X-API-Key: changeme" \
  -d '{"success": true, "result": "b3V0cHV0"}'
```

**Response:** `200 OK`, empty body. Transitions to `SUCCEEDED`/`FAILED`; decrements CMS (−1) and `client_counts`; feeds adaptive RPS.

**Idempotency:** duplicate completions of an already-terminal task are **ignored** (no error, no double-decrement). Completing a non-in-flight (e.g. `QUEUED`) task is rejected (`IllegalArgumentException` → `400 BAD_REQUEST`).

**Errors:** `400 VALIDATION_FAILED` (`success=false` without `error`); `400 BAD_REQUEST` (unknown id state transition); `404 NOT_FOUND` (no task with id); `401`; `500`.

### `GET /api/v1/tasks/{taskId}`

**Purpose:** task status & progress. Source: `TaskManagementController.getTask` → `TaskStatusResponse`.

**Response** `200 OK` (`TaskStatusResponse`):

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "fairnessKey": "tenant-123",
  "status": "QUEUED",
  "priority": 1751376000123,
  "createdAt": "2026-01-01T00:00:00Z",
  "completedAt": null,
  "retryCount": 0,
  "lastError": null
}
```

`status` is one of `RECEIVED, QUEUED, DISPATCHED, COMMITTED, SUCCEEDED, FAILED, TIMEOUT`. `priority` is `null` until `QUEUED`. `completedAt` set only on terminal states.

**Errors:** `404 NOT_FOUND`; `401`; `500`.

### `GET /api/v1/tasks?fairnessKey=<key>[&status=<STATUS>]`

**Purpose:** list tasks for a fairness key. Source: `TaskManagementController.getTasksByClient`.

Query parameters:

| Param | Required | Notes |
|---|---|---|
| `fairnessKey` | yes | — |
| `status` | no | single `TaskStatus` enum filter; omit for all statuses |

**Response:** `200 OK`, array of `TaskStatusResponse` (possibly `[]`).

**Errors:** `400` on missing `fairnessKey` / invalid enum (Spring type-mismatch); `401`; `500`.

### `GET /api/v1/status`

**Purpose:** system snapshot. Source: `SystemStatusController` → `SystemStatusResponse(cms.totalInFlight(), adaptiveRpsController.getCurrentRps())`.

**Response** `200 OK`:

```json
{"inFlight": 12, "currentRps": 8.5}
```

`inFlight` is the CMS **total** estimate (`totalInFlight`; hierarchical mode reads the root `""` estimate). `currentRps` is the adaptive controller's current cap.

**Errors:** `401`; `500`.

---

## Management / operational endpoints

Spring Actuator-provided, **not** part of the behavioral contract equalix-go must reproduce field-for-field, but parity targets:

| Java | equalix-go equivalent |
|---|---|
| `GET /actuator/health`, `/actuator/info` (public) | `GET /healthz` (liveness), `GET /readyz` (readiness) |
| `GET /actuator/prometheus` (requires `X-API-Key`) | `GET /metrics` (Prometheus) |

---

## Kafka ingestion (optional path)

Source: `TaskIngestionKafkaConsumer` (`@KafkaListener(topics = "${app.kafka.topics.ingestion}"`, default topic `equalix-tasks`, group `equalix-ingestion`, `AckMode.MANUAL_IMMEDIATE`).

- **Key:** Kafka record key → `fairnessKey` (null key → `"default"`).
- **Value:** raw bytes → `payload`; always ingested with `weight = 1.0`, `sequential = false`, no sequence/dependency/passthrough.
- **Ack semantics (at-least-once):** empty payload → acknowledge + drop; success → `createTask` then acknowledge; exception → **not** acknowledged (redelivered).
- Without a broker on `localhost:9092`, the listener logs connection warnings; REST ingestion is unaffected.

equalix-go: Kafka adapter is optional and feature-flagged (Phase 2); the contract above is what it must preserve if enabled.

---

## Open questions

- [x] Base path, auth, error envelope — resolved from `SecurityConfig`, `ApiKeyAuthFilter`, `GlobalExceptionHandler` (see Conventions).
- [x] Completion idempotency — resolved: terminal duplicates ignored (`CompletionHandlerService`, `SequentialCompletionHandlerService`).
- [ ] Go actuator equivalents (`/healthz` vs `/readyz` semantics: what makes `readyz` fail — DB unreachable? Redis? both?) — deferred to EQLX-6 (operability), mark behaviour in `docs/runbook.md`.

---

## Contract test plan

Each endpoint above gets a Go integration test in `test/integration/api_*_test.go` (Phase 2): ingest → get → list → complete → duplicate-complete → status, plus 401/400/404 cases. Differential tests in `test/differential/` replay Java-observed request/response pairs against equalix-go (Phase 5).
