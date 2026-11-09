# Booking Service — Design Document

**Status:** Draft — for review
**Owner:** <your name>
**Last updated:** 2026-09-18

---

## 1. Purpose

Booking Service owns the lifecycle of a ticket booking: holding seats while a user pays,
confirming the booking once payment succeeds, and releasing seats when payment fails or
the user abandons checkout.

It is the only service allowed to write seat inventory. Everything else reads.

### Non-goals

- Real payment processing (payment-service is a mock).
- Seat pricing rules beyond a flat price per seat class.
- Refunds and partial cancellation.
- Any UI. Swagger UI is the demo surface.

---

## 2. Why this service is hard

Two users tap the same seat at the same moment. Both requests are in flight. One must win.

That is the entire problem, and every design decision below exists to answer it. A naive
`SELECT ... then INSERT` loses the race under concurrency, so we need a lock. But a lock in
Redis is not a correctness guarantee — Redis can lose a key on failover, and a hold can
expire in the millisecond between the check and the write. So Redis gives us *speed and
user experience*, and the database gives us *correctness*.

**Rule to remember for interviews:** the fast path prevents contention, the database
prevents corruption. You need both, and you should be able to say why in one sentence.

---

## 3. Domain model

| Entity | Notes |
|---|---|
| `Show` | Replicated read-model from catalog-service (id, movie, theatre, screen, starts_at). Kept in sync via Kafka. |
| `ShowSeat` | One row per (show, seat). This is the inventory. Status: `AVAILABLE`, `HELD`, `BOOKED`. |
| `Booking` | One per checkout attempt. Has a state machine (§4). |
| `BookingSeat` | Join row: which seats belong to which booking. |
| `OutboxEvent` | Events pending publication to Kafka (§8). |
| `IdempotencyRecord` | Dedupes retried client requests (§7). |

### Why booking-service owns `ShowSeat`

catalog-service owns the *seat layout* (this screen has rows A–J, 20 seats each, row A is
recliner). booking-service owns *seat state for a given show*. Splitting it this way means
inventory writes never cross a service boundary, which is what makes the locking tractable.

This is a deliberate decision and a likely interview question. The alternative — catalog
owning inventory — forces a distributed transaction on every booking. We avoid it.

---

## 4. Booking state machine

```
                    ┌──────────────────────────────────────┐
                    │                                      │
  [create] ──► PENDING ──► AWAITING_PAYMENT ──► CONFIRMED  │
                  │               │                        │
                  │               ├──► PAYMENT_FAILED ─────┤
                  │               │                        │
                  └──► EXPIRED ◄──┘                        │
                                                     (terminal)
```

| State | Meaning | Seats |
|---|---|---|
| `PENDING` | Row written, seats being held | being acquired |
| `AWAITING_PAYMENT` | Hold acquired, waiting on payment-service | `HELD` |
| `CONFIRMED` | Payment succeeded | `BOOKED` |
| `PAYMENT_FAILED` | Payment declined | released to `AVAILABLE` |
| `EXPIRED` | Hold TTL elapsed with no payment result | released to `AVAILABLE` |

Transitions are guarded — a `CONFIRMED` booking can never move to `EXPIRED`, even if a
late sweeper fires. Enforce this in code *and* with a check constraint mindset, because the
expiry sweeper and the payment callback genuinely do race in production.

---

## 5. Seat hold design

### Redis keys

```
Key:   seat:{showId}:{seatId}
Value: {holdToken}          # UUID unique to this booking attempt
TTL:   480000 ms (8 minutes)
```

### Acquire (all seats, atomically)

Use a **Lua script** so the multi-seat acquire is all-or-nothing. Doing N separate
`SET NX` calls means you can acquire 3 of 4 seats and have to unwind — messy and racy.

```lua
-- KEYS: seat keys;  ARGV[1]: holdToken, ARGV[2]: ttlMillis
for i = 1, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    return i          -- index of the first conflicting seat
  end
end
for i = 1, #KEYS do
  redis.call('SET', KEYS[i], ARGV[1], 'PX', ARGV[2])
end
return 0              -- success
```

### Release

Never plain `DEL`. Compare the token first, or you will delete a hold that a *different*
booking acquired after yours expired:

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
```

This "check the token before releasing" point is worth understanding deeply. It is the same
reasoning as the correct implementation of a distributed lock, and it comes up constantly in
interviews.

### The database backstop

```sql
-- inside one transaction, after the Redis hold succeeds
SELECT * FROM show_seat
 WHERE show_id = ? AND seat_id IN (?)
   FOR UPDATE;               -- row locks, consistent ordering by seat_id to avoid deadlock
-- verify every row is still AVAILABLE, else roll back and release the Redis hold
UPDATE show_seat SET status = 'HELD', booking_id = ? WHERE ...;
```

Plus a partial unique index so corruption is impossible even if all the application logic
above is wrong:

```sql
CREATE UNIQUE INDEX uq_show_seat_active
  ON booking_seat (show_id, seat_id)
  WHERE status IN ('HELD', 'BOOKED');
```

**Always order seat IDs before locking.** Two concurrent transactions locking the same rows
in opposite orders deadlock. Sorting is a one-line fix for a bug that is miserable to
diagnose.

---

## 6. API contract

Base path: `/api/v1/bookings`. All routes require a JWT; `userId` comes from the token,
never from the request body.

### `POST /api/v1/bookings`

Header: `Idempotency-Key: <uuid>` (required)

```json
{ "showId": "...", "seatIds": ["A1", "A2"] }
```

`201`
```json
{
  "bookingId": "...",
  "status": "AWAITING_PAYMENT",
  "seats": ["A1", "A2"],
  "amount": 500.00,
  "holdExpiresAt": "2026-09-18T10:38:00Z"
}
```

`409 SEATS_UNAVAILABLE`
```json
{ "code": "SEATS_UNAVAILABLE", "conflictingSeats": ["A2"] }
```

### `GET /api/v1/bookings/{id}`
Returns current state. The client polls this after initiating payment.

### `POST /api/v1/bookings/{id}/cancel`
Only valid from `AWAITING_PAYMENT`. Releases the hold immediately.

### `GET /api/v1/shows/{showId}/seats`
Seat map with live availability. Cached in Redis, 5s TTL — this is the read-heavy endpoint
and a good place to talk about cache invalidation trade-offs.

### Error envelope

Every error uses the same shape, via `@RestControllerAdvice`:
```json
{ "code": "...", "message": "...", "traceId": "...", "timestamp": "..." }
```

---

## 7. Idempotency

The client retries. Networks drop responses. Without this, one user creates three bookings.

On `POST /bookings`:
1. Try `INSERT INTO idempotency_record (key, user_id, status) VALUES (?, ?, 'IN_PROGRESS')`.
2. Unique violation → a request with this key already ran. If it finished, return the stored
   response body verbatim. If still `IN_PROGRESS`, return `409 REQUEST_IN_FLIGHT`.
3. On success, store the response body and mark `COMPLETED`.

The same pattern protects the payment callback — payment gateways retry webhooks, and
confirming a booking twice must be harmless.

---

## 8. Events and the outbox

### The problem

```java
bookingRepository.save(booking);   // committed
kafkaTemplate.send(event);         // process dies here — event lost forever
```

Two systems, no shared transaction. The booking exists and nothing downstream knows.

### The fix

Write the event into an `outbox_event` table **in the same transaction as the booking**.
A separate poller (`@Scheduled`, every 500ms) reads unpublished rows, publishes to Kafka,
marks them sent. Now the write is atomic, and delivery is at-least-once — which is exactly
why every consumer must be idempotent (§7).

This is the transactional outbox pattern. Implementing it takes about half a day and it is
one of the strongest things you can have on a resume project.

### Topics

| Topic | Producer | Key | Consumers |
|---|---|---|---|
| `booking.created` | booking | bookingId | payment, notification |
| `payment.succeeded` | payment | bookingId | booking, notification |
| `payment.failed` | payment | bookingId | booking, notification |
| `booking.confirmed` | booking | bookingId | notification |
| `booking.expired` | booking | bookingId | notification |

Partition by `bookingId` so all events for one booking land on one partition and stay
ordered. Worth being able to explain.

### Saga (choreography)

```
booking.created ──► payment-service charges
                       │
        ┌──────────────┴──────────────┐
  payment.succeeded              payment.failed
        │                              │
  seats → BOOKED                 seats → AVAILABLE
  status → CONFIRMED             status → PAYMENT_FAILED
  booking.confirmed              release Redis holds
```

No 2PC anywhere. Compensation is "release the seats", which is cheap and safe to repeat.

---

## 9. Expiry sweeper

Redis TTL frees the *lock*, but the database row still says `HELD`. A `@Scheduled` job every
30 seconds finds bookings in `AWAITING_PAYMENT` past `hold_expires_at` and expires them.

Use `SELECT ... FOR UPDATE SKIP LOCKED` so multiple instances of booking-service can run the
sweeper concurrently without fighting. Guard the transition — if payment confirmed the
booking a moment ago, the sweeper must lose.

---

## 10. Resilience

- **Circuit breaker** (Resilience4j) on any synchronous outbound call.
- **Retry with exponential backoff + jitter** on Kafka consumers; after N attempts, route to
  a **dead letter topic**. Never retry forever, never drop silently.
- **Timeouts on everything.** An unbounded call is an outage waiting to happen.

---

## 11. Package structure

```
booking-service/app/
├── api/          routers (FastAPI), Pydantic request/response schemas, exception handlers
├── domain/       SQLAlchemy models, enums, state machine
├── repository/   query functions (thin — SQLAlchemy async session wrappers)
├── service/      booking_service.py, seat_hold_service.py, expiry_service.py
├── messaging/    kafka producers/consumers, outbox poller
├── core/         config, redis client, db session, security (JWT)
└── lua/          acquire.lua, release.lua
alembic/           migration scripts
tests/
```

Layered, not hexagonal — same reasoning as before: hexagonal buys architectural purity you
won't need to defend in three weeks, and layered is what most FastAPI codebases in the wild
actually look like.

### Stack

- **FastAPI** — routing, dependency injection, automatic OpenAPI (this is your Swagger demo)
- **SQLAlchemy 2.0 (async)** + **asyncpg** — ORM and driver; async end-to-end matters because
  a sync DB call inside an `async def` route blocks the whole event loop
- **Alembic** — migrations
- **Pydantic v2** — request/response validation
- **redis.asyncio** — seat holds, cache; Lua scripts via `redis.register_script`
- **aiokafka** — outbox publishing, saga event consumers
- **APScheduler** — expiry sweeper (`AsyncIOScheduler`, interval trigger)
- **tenacity** — retry with backoff on outbound calls and Kafka consumers
- **testcontainers-python** + **pytest-asyncio** — integration tests against real Postgres/Redis/Kafka
- **structlog** — structured JSON logs with correlation ID

### No Eureka

Service discovery is dropped. Six services behind Docker Compose's built-in DNS (each
service reachable by its Compose service name) is simpler and correct at this scale. Worth
stating explicitly in the README as a deliberate choice, not an oversight — interviewers
will sometimes probe exactly this to see if you understand *when* you'd need discovery
(dynamic scaling, many instances per service, a service mesh) versus when you wouldn't.

---

## 12. Definition of done

- [ ] Flyway migrations; schema created on startup
- [ ] All five endpoints working end-to-end via Swagger
- [ ] Redis Lua acquire/release with token comparison
- [ ] Outbox table + poller publishing to Kafka
- [ ] Idempotency on create and on payment callback
- [ ] Expiry sweeper with `SKIP LOCKED`
- [ ] **Concurrency test**: 50 threads book the same seat; exactly one succeeds
- [ ] Testcontainers integration tests (Postgres, Redis, Kafka)
- [ ] Structured JSON logs with a correlation ID on every line
- [ ] `/actuator/health` with readiness and liveness probes

The concurrency test is not optional. It is the proof that the design works, and it is the
first thing worth showing an interviewer.

---

## 13. Open questions

1. **Hold TTL** — 8 minutes assumed. BookMyShow uses roughly 5–10.
2. **Seat map staleness** — 5s cache means a user may tap a seat that was just taken. The
   `409` handles it. Acceptable, or do we want WebSocket push for live seat updates?
   (Nice demo, but scope risk in three weeks.)
3. **`Show` replication** — replicate into booking's schema via Kafka, or call catalog
   synchronously? Replication is more correct and more interesting to discuss; sync calls
   are faster to build.

---

## 14. Monorepo layout

```
bookmyshow-microservices/
├── docs/
│   ├── architecture.md
│   ├── booking-service-design.md   ← this file
│   └── adr/                   architecture decision records
├── common/                    shared Pydantic event schemas, error envelope (pip-installable local package)
├── booking-service/
│   ├── app/
│   ├── alembic/
│   ├── tests/
│   ├── pyproject.toml
│   └── Dockerfile
├── auth-service/
├── catalog-service/
├── payment-service/
├── notification-service/
├── api-gateway/
├── docker/docker-compose.yml
└── .github/workflows/ci.yml
```

Each service gets its own `pyproject.toml` and virtualenv — services stay independently
deployable, which is the whole point of the exercise. `common` stays small: shared event
schemas and the error envelope only. The moment shared business logic lands in there, the
services are coupled and the exercise loses its point.
