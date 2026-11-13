import uuid

from tests.conftest import auth, db_scalar, outbox_topics, post_booking


async def test_create_booking_happy_path(app, client, show_id):
    r = await post_booking(client, "alice", show_id, ["a2", "A1"])  # lower-case + unsorted on purpose

    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "AWAITING_PAYMENT"
    assert body["seats"] == ["A1", "A2"]  # normalised and sorted
    assert body["amount"] == "200.00"
    # Every layer agrees:
    assert await db_scalar(app, "SELECT count(*) FROM show_seat WHERE status='HELD'") == 2
    assert await app.state.redis.exists(f"seat:{show_id}:A1") == 1
    assert await outbox_topics(app) == ["booking.created"]


async def test_second_user_gets_409_naming_the_conflicting_seat(client, show_id):
    await post_booking(client, "alice", show_id, ["A1", "A2"])
    r = await post_booking(client, "bob", show_id, ["A2", "A3"])

    assert r.status_code == 409
    assert r.json()["code"] == "SEATS_UNAVAILABLE"
    assert r.json()["conflictingSeats"] == ["A2"]
    assert r.json()["traceId"]


async def test_failed_acquire_leaves_no_partial_hold(app, client, show_id):
    await post_booking(client, "alice", show_id, ["A2"])
    await post_booking(client, "bob", show_id, ["A2", "A3"])  # fails on A2
    assert await app.state.redis.exists(f"seat:{show_id}:A3") == 0  # all-or-nothing


async def test_unknown_seat_is_404_and_releases_redis_hold(app, client, show_id):
    r = await post_booking(client, "alice", show_id, ["Z9"])
    assert r.status_code == 404
    assert r.json()["code"] == "SEAT_NOT_FOUND"
    assert await app.state.redis.exists(f"seat:{show_id}:Z9") == 0


async def test_requires_auth(client, show_id):
    r = await client.post(
        "/api/v1/bookings",
        json={"showId": str(show_id), "seatIds": ["A1"]},
        headers={"Idempotency-Key": "k" * 10},
    )
    assert r.status_code == 401
    assert r.json()["code"] == "UNAUTHORIZED"


async def test_invalid_seat_id_is_422_with_envelope(client, show_id):
    r = await post_booking(client, "alice", show_id, ["not a seat"])
    assert r.status_code == 422
    assert r.json()["code"] == "VALIDATION_ERROR"


async def test_idempotent_retry_returns_same_booking_and_creates_only_one(app, client, show_id):
    first = await post_booking(client, "alice", show_id, ["A1"], key="retry-key-0001")
    second = await post_booking(client, "alice", show_id, ["A1"], key="retry-key-0001")

    assert first.status_code == second.status_code == 201
    assert first.json()["bookingId"] == second.json()["bookingId"]
    assert second.headers["idempotent-replayed"] == "true"
    assert await db_scalar(app, "SELECT count(*) FROM booking") == 1
    assert await outbox_topics(app) == ["booking.created"]  # one event, not two


async def test_same_key_with_different_request_is_rejected(client, show_id):
    await post_booking(client, "alice", show_id, ["A1"], key="reused-key-001")
    r = await post_booking(client, "alice", show_id, ["A2"], key="reused-key-001")
    assert r.status_code == 422
    assert r.json()["code"] == "IDEMPOTENCY_KEY_REUSED"


async def test_failed_attempt_does_not_burn_the_idempotency_key(client, show_id):
    first = await post_booking(client, "alice", show_id, ["A1"])
    assert (await post_booking(client, "bob", show_id, ["A1"], key="bob-retry-key1")).status_code == 409

    await client.post(f"/api/v1/bookings/{first.json()['bookingId']}/cancel", headers=auth("alice"))

    # Bob's retry with the SAME key now works: the failed attempt released its claim.
    assert (await post_booking(client, "bob", show_id, ["A1"], key="bob-retry-key1")).status_code == 201


async def test_only_the_owner_can_read_a_booking(client, show_id):
    booking_id = (await post_booking(client, "alice", show_id, ["A1"])).json()["bookingId"]
    assert (await client.get(f"/api/v1/bookings/{booking_id}", headers=auth("alice"))).status_code == 200
    assert (await client.get(f"/api/v1/bookings/{booking_id}", headers=auth("bob"))).status_code == 404
    assert (await client.get(f"/api/v1/bookings/{uuid.uuid4()}", headers=auth("alice"))).status_code == 404


async def test_cancel_releases_seats_everywhere(app, client, show_id):
    booking_id = (await post_booking(client, "alice", show_id, ["A1"])).json()["bookingId"]

    r = await client.post(f"/api/v1/bookings/{booking_id}/cancel", headers=auth("alice"))

    assert r.json()["status"] == "CANCELLED"
    assert await db_scalar(app, "SELECT status FROM show_seat WHERE seat_id='A1'") == "AVAILABLE"
    assert await db_scalar(app, "SELECT status FROM booking_seat") == "RELEASED"
    assert await app.state.redis.exists(f"seat:{show_id}:A1") == 0
    assert (await post_booking(client, "bob", show_id, ["A1"])).status_code == 201  # seat is bookable again
    assert "booking.cancelled" in await outbox_topics(app)


async def test_cannot_cancel_twice(client, show_id):
    booking_id = (await post_booking(client, "alice", show_id, ["A1"])).json()["bookingId"]
    await client.post(f"/api/v1/bookings/{booking_id}/cancel", headers=auth("alice"))
    r = await client.post(f"/api/v1/bookings/{booking_id}/cancel", headers=auth("alice"))
    assert r.status_code == 409
    assert r.json()["code"] == "INVALID_BOOKING_STATE"


async def test_seat_map(client, show_id):
    await post_booking(client, "alice", show_id, ["A1"])
    r = await client.get(f"/api/v1/shows/{show_id}/seats")
    seats = {s["seatId"]: s["status"] for s in r.json()}
    assert seats["A1"] == "HELD" and seats["A2"] == "AVAILABLE"
    assert [s["seatId"] for s in r.json()][:3] == ["A1", "A2", "A3"]
    assert (await client.get(f"/api/v1/shows/{uuid.uuid4()}/seats")).status_code == 404


async def test_health(client):
    r = await client.get("/health/ready")
    assert r.status_code == 200 and r.json() == {"status": "ok", "postgres": "up", "redis": "up"}
