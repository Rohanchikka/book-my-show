-- Acquire holds on ALL seats, or on none.
--
-- Redis runs a Lua script atomically: no other command can run between the check loop and
-- the set loop. That is what makes the multi-seat acquire all-or-nothing without any
-- unwinding logic. (N separate SET NX calls could grab 3 of 4 seats, then have to undo.)
--
-- KEYS: one key per seat, e.g. seat:{showId}:{seatId}
-- ARGV[1]: hold token (unique per booking attempt)
-- ARGV[2]: TTL in milliseconds
-- Returns: empty list on success, otherwise the list of keys that were already held.
local conflicts = {}
for i = 1, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    conflicts[#conflicts + 1] = KEYS[i]
  end
end
if #conflicts > 0 then
  return conflicts
end
for i = 1, #KEYS do
  redis.call('SET', KEYS[i], ARGV[1], 'PX', ARGV[2])
end
return conflicts
