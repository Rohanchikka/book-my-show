-- Release seat holds, but ONLY the ones that still belong to this token.
--
-- Why compare the token? Scenario: my hold expires, someone else acquires the seat, then my
-- slow cleanup runs. A plain DEL would delete THEIR hold and let a third person in.
-- Compare-and-delete must be atomic, hence Lua.
--
-- KEYS: seat keys; ARGV[1]: hold token
-- Returns: number of keys actually deleted.
local released = 0
for i = 1, #KEYS do
  if redis.call('GET', KEYS[i]) == ARGV[1] then
    released = released + redis.call('DEL', KEYS[i])
  end
end
return released
