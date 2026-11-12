-- Token bucket, cost 1. KEYS[1] = bucket key. ARGV[1] = capacity, ARGV[2] = refill rate in tokens per second.
-- Time comes from Redis (one clock for every app instance). Arguments are validated in Java.
-- Returns {allowed (0/1), remaining (floored), retry-after in ms (0 when allowed)}.
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])

local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity          -- first sight of a key: the bucket starts full
  ts = now_ms
end

local elapsed = now_ms - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + elapsed * rate / 1000)

local allowed = 0
local retry_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_ms = math.ceil((1 - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', string.format('%.6f', tokens), 'ts', string.format('%d', now_ms))
-- expire exactly when the bucket would be full again (never less than 1 s)
local ttl_ms = math.max(1000, math.ceil(capacity * 1000 / rate))
redis.call('PEXPIRE', KEYS[1], string.format('%d', ttl_ms))

return {allowed, math.floor(tokens), retry_ms}
