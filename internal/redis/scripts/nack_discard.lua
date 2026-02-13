-- nack_discard.lua: Atomic discard path
-- ARGV[1] = jobID
-- ARGV[2] = new_attempt (string)
-- ARGV[3] = error_json (empty string if none)
-- ARGV[4] = error_history_json
-- ARGV[5] = completed_at
-- ARGV[6] = on_exhaustion ("discard" or "dead_letter")
-- ARGV[7] = now_ms (for dead letter score)
-- Returns: {status, queue} or {status, current_state}
--   status 0 = success
--   status 1 = not found
--   status 2 = conflict (wrong state)

local job_id = ARGV[1]
local new_attempt = ARGV[2]
local error_json = ARGV[3]
local error_history_json = ARGV[4]
local completed_at = ARGV[5]
local on_exhaustion = ARGV[6]
local now_ms = tonumber(ARGV[7])

local prefix = "ojs:"
local job_key = prefix .. "job:" .. job_id

-- Read current state
local state = redis.call("HGET", job_key, "state")
if not state or state == false then
    return {1}
end

if state ~= "active" then
    return {2, state}
end

local queue = redis.call("HGET", job_key, "queue")

-- Update job state
redis.call("HSET", job_key,
    "state", "discarded",
    "completed_at", completed_at,
    "error_history", error_history_json,
    "attempt", new_attempt)

if error_json ~= "" then
    redis.call("HSET", job_key, "error", error_json)
end

-- Remove from active, delete visibility
redis.call("SREM", prefix .. "queue:" .. queue .. ":active", job_id)
redis.call("DEL", prefix .. "visibility:" .. job_id)

-- Add to dead letter queue if configured
if on_exhaustion == "dead_letter" then
    redis.call("ZADD", prefix .. "dead", now_ms, job_id)
end

return {0, queue}
