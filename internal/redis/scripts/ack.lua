-- ack.lua: Atomic read-validate-complete
-- ARGV[1] = jobID
-- ARGV[2] = completed_at
-- ARGV[3] = result (JSON string, empty string if none)
-- Returns: {status, queue} or {status, current_state}
--   status 0 = success
--   status 1 = not found
--   status 2 = conflict (wrong state)

local job_id = ARGV[1]
local completed_at = ARGV[2]
local result = ARGV[3]

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
redis.call("HSET", job_key, "state", "completed", "completed_at", completed_at)
if result ~= "" then
    redis.call("HSET", job_key, "result", result)
end

-- Clear error field on successful acknowledgment
redis.call("HDEL", job_key, "error")

-- Remove from active set and delete visibility key
redis.call("SREM", prefix .. "queue:" .. queue .. ":active", job_id)
redis.call("DEL", prefix .. "visibility:" .. job_id)

-- Increment completed counter
redis.call("INCR", prefix .. "queue:" .. queue .. ":completed")

return {0, queue}
