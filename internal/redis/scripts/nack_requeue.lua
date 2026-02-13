-- nack_requeue.lua: Atomic requeue path
-- ARGV[1] = jobID
-- ARGV[2] = enqueued_at
-- ARGV[3] = now_ms (for score computation)
-- Returns: {status, queue} or {status, current_state}
--   status 0 = success
--   status 1 = not found
--   status 2 = conflict (wrong state)

local job_id = ARGV[1]
local enqueued_at = ARGV[2]
local now_ms = tonumber(ARGV[3])

local prefix = "ojs:"
local job_key = prefix .. "job:" .. job_id

-- Read current state and queue
local state = redis.call("HGET", job_key, "state")
if not state or state == false then
    return {1}
end

if state ~= "active" then
    return {2, state}
end

local queue = redis.call("HGET", job_key, "queue")
local priority_str = redis.call("HGET", job_key, "priority")
local priority = tonumber(priority_str) or 0
local score = (100 - priority) * 1e15 + now_ms

-- Update job state
redis.call("HSET", job_key,
    "state", "available",
    "started_at", "",
    "worker_id", "",
    "enqueued_at", enqueued_at)

-- Remove from active, delete visibility
redis.call("SREM", prefix .. "queue:" .. queue .. ":active", job_id)
redis.call("DEL", prefix .. "visibility:" .. job_id)

-- Add to available queue
redis.call("ZADD", prefix .. "queue:" .. queue .. ":available", score, job_id)

return {0, queue}
