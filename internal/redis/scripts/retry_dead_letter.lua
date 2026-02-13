-- retry_dead_letter.lua: Atomic DLQ retry
-- ARGV[1] = jobID
-- ARGV[2] = enqueued_at
-- ARGV[3] = now_ms (for available queue score)
-- Returns: {status}
--   status 0 = success
--   status 1 = not found in dead letter queue

local job_id = ARGV[1]
local enqueued_at = ARGV[2]
local now_ms = tonumber(ARGV[3])

local prefix = "ojs:"
local job_key = prefix .. "job:" .. job_id
local dead_key = prefix .. "dead"

-- Check if in dead letter queue
local score = redis.call("ZSCORE", dead_key, job_id)
if not score or score == false then
    return {1}
end

-- Get queue from job hash
local queue = redis.call("HGET", job_key, "queue")
if not queue or queue == false then
    return {1}
end

-- Compute score for available queue
local priority_str = redis.call("HGET", job_key, "priority")
local priority = tonumber(priority_str) or 0
local available_score = (100 - priority) * 1e15 + now_ms

-- Remove from dead letter queue
redis.call("ZREM", dead_key, job_id)

-- Update job state
redis.call("HSET", job_key,
    "state", "available",
    "attempt", "0",
    "enqueued_at", enqueued_at)

-- Clear error-related fields and completion timestamp
redis.call("HDEL", job_key, "error", "error_history", "completed_at", "retry_delay_ms")

-- Add to available queue
redis.call("ZADD", prefix .. "queue:" .. queue .. ":available", available_score, job_id)

return {0}
