-- requeue_stalled.lua: Atomic visibility check + requeue (per job)
-- ARGV[1] = jobID
-- ARGV[2] = queue
-- ARGV[3] = now_formatted
-- ARGV[4] = now_ms
-- Returns: {status}
--   status 0 = requeued successfully
--   status 1 = not stalled (visibility not expired or no visibility key)
--   status 2 = job not found or not in active state

local job_id = ARGV[1]
local queue = ARGV[2]
local now_formatted = ARGV[3]
local now_ms = tonumber(ARGV[4])

local prefix = "ojs:"
local job_key = prefix .. "job:" .. job_id
local vis_key = prefix .. "visibility:" .. job_id

-- Check visibility timeout (stored as Unix milliseconds)
local vis_deadline = redis.call("GET", vis_key)
if not vis_deadline or vis_deadline == false then
    return {1}
end

local deadline_ms = tonumber(vis_deadline)
if now_ms <= deadline_ms then
    return {1}
end

-- Verify job is still active
local state = redis.call("HGET", job_key, "state")
if not state or state ~= "active" then
    return {2}
end

-- Compute score for priority ordering
local priority_str = redis.call("HGET", job_key, "priority")
local priority = tonumber(priority_str) or 0
local score = (100 - priority) * 1e15 + now_ms

-- Requeue: remove from active, update state, add to available, delete visibility
redis.call("SREM", prefix .. "queue:" .. queue .. ":active", job_id)
redis.call("HSET", job_key,
    "state", "available",
    "started_at", "",
    "worker_id", "",
    "enqueued_at", now_formatted)
redis.call("ZADD", prefix .. "queue:" .. queue .. ":available", score, job_id)
redis.call("DEL", vis_key)

return {0}
