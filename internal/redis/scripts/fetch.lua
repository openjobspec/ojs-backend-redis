-- fetch.lua: Atomic pop + expiry check + state transition
-- ARGV[1] = queue name
-- ARGV[2] = now_formatted (RFC3339 for started_at)
-- ARGV[3] = worker_id
-- ARGV[4] = now_ms (for visibility deadline)
-- ARGV[5] = vis_timeout_ms (request-level, "0" if not set)
-- ARGV[6] = default_vis_timeout_ms
-- Returns: {status, job_id}
--   status 0 = success
--   status 1 = no job available (queue empty)
--   status 3 = expired (job discarded, caller should retry)

local queue = ARGV[1]
local now_formatted = ARGV[2]
local worker_id = ARGV[3]
local now_ms = tonumber(ARGV[4])
local vis_timeout_ms = tonumber(ARGV[5])
local default_vis_timeout_ms = tonumber(ARGV[6])

local prefix = "ojs:"
local available_key = prefix .. "queue:" .. queue .. ":available"

-- Atomic pop from available queue (lowest score = highest priority)
local results = redis.call("ZPOPMIN", available_key, 1)
if #results == 0 then
    return {1}
end

local job_id = results[1]
local job_key = prefix .. "job:" .. job_id

-- Check if job has expired (expires_at is RFC3339 UTC, lexicographically comparable)
local expires_at = redis.call("HGET", job_key, "expires_at")
if expires_at and expires_at ~= false and expires_at ~= "" then
    if expires_at < now_formatted then
        -- Discard expired job
        redis.call("HSET", job_key, "state", "discarded")
        return {3, job_id}
    end
end

-- Determine effective visibility timeout: request > job-level > default
local effective_vis = vis_timeout_ms
if effective_vis <= 0 then
    local job_vis = redis.call("HGET", job_key, "visibility_timeout_ms")
    if job_vis and job_vis ~= false and job_vis ~= "" then
        effective_vis = tonumber(job_vis) or 0
    end
end
if effective_vis <= 0 then
    effective_vis = default_vis_timeout_ms
end

-- Update job state to active
redis.call("HSET", job_key,
    "state", "active",
    "started_at", now_formatted,
    "worker_id", worker_id)

-- Add to active set
redis.call("SADD", prefix .. "queue:" .. queue .. ":active", job_id)

-- Set visibility timeout (stored as Unix milliseconds)
redis.call("SET", prefix .. "visibility:" .. job_id, now_ms + effective_vis)

return {0, job_id}
