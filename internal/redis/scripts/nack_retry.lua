-- nack_retry.lua: Atomic retry path
--
-- Schedules a failed job for retry. Transitions to "retryable" state,
-- stores the error and full error history, increments the attempt counter,
-- and adds the job to the retry sorted set with a score representing the
-- next attempt time (computed by the caller using backoff strategy).
-- The promote.lua script later moves due retries back to available.
--
-- State transition: active → retryable
--
-- Pre-conditions:
--   - Job hash exists with state = "active"
--   - Caller has computed retry delay and next_attempt_at_ms
--   - Retries remaining (not exhausted — nack_discard handles that case)
--
-- Post-conditions:
--   - Job state = "retryable", attempt incremented, retry_delay_ms stored
--   - Error and error_history stored on job hash
--   - Removed from active set, visibility key deleted
--   - Added to ojs:retry ZSET with score = next_attempt_at_ms
--
-- Atomicity: Error recording + state change + retry scheduling are atomic.
-- Prevents duplicate retry entries or lost error history.
--
-- ARGV[1] = jobID
-- ARGV[2] = new_attempt (string)
-- ARGV[3] = error_json (empty string if none)
-- ARGV[4] = error_history_json
-- ARGV[5] = retry_delay_ms (string)
-- ARGV[6] = next_attempt_at_ms (score for retry sorted set)
-- Returns: {status, queue} or {status, current_state}
--   status 0 = success
--   status 1 = not found
--   status 2 = conflict (wrong state)

local job_id = ARGV[1]
local new_attempt = ARGV[2]
local error_json = ARGV[3]
local error_history_json = ARGV[4]
local retry_delay_ms = ARGV[5]
local next_attempt_at_ms = tonumber(ARGV[6])

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
    "state", "retryable",
    "error_history", error_history_json,
    "attempt", new_attempt,
    "retry_delay_ms", retry_delay_ms)

if error_json ~= "" then
    redis.call("HSET", job_key, "error", error_json)
end

-- Remove from active, delete visibility
redis.call("SREM", prefix .. "queue:" .. queue .. ":active", job_id)
redis.call("DEL", prefix .. "visibility:" .. job_id)

-- Add to retry sorted set
redis.call("ZADD", prefix .. "retry", next_attempt_at_ms, job_id)

return {0, queue}
