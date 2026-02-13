-- cancel.lua: Atomic read-validate-cancel
-- ARGV[1] = jobID
-- ARGV[2] = cancelled_at
-- Returns: {status, queue} or {status, current_state}
--   status 0 = success
--   status 1 = not found
--   status 2 = conflict (terminal state)

local job_id = ARGV[1]
local cancelled_at = ARGV[2]

local prefix = "ojs:"
local job_key = prefix .. "job:" .. job_id

-- Read current job data
local data = redis.call("HGETALL", job_key)
if #data == 0 then
    return {1}
end

-- Parse hash into table
local job = {}
for i = 1, #data, 2 do
    job[data[i]] = data[i + 1]
end

local state = job["state"]

-- Check for terminal states
if state == "completed" or state == "discarded" or state == "cancelled" then
    return {2, state}
end

local queue = job["queue"]

-- Update job state
redis.call("HSET", job_key, "state", "cancelled", "cancelled_at", cancelled_at)

-- Remove from all possible sets
redis.call("ZREM", prefix .. "queue:" .. queue .. ":available", job_id)
redis.call("SREM", prefix .. "queue:" .. queue .. ":active", job_id)
redis.call("ZREM", prefix .. "scheduled", job_id)
redis.call("ZREM", prefix .. "retry", job_id)
redis.call("DEL", prefix .. "visibility:" .. job_id)

return {0, queue}
