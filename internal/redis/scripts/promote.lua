-- promote.lua: Atomic batch promotion from sorted set
-- KEYS[1] = source sorted set key (e.g. ojs:scheduled or ojs:retry)
-- ARGV[1] = now_ms (entries with score <= now_ms are due)
-- ARGV[2] = now_formatted (for enqueued_at timestamp)
-- Returns: {0, count}

local source_key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local now_formatted = ARGV[2]

local prefix = "ojs:"
local count = 0

-- Get all due entries
local ids = redis.call("ZRANGEBYSCORE", source_key, "0", tostring(now_ms))

for _, job_id in ipairs(ids) do
    local data = redis.call("HGETALL", prefix .. "job:" .. job_id)
    if #data == 0 then
        -- Orphan entry, remove from source
        redis.call("ZREM", source_key, job_id)
    else
        -- Parse hash
        local job = {}
        for i = 1, #data, 2 do
            job[data[i]] = data[i + 1]
        end

        local queue = job["queue"]
        local priority = tonumber(job["priority"]) or 0
        local score = (100 - priority) * 1e15 + now_ms

        -- Remove from source
        redis.call("ZREM", source_key, job_id)

        -- Update state to available
        redis.call("HSET", prefix .. "job:" .. job_id,
            "state", "available",
            "enqueued_at", now_formatted)

        -- Add to available queue
        redis.call("ZADD", prefix .. "queue:" .. queue .. ":available", score, job_id)

        count = count + 1
    end
end

return {0, count}
