-- advance_workflow.lua: Atomic counter increment + state check
-- ARGV[1] = workflowID
-- ARGV[2] = failed_flag ("1" if failed, "0" if success)
-- ARGV[3] = step_idx (string, the index of the completed step)
-- ARGV[4] = result (JSON string, empty string if none)
-- ARGV[5] = now_formatted
-- Returns: {status, action, ...}
--   status 0 = success
--   status 1 = workflow not found
--   action values:
--     "in_progress" = workflow still running, nothing to do
--     "chain_next"  = enqueue next chain step (extra: next_step_idx)
--     "chain_failed" = chain failed, workflow marked failed
--     "completed"   = workflow completed successfully
--     "failed"      = group/batch workflow failed
--     "batch_completed" = batch completed (fire callbacks, extra: failed_count)
--     "batch_failed"    = batch failed (fire callbacks, extra: failed_count)

local workflow_id = ARGV[1]
local is_failed = ARGV[2] == "1"
local step_idx = ARGV[3]
local result = ARGV[4]
local now_formatted = ARGV[5]

local prefix = "ojs:"
local wf_key = prefix .. "workflow:" .. workflow_id

-- Read workflow data
local data = redis.call("HGETALL", wf_key)
if #data == 0 then
    return {1}
end

-- Parse hash
local wf = {}
for i = 1, #data, 2 do
    wf[data[i]] = data[i + 1]
end

-- Only advance running workflows
if wf["state"] ~= "running" then
    return {0, "in_progress"}
end

local wf_type = wf["type"]
local total = tonumber(wf["total"])
local completed = tonumber(wf["completed"])
local failed_count = tonumber(wf["failed"])

-- Store result for chain result passing
if result ~= "" then
    redis.call("HSET", wf_key .. ":results", step_idx, result)
end

-- Increment counters atomically
if is_failed then
    failed_count = failed_count + 1
    redis.call("HSET", wf_key, "failed", tostring(failed_count))
else
    completed = completed + 1
    redis.call("HSET", wf_key, "completed", tostring(completed))
end

local total_finished = completed + failed_count

if wf_type == "chain" then
    if is_failed then
        -- Chain stops on failure
        redis.call("HSET", wf_key,
            "state", "failed",
            "completed_at", now_formatted)
        return {0, "chain_failed"}
    end

    if total_finished >= total then
        -- Chain complete
        redis.call("HSET", wf_key,
            "state", "completed",
            "completed_at", now_formatted)
        return {0, "completed"}
    end

    -- Need to enqueue next step
    return {0, "chain_next", tostring(tonumber(step_idx) + 1)}
end

-- Group/Batch
if total_finished >= total then
    local final_state = "completed"
    if failed_count > 0 then
        final_state = "failed"
    end
    redis.call("HSET", wf_key,
        "state", final_state,
        "completed_at", now_formatted)

    -- For batch workflows, return action to fire callbacks
    if wf_type == "batch" then
        return {0, "batch_" .. final_state, tostring(failed_count)}
    end
    return {0, final_state}
end

return {0, "in_progress"}
