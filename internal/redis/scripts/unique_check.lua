-- unique_check.lua: Atomic unique job dedup
--
-- Enforces job uniqueness by fingerprint. Before a new job is enqueued,
-- this script checks if an existing job with the same fingerprint is in a
-- "relevant" state. The relevance check and conflict resolution happen
-- atomically to prevent race conditions where two identical jobs could
-- both pass the uniqueness check.
--
-- Unique key: ojs:unique:<fingerprint> → job_id (with TTL)
-- The TTL controls how long the uniqueness constraint is enforced.
--
-- Conflict resolution policies:
--   reject:  Return error, don't enqueue (caller returns 409 Conflict)
--   ignore:  Silently return existing job ID (idempotent enqueue)
--   replace: Update unique key to new job, caller cancels existing job
--
-- State relevance:
--   Default: any non-terminal state (not completed/discarded/cancelled)
--   Custom:  caller provides JSON array of states to check against
--
-- Pre-conditions:
--   - Fingerprint is a deterministic hash of job type + args + queue
--
-- Post-conditions (proceed):
--   - Unique key set with TTL, new job can be enqueued
-- Post-conditions (reject/ignore):
--   - No changes to unique key, existing job returned
-- Post-conditions (replace):
--   - Unique key updated to new job ID, existing job ID returned for cancellation
--
-- Atomicity: GET + state check + SET are atomic. Two concurrent enqueues
-- with the same fingerprint cannot both get "proceed" — one will always
-- see the other's unique key.
--
-- ARGV[1] = fingerprint
-- ARGV[2] = new_job_id
-- ARGV[3] = ttl_seconds
-- ARGV[4] = conflict_policy ("reject", "ignore", "replace")
-- ARGV[5] = states_json (JSON array of states, empty string for default behavior)
-- Returns: {status, action, [existing_job_id, [fingerprint]]}
--   status 0 = success
--   action values:
--     "proceed" = no conflict, unique key set for the new job
--     "reject"  = duplicate exists, reject new job (extra: existing_id, fingerprint)
--     "ignore"  = duplicate exists, return existing job (extra: existing_id)
--     "replace" = duplicate exists, cancel existing and proceed (extra: existing_id)

local fingerprint = ARGV[1]
local new_job_id = ARGV[2]
local ttl_seconds = tonumber(ARGV[3])
local conflict_policy = ARGV[4]
local states_json = ARGV[5]

local prefix = "ojs:"
local unique_key = prefix .. "unique:" .. fingerprint

-- Check for existing unique job
local existing_id = redis.call("GET", unique_key)
if not existing_id or existing_id == false then
    -- No existing job, set unique key and proceed
    redis.call("SET", unique_key, new_job_id, "EX", ttl_seconds)
    return {0, "proceed"}
end

-- Check existing job's state
local existing_state = redis.call("HGET", prefix .. "job:" .. existing_id, "state")
if not existing_state or existing_state == false then
    -- Existing job no longer exists, set unique key and proceed
    redis.call("SET", unique_key, new_job_id, "EX", ttl_seconds)
    return {0, "proceed"}
end

-- Check if the existing job's state is relevant
local is_relevant = false
if states_json ~= "" then
    -- Check if existing_state appears in the states JSON array
    if string.find(states_json, '"' .. existing_state .. '"') then
        is_relevant = true
    end
else
    -- Default: any non-terminal state is relevant
    if existing_state ~= "completed" and existing_state ~= "discarded" and existing_state ~= "cancelled" then
        is_relevant = true
    end
end

if not is_relevant then
    -- Existing job is not in a relevant state, set unique key and proceed
    redis.call("SET", unique_key, new_job_id, "EX", ttl_seconds)
    return {0, "proceed"}
end

-- Conflict detected
if conflict_policy == "reject" then
    return {0, "reject", existing_id, fingerprint}
elseif conflict_policy == "ignore" then
    return {0, "ignore", existing_id}
elseif conflict_policy == "replace" then
    -- Set the unique key to the new job ID (actual cancel stays in Go)
    redis.call("SET", unique_key, new_job_id, "EX", ttl_seconds)
    return {0, "replace", existing_id}
end

-- Unknown policy, default to reject
return {0, "reject", existing_id, fingerprint}
