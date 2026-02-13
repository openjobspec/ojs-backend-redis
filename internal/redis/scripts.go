package redis

import (
	_ "embed"

	redisclient "github.com/redis/go-redis/v9"
)

// Lua script sources embedded from files.
var (
	//go:embed scripts/fetch.lua
	fetchScriptSrc string

	//go:embed scripts/ack.lua
	ackScriptSrc string

	//go:embed scripts/nack_requeue.lua
	nackRequeueScriptSrc string

	//go:embed scripts/nack_discard.lua
	nackDiscardScriptSrc string

	//go:embed scripts/nack_retry.lua
	nackRetryScriptSrc string

	//go:embed scripts/cancel.lua
	cancelScriptSrc string

	//go:embed scripts/advance_workflow.lua
	advanceWorkflowScriptSrc string

	//go:embed scripts/requeue_stalled.lua
	requeueStalledScriptSrc string

	//go:embed scripts/retry_dead_letter.lua
	retryDeadLetterScriptSrc string

	//go:embed scripts/unique_check.lua
	uniqueCheckScriptSrc string

	//go:embed scripts/promote.lua
	promoteScriptSrc string
)

// Pre-compiled Lua scripts (EVALSHA caching handled by go-redis).
var (
	fetchScript           *redisclient.Script
	ackScript             *redisclient.Script
	nackRequeueScript     *redisclient.Script
	nackDiscardScript     *redisclient.Script
	nackRetryScript       *redisclient.Script
	cancelScript          *redisclient.Script
	advanceWorkflowScript *redisclient.Script
	requeueStalledScript  *redisclient.Script
	retryDeadLetterScript *redisclient.Script
	uniqueCheckScript     *redisclient.Script
	promoteScript         *redisclient.Script
)

func init() {
	fetchScript = redisclient.NewScript(fetchScriptSrc)
	ackScript = redisclient.NewScript(ackScriptSrc)
	nackRequeueScript = redisclient.NewScript(nackRequeueScriptSrc)
	nackDiscardScript = redisclient.NewScript(nackDiscardScriptSrc)
	nackRetryScript = redisclient.NewScript(nackRetryScriptSrc)
	cancelScript = redisclient.NewScript(cancelScriptSrc)
	advanceWorkflowScript = redisclient.NewScript(advanceWorkflowScriptSrc)
	requeueStalledScript = redisclient.NewScript(requeueStalledScriptSrc)
	retryDeadLetterScript = redisclient.NewScript(retryDeadLetterScriptSrc)
	uniqueCheckScript = redisclient.NewScript(uniqueCheckScriptSrc)
	promoteScript = redisclient.NewScript(promoteScriptSrc)
}

// Lua script return status codes.
const (
	luaStatusOK       = 0
	luaStatusNotFound = 1
	luaStatusConflict = 2
	luaStatusExpired  = 3
)

// parseLuaResult extracts the status code and data from a Lua script result.
// All scripts return {status_code, ...data}.
func parseLuaResult(result interface{}) (int64, []interface{}) {
	arr, ok := result.([]interface{})
	if !ok || len(arr) == 0 {
		return -1, nil
	}
	status, ok := arr[0].(int64)
	if !ok {
		return -1, nil
	}
	return status, arr[1:]
}
