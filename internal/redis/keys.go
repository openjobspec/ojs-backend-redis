package redis

import "fmt"

const (
	keyPrefix = "ojs:"
)

// Redis key builders
func jobKey(id string) string                  { return fmt.Sprintf("%sjob:%s", keyPrefix, id) }
func queueAvailableKey(name string) string     { return fmt.Sprintf("%squeue:%s:available", keyPrefix, name) }
func queueActiveKey(name string) string        { return fmt.Sprintf("%squeue:%s:active", keyPrefix, name) }
func queuePausedKey(name string) string        { return fmt.Sprintf("%squeue:%s:paused", keyPrefix, name) }
func queuesKey() string                        { return keyPrefix + "queues" }
func scheduledKey() string                     { return keyPrefix + "scheduled" }
func retryKey() string                         { return keyPrefix + "retry" }
func deadKey() string                          { return keyPrefix + "dead" }
func uniqueKey(fingerprint string) string      { return fmt.Sprintf("%sunique:%s", keyPrefix, fingerprint) }
func cronKey(name string) string               { return fmt.Sprintf("%scron:%s", keyPrefix, name) }
func cronNamesKey() string                     { return keyPrefix + "cron:names" }
func workflowKey(id string) string             { return fmt.Sprintf("%sworkflow:%s", keyPrefix, id) }
func workflowStepsKey(id string) string        { return fmt.Sprintf("%sworkflow:%s:steps", keyPrefix, id) }
func workerKey(id string) string               { return fmt.Sprintf("%sworker:%s", keyPrefix, id) }
func workersKey() string                       { return keyPrefix + "workers" }
func visibilityKey(jobID string) string        { return fmt.Sprintf("%svisibility:%s", keyPrefix, jobID) }
func jobWorkflowKey(jobID string) string       { return fmt.Sprintf("%sjob:%s:workflow", keyPrefix, jobID) }
func cronInstanceKey(name string) string       { return fmt.Sprintf("%scron:%s:instance", keyPrefix, name) }
func queueCompletedKey(name string) string     { return fmt.Sprintf("%squeue:%s:completed", keyPrefix, name) }
func queueRateLimitKey(name string) string     { return fmt.Sprintf("%squeue:%s:ratelimit", keyPrefix, name) }
func queueRateLimitLastKey(name string) string { return fmt.Sprintf("%squeue:%s:ratelimit:last", keyPrefix, name) }
