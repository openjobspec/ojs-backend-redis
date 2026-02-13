package redis

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

const workerStaleThreshold = 60 * time.Second

// ListJobs lists jobs for admin views with optional filtering and pagination.
func (b *RedisBackend) ListJobs(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error) {
	keys, err := b.scanKeys(ctx, jobKey("*"))
	if err != nil {
		return nil, 0, err
	}

	jobs := make([]*core.Job, 0, len(keys))
	for _, key := range keys {
		data, err := b.client.HGetAll(ctx, key).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		if !jobMatchesFilters(data, filters) {
			continue
		}
		job := hashToJob(data)
		if job == nil {
			continue
		}
		jobs = append(jobs, job)
	}

	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt == jobs[j].CreatedAt {
			return jobs[i].ID > jobs[j].ID
		}
		return jobs[i].CreatedAt > jobs[j].CreatedAt
	})

	total := len(jobs)
	start, end := paginate(total, limit, offset)
	return jobs[start:end], total, nil
}

// ListWorkers lists workers for admin views with pagination and summary counts.
func (b *RedisBackend) ListWorkers(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error) {
	ids, err := b.client.SMembers(ctx, workersKey()).Result()
	if err != nil {
		return nil, core.WorkerSummary{}, err
	}
	sort.Strings(ids)

	all := make([]*core.WorkerInfo, 0, len(ids))
	summary := core.WorkerSummary{}
	now := time.Now().UTC()

	for _, id := range ids {
		data, err := b.client.HGetAll(ctx, workerKey(id)).Result()
		if err != nil || len(data) == 0 {
			continue
		}

		directive := data["directive"]
		if directive == "" {
			directive = "continue"
		}

		activeJobs, _ := strconv.Atoi(data["active_jobs"])
		lastHeartbeat := data["last_heartbeat"]

		state := "running"
		if directive == "quiet" {
			state = "quiet"
		}

		if lastHeartbeat == "" {
			state = "stale"
		} else {
			heartbeatAt, parseErr := time.Parse(core.TimeFormat, lastHeartbeat)
			if parseErr != nil || now.Sub(heartbeatAt) > workerStaleThreshold {
				state = "stale"
			}
		}

		worker := &core.WorkerInfo{
			ID:            id,
			State:         state,
			Directive:     directive,
			ActiveJobs:    activeJobs,
			LastHeartbeat: lastHeartbeat,
		}
		all = append(all, worker)
	}

	summary.Total = len(all)
	for _, w := range all {
		switch w.State {
		case "quiet":
			summary.Quiet++
		case "stale":
			summary.Stale++
		default:
			summary.Running++
		}
	}

	start, end := paginate(len(all), limit, offset)
	return all[start:end], summary, nil
}

func (b *RedisBackend) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	var (
		cursor uint64
		keys   []string
		seen   = make(map[string]struct{})
	)
	for {
		batch, next, err := b.client.Scan(ctx, cursor, pattern, 200).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range batch {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

func jobMatchesFilters(data map[string]string, filters core.JobListFilters) bool {
	if filters.State != "" && data["state"] != filters.State {
		return false
	}
	if filters.Queue != "" && data["queue"] != filters.Queue {
		return false
	}
	if filters.Type != "" && data["type"] != filters.Type {
		return false
	}
	if filters.WorkerID != "" && data["worker_id"] != filters.WorkerID {
		return false
	}
	return true
}

func paginate(total, limit, offset int) (int, int) {
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return total, total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return offset, end
}
