package core

import (
	"encoding/json"
	"testing"
)

func TestParseCapabilities(t *testing.T) {
	caps := ParseCapabilities([]string{
		"gpu:nvidia-a100",
		"gpu_count:4",
		"memory_gb:256",
		"region:us-east-1",
		"spot",
	})

	if caps.Labels["gpu"] != "nvidia-a100" {
		t.Errorf("expected gpu=nvidia-a100, got %s", caps.Labels["gpu"])
	}
	if caps.Labels["gpu_count"] != "4" {
		t.Errorf("expected gpu_count=4, got %s", caps.Labels["gpu_count"])
	}
	if caps.Labels["spot"] != "" {
		t.Errorf("expected spot='', got %s", caps.Labels["spot"])
	}
}

func TestExtractResourceRequirements(t *testing.T) {
	meta := json.RawMessage(`{"resources":{"gpu":{"count":2,"type":"nvidia-a100","memory_gb":40},"memory_gb":64}}`)

	resources := ExtractResourceRequirements(meta)
	if resources == nil {
		t.Fatal("expected non-nil resources")
	}
	if resources.GPU.Count != 2 {
		t.Errorf("expected gpu count 2, got %d", resources.GPU.Count)
	}
	if resources.GPU.Type != "nvidia-a100" {
		t.Errorf("expected gpu type nvidia-a100, got %s", resources.GPU.Type)
	}
	if resources.MemoryGB != 64 {
		t.Errorf("expected memory 64, got %f", resources.MemoryGB)
	}
}

func TestExtractResourceRequirements_Empty(t *testing.T) {
	resources := ExtractResourceRequirements(nil)
	if resources != nil {
		t.Error("expected nil for empty meta")
	}

	resources = ExtractResourceRequirements(json.RawMessage(`{}`))
	if resources != nil {
		t.Error("expected nil for meta without resources")
	}
}

func TestMatchesCapabilities_NoRequirements(t *testing.T) {
	job := &Job{
		ID:   "job-1",
		Type: "email.send",
		Meta: json.RawMessage(`{}`),
	}
	caps := ParseCapabilities([]string{"gpu:nvidia-a100"})

	if !MatchesCapabilities(job, caps) {
		t.Error("job without resource requirements should match any worker")
	}
}

func TestMatchesCapabilities_NoCaps(t *testing.T) {
	job := &Job{
		ID:   "job-1",
		Type: "ml.train",
		Meta: json.RawMessage(`{"resources":{"gpu":{"count":1,"type":"nvidia-a100"}}}`),
	}
	caps := ParseCapabilities(nil)

	if !MatchesCapabilities(job, caps) {
		t.Error("worker without caps should match all jobs (backward compat)")
	}
}

func TestMatchesCapabilities_GPUMatch(t *testing.T) {
	job := &Job{
		ID:   "job-1",
		Type: "ml.train",
		Meta: json.RawMessage(`{"resources":{"gpu":{"count":1,"type":"nvidia-a100"}}}`),
	}

	// Worker with matching GPU
	caps := ParseCapabilities([]string{"gpu:nvidia-a100", "gpu_count:4"})
	if !MatchesCapabilities(job, caps) {
		t.Error("worker with matching GPU should match")
	}

	// Worker with wrong GPU
	caps = ParseCapabilities([]string{"gpu:nvidia-t4"})
	if MatchesCapabilities(job, caps) {
		t.Error("worker with wrong GPU type should not match")
	}

	// Worker with no GPU
	caps = ParseCapabilities([]string{"memory_gb:256"})
	if MatchesCapabilities(job, caps) {
		t.Error("worker without GPU should not match GPU job")
	}
}

func TestMatchesCapabilities_Affinity(t *testing.T) {
	job := &Job{
		ID:   "job-1",
		Type: "ml.inference",
		Meta: json.RawMessage(`{
			"affinity": {
				"required": [
					{"key": "region", "operator": "In", "values": ["us-east-1", "us-west-2"]}
				]
			}
		}`),
	}

	// Worker in matching region
	caps := ParseCapabilities([]string{"region:us-east-1"})
	if !MatchesCapabilities(job, caps) {
		t.Error("worker in us-east-1 should match")
	}

	// Worker in non-matching region
	caps = ParseCapabilities([]string{"region:eu-west-1"})
	if MatchesCapabilities(job, caps) {
		t.Error("worker in eu-west-1 should not match")
	}
}

func TestMatchesCapabilities_AffinityNotIn(t *testing.T) {
	job := &Job{
		ID:   "job-1",
		Type: "ml.train",
		Meta: json.RawMessage(`{
			"affinity": {
				"required": [
					{"key": "spot", "operator": "NotIn", "values": ["true"]}
				]
			}
		}`),
	}

	// Worker that is NOT a spot instance
	caps := ParseCapabilities([]string{"spot:false"})
	if !MatchesCapabilities(job, caps) {
		t.Error("non-spot worker should match")
	}

	// Worker that IS a spot instance
	caps = ParseCapabilities([]string{"spot:true"})
	if MatchesCapabilities(job, caps) {
		t.Error("spot worker should not match")
	}
}

func TestMatchesCapabilities_AffinityExists(t *testing.T) {
	job := &Job{
		ID:   "job-1",
		Type: "ml.train",
		Meta: json.RawMessage(`{
			"affinity": {
				"required": [
					{"key": "gpu", "operator": "Exists"}
				]
			}
		}`),
	}

	caps := ParseCapabilities([]string{"gpu:nvidia-a100"})
	if !MatchesCapabilities(job, caps) {
		t.Error("worker with GPU label should match Exists rule")
	}

	caps = ParseCapabilities([]string{"memory_gb:256"})
	if MatchesCapabilities(job, caps) {
		t.Error("worker without GPU label should not match Exists rule")
	}
}

func TestEvaluateRule_DoesNotExist(t *testing.T) {
	rule := AffinityRule{Key: "tainted", Operator: "DoesNotExist"}

	caps := ParseCapabilities([]string{"gpu:nvidia-a100"})
	if !evaluateRule(rule, caps) {
		t.Error("worker without tainted label should match DoesNotExist")
	}

	caps = ParseCapabilities([]string{"tainted:true"})
	if evaluateRule(rule, caps) {
		t.Error("worker with tainted label should not match DoesNotExist")
	}
}

func TestMatchesCapabilities_Combined(t *testing.T) {
	// Job needs GPU + specific region
	job := &Job{
		ID:   "job-1",
		Type: "ml.train",
		Meta: json.RawMessage(`{
			"resources": {"gpu": {"count": 1, "type": "nvidia-a100"}},
			"affinity": {
				"required": [
					{"key": "region", "operator": "In", "values": ["us-east-1"]}
				]
			}
		}`),
	}

	// Worker with GPU + right region
	caps := ParseCapabilities([]string{"gpu:nvidia-a100", "region:us-east-1"})
	if !MatchesCapabilities(job, caps) {
		t.Error("worker with matching GPU and region should match")
	}

	// Worker with GPU + wrong region
	caps = ParseCapabilities([]string{"gpu:nvidia-a100", "region:eu-west-1"})
	if MatchesCapabilities(job, caps) {
		t.Error("worker with matching GPU but wrong region should not match")
	}

	// Worker with right region + wrong GPU
	caps = ParseCapabilities([]string{"gpu:nvidia-t4", "region:us-east-1"})
	if MatchesCapabilities(job, caps) {
		t.Error("worker with right region but wrong GPU should not match")
	}
}
