package main

import (
	"testing"

	"github.com/orian/clicktelligence/models"
	"github.com/stretchr/testify/assert"
)

func TestFilterExplainConfigs(t *testing.T) {
	tests := []struct {
		name           string
		configs        []models.ExplainConfig
		serverSettings map[string]string
		forceAnalyzer  bool
		wantTypes      []models.ExplainType
	}{
		{
			name: "no filtering when forceAnalyzer is true",
			configs: []models.ExplainConfig{
				{Type: models.ExplainPlan, Enabled: true},
				{Type: models.ExplainQueryTree, Enabled: true},
			},
			serverSettings: map[string]string{"enable_analyzer": "0"},
			forceAnalyzer:  true,
			wantTypes:      []models.ExplainType{models.ExplainPlan, models.ExplainQueryTree},
		},
		{
			name: "no filtering when analyzer is enabled",
			configs: []models.ExplainConfig{
				{Type: models.ExplainPlan, Enabled: true},
				{Type: models.ExplainQueryTree, Enabled: true},
			},
			serverSettings: map[string]string{"enable_analyzer": "1"},
			forceAnalyzer:  false,
			wantTypes:      []models.ExplainType{models.ExplainPlan, models.ExplainQueryTree},
		},
		{
			name: "no filtering when enable_analyzer not in settings",
			configs: []models.ExplainConfig{
				{Type: models.ExplainPlan, Enabled: true},
				{Type: models.ExplainQueryTree, Enabled: true},
			},
			serverSettings: map[string]string{},
			forceAnalyzer:  false,
			wantTypes:      []models.ExplainType{models.ExplainPlan, models.ExplainQueryTree},
		},
		{
			name: "filters QUERY TREE when analyzer disabled",
			configs: []models.ExplainConfig{
				{Type: models.ExplainPlan, Enabled: true},
				{Type: models.ExplainQueryTree, Enabled: true},
				{Type: models.ExplainPipeline, Enabled: true},
			},
			serverSettings: map[string]string{"enable_analyzer": "0"},
			forceAnalyzer:  false,
			wantTypes:      []models.ExplainType{models.ExplainPlan, models.ExplainPipeline},
		},
		{
			name:           "empty configs returns empty",
			configs:        []models.ExplainConfig{},
			serverSettings: map[string]string{"enable_analyzer": "0"},
			forceAnalyzer:  false,
			wantTypes:      nil,
		},
		{
			name: "nil server settings",
			configs: []models.ExplainConfig{
				{Type: models.ExplainQueryTree, Enabled: true},
			},
			serverSettings: nil,
			forceAnalyzer:  false,
			wantTypes:      []models.ExplainType{models.ExplainQueryTree},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterExplainConfigs(tt.configs, tt.serverSettings, tt.forceAnalyzer)

			if tt.wantTypes == nil {
				assert.Nil(t, got)
				return
			}

			assert.Len(t, got, len(tt.wantTypes))
			for i, config := range got {
				assert.Equal(t, tt.wantTypes[i], config.Type)
			}
		})
	}
}

func TestGetExplainConfigs(t *testing.T) {
	tests := []struct {
		name    string
		configs []models.ExplainConfig
		wantLen int
	}{
		{
			name:    "returns provided configs",
			configs: []models.ExplainConfig{{Type: models.ExplainPlan}},
			wantLen: 1,
		},
		{
			name:    "returns defaults when empty",
			configs: []models.ExplainConfig{},
			wantLen: 6, // default configs count
		},
		{
			name:    "returns defaults when nil",
			configs: nil,
			wantLen: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getExplainConfigs(tt.configs)
			assert.Len(t, got, tt.wantLen)
		})
	}
}

func TestBuildExplainResponse(t *testing.T) {
	version := &models.QueryVersion{
		ID:        "test-version-id",
		BranchID:  "test-branch-id",
		Query:     "SELECT 1",
		QueryHash: "abc123",
	}

	branch := &models.Branch{
		ID:   "new-branch-id",
		Name: "new-branch",
	}

	tests := []struct {
		name          string
		version       *models.QueryVersion
		autoBranched  bool
		newBranch     *models.Branch
		resultsReused bool
		wantKeys      []string
		checkBranch   bool
	}{
		{
			name:          "basic response without auto-branch",
			version:       version,
			autoBranched:  false,
			newBranch:     nil,
			resultsReused: false,
			wantKeys:      []string{"version", "autoBranched", "resultsReused"},
			checkBranch:   false,
		},
		{
			name:          "response with auto-branched",
			version:       version,
			autoBranched:  true,
			newBranch:     branch,
			resultsReused: false,
			wantKeys:      []string{"version", "autoBranched", "resultsReused", "newBranch"},
			checkBranch:   true,
		},
		{
			name:          "response with results reused",
			version:       version,
			autoBranched:  false,
			newBranch:     nil,
			resultsReused: true,
			wantKeys:      []string{"version", "autoBranched", "resultsReused"},
			checkBranch:   false,
		},
		{
			name:          "autoBranched true but nil branch",
			version:       version,
			autoBranched:  true,
			newBranch:     nil,
			resultsReused: false,
			wantKeys:      []string{"version", "autoBranched", "resultsReused"},
			checkBranch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildExplainResponse(tt.version, tt.autoBranched, tt.newBranch, tt.resultsReused)

			// Check all expected keys exist
			for _, key := range tt.wantKeys {
				assert.Contains(t, got, key)
			}

			// Check values
			assert.Equal(t, tt.version, got["version"])
			assert.Equal(t, tt.autoBranched, got["autoBranched"])
			assert.Equal(t, tt.resultsReused, got["resultsReused"])

			if tt.checkBranch {
				assert.Equal(t, tt.newBranch, got["newBranch"])
			} else {
				_, hasBranch := got["newBranch"]
				assert.False(t, hasBranch)
			}
		})
	}
}

func TestCreateVersion(t *testing.T) {
	req := &ExplainRequest{
		BranchID:        "original-branch",
		Query:           "SELECT 1",
		ParentVersionID: "parent-id",
	}

	results := []models.ExplainResult{
		{Type: models.ExplainPlan, Output: "plan output"},
	}

	version := createVersion("target-branch", req, "hash123", results)

	assert.NotEmpty(t, version.ID)
	assert.Equal(t, "target-branch", version.BranchID)
	assert.Equal(t, "SELECT 1", version.Query)
	assert.Equal(t, "hash123", version.QueryHash)
	assert.Equal(t, results, version.ExplainResults)
	assert.Equal(t, "parent-id", version.ParentVersionID)
	assert.NotNil(t, version.ExecutionStats)
	assert.False(t, version.Timestamp.IsZero())
}
