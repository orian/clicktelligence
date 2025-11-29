package main

import (
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/orian/clicktelligence/models"
)

// ExplainRequest represents the incoming request for explaining a query.
type ExplainRequest struct {
	BranchID           string                 `json:"branchId"`
	Query              string                 `json:"query"`
	ParentVersionID    string                 `json:"parentVersionId"`
	ExplainConfigs     []models.ExplainConfig `json:"explainConfigs,omitempty"`
	ForceAnalyzer      bool                   `json:"forceAnalyzer,omitempty"`
	ServerSettings     map[string]string      `json:"serverSettings,omitempty"`
	MaxExecutionTimeMs int                    `json:"maxExecutionTimeMs,omitempty"`
}

// filterExplainConfigs filters out EXPLAIN QUERY TREE when the analyzer is disabled
// and forceAnalyzer is false. Returns the filtered list of configs.
func filterExplainConfigs(configs []models.ExplainConfig, serverSettings map[string]string, forceAnalyzer bool) []models.ExplainConfig {
	if forceAnalyzer {
		return configs
	}

	analyzerValue, ok := serverSettings["enable_analyzer"]
	if !ok || analyzerValue != "0" {
		return configs
	}

	// Filter out QUERY TREE
	var filtered []models.ExplainConfig
	for _, config := range configs {
		if config.Type != models.ExplainQueryTree {
			filtered = append(filtered, config)
		} else {
			log.Println("Skipping EXPLAIN QUERY TREE because enable_analyzer=0")
		}
	}
	return filtered
}

// getExplainConfigs returns the provided configs or default configs if none provided.
func getExplainConfigs(configs []models.ExplainConfig) []models.ExplainConfig {
	if len(configs) == 0 {
		log.Println("No EXPLAIN configurations provided, using default set")
		return models.GetDefaultExplainConfigs()
	}
	return configs
}

// checkCachedVersion checks if the parent version can be reused.
// Returns the parent version and true if:
// - parentVersionID is not empty
// - parent version exists
// - query hash matches
// - parent has explain results
// - parent has no errors
func checkCachedVersion(storage models.Storage, parentVersionID, queryHash string) (*models.QueryVersion, bool) {
	if parentVersionID == "" {
		return nil, false
	}

	parentVersion, exists := storage.GetVersion(parentVersionID)
	if !exists {
		return nil, false
	}

	if parentVersion.QueryHash != queryHash {
		return nil, false
	}

	if len(parentVersion.ExplainResults) == 0 {
		return nil, false
	}

	// Check if parent has any errors
	for _, result := range parentVersion.ExplainResults {
		if result.Error != "" {
			log.Printf("Query unchanged but parent had errors, re-executing EXPLAIN")
			return nil, false
		}
	}

	log.Printf("Query unchanged, returning existing version %s (no new version created)", parentVersionID)
	return parentVersion, true
}

// AutoBranchResult contains the result of auto-branch check.
type AutoBranchResult struct {
	TargetBranchID string
	NewBranch      *models.Branch
	AutoBranched   bool
}

// checkAutoBranch checks if editing a non-head version and creates a new branch if needed.
// Returns the target branch ID and optionally the new branch.
func checkAutoBranch(storage models.Storage, branchID, parentVersionID string) (*AutoBranchResult, error) {
	result := &AutoBranchResult{
		TargetBranchID: branchID,
		AutoBranched:   false,
	}

	if parentVersionID == "" {
		return result, nil
	}

	branch, exists := storage.GetBranch(branchID)
	if !exists {
		return result, nil
	}

	// Check if editing non-head version
	if branch.CurrentVersionID == "" || branch.CurrentVersionID == parentVersionID {
		return result, nil
	}

	// User is editing a non-head version, auto-create new branch
	newBranchName := fmt.Sprintf("branch-%s", time.Now().Format("2006-01-02-15:04:05"))
	newBranch, err := storage.CreateBranch(newBranchName, branchID, parentVersionID)
	if err != nil {
		log.Printf("Failed to auto-create branch: %v", err)
		return result, nil // Don't fail, just use original branch
	}

	log.Printf("Auto-created branch '%s' (ID: %s) from version %s", newBranchName, newBranch.ID, parentVersionID)
	return &AutoBranchResult{
		TargetBranchID: newBranch.ID,
		NewBranch:      newBranch,
		AutoBranched:   true,
	}, nil
}

// buildExplainResponse builds the JSON response for an explain query.
func buildExplainResponse(version *models.QueryVersion, autoBranched bool, newBranch *models.Branch, resultsReused bool) map[string]interface{} {
	response := map[string]interface{}{
		"version":       version,
		"autoBranched":  autoBranched,
		"resultsReused": resultsReused,
	}

	if autoBranched && newBranch != nil {
		response["newBranch"] = newBranch
	}

	return response
}

// createVersion creates a new QueryVersion from the request and explain results.
func createVersion(branchID string, req *ExplainRequest, queryHash string, results []models.ExplainResult) *models.QueryVersion {
	return &models.QueryVersion{
		ID:              uuid.New().String(),
		BranchID:        branchID,
		Query:           req.Query,
		QueryHash:       queryHash,
		ExplainResults:  results,
		ExecutionStats:  make(map[string]interface{}),
		Timestamp:       time.Now(),
		ParentVersionID: req.ParentVersionID,
	}
}
