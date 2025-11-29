// Package models defines the core data types for clicktelligence,
// a version-controlled query optimization tool for ClickHouse workloads.
package models

import "time"

// QueryVersion represents a single version of a query with its analysis results.
// Each version is immutable and linked to its parent version, forming a
// version history similar to git commits.
type QueryVersion struct {
	// ID is the unique identifier for this version (UUID).
	ID string `json:"id"`

	// BranchID references the branch this version belongs to.
	BranchID string `json:"branchId"`

	// Query is the SQL query text.
	Query string `json:"query"`

	// QueryHash is the SHA-256 hash of the query text, used for
	// detecting unchanged queries and deduplication.
	QueryHash string `json:"queryHash"`

	// ExplainResults contains the output from various EXPLAIN query types
	// (PLAN, PIPELINE, ESTIMATE, AST, SYNTAX, QUERY TREE).
	ExplainResults []ExplainResult `json:"explainResults"`

	// ExecutionStats contains flexible execution statistics as key-value pairs.
	ExecutionStats map[string]interface{} `json:"executionStats"`

	// Timestamp is when this version was created.
	Timestamp time.Time `json:"timestamp"`

	// ParentVersionID references the previous version this was derived from.
	// Empty for the first version in a branch.
	ParentVersionID string `json:"parentVersionId,omitempty"`

	// Tags contains all tags associated with this version.
	Tags []*VersionTag `json:"tags,omitempty"`
}

// Branch represents a line of query development, similar to a git branch.
// Branches allow exploring different optimization paths independently.
type Branch struct {
	// ID is the unique identifier for this branch (UUID).
	ID string `json:"id"`

	// Name is the human-readable branch name (e.g., "main", "optimize-joins").
	Name string `json:"name"`

	// ParentBranchID references the branch this was forked from.
	// Empty for root branches.
	ParentBranchID string `json:"parentBranchId,omitempty"`

	// BranchFromVersionID tracks which specific version this branch
	// was created from, enabling precise branch point visualization.
	BranchFromVersionID string `json:"branchFromVersionId,omitempty"`

	// CurrentVersionID points to the latest (head) version on this branch.
	CurrentVersionID string `json:"currentVersionId,omitempty"`

	// CreatedAt is when this branch was created.
	CreatedAt time.Time `json:"createdAt"`
}
