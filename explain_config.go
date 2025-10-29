package main

import (
	"fmt"
	"strings"
)

// ExplainType represents the type of EXPLAIN to run
type ExplainType string

const (
	ExplainAST           ExplainType = "AST"
	ExplainSyntax        ExplainType = "SYNTAX"
	ExplainQueryTree     ExplainType = "QUERY TREE"
	ExplainPlan          ExplainType = "PLAN"
	ExplainPipeline      ExplainType = "PIPELINE"
	ExplainEstimate      ExplainType = "ESTIMATE"
	ExplainTableOverride ExplainType = "TABLE OVERRIDE"
)

// ExplainSettings represents settings for an EXPLAIN query
type ExplainSettings struct {
	// Common settings
	Header      *int `json:"header,omitempty"`      // Include headers (PLAN, PIPELINE)
	Description *int `json:"description,omitempty"` // Include descriptions (PLAN)

	// PLAN specific
	Indexes     *int `json:"indexes,omitempty"`     // Show index usage
	Projections *int `json:"projections,omitempty"` // Show projections
	Actions     *int `json:"actions,omitempty"`     // Show detailed actions
	JSONFormat  *int `json:"json,omitempty"`        // Output as JSON

	// PIPELINE specific
	Graph   *int `json:"graph,omitempty"`   // Output DOT graph
	Compact *int `json:"compact,omitempty"` // Compact mode

	// SYNTAX specific
	OneLine            *int `json:"oneline,omitempty"`               // Single line output
	RunQueryTreePasses *int `json:"run_query_tree_passes,omitempty"` // Run optimization passes
	QueryTreePasses    *int `json:"query_tree_passes,omitempty"`     // Number of passes

	// QUERY TREE specific
	RunPasses  *int `json:"run_passes,omitempty"`  // Execute all passes
	DumpPasses *int `json:"dump_passes,omitempty"` // Show pass info
	Passes     *int `json:"passes,omitempty"`      // Number of passes (-1 = all)
	DumpTree   *int `json:"dump_tree,omitempty"`   // Display tree
	DumpAST    *int `json:"dump_ast,omitempty"`    // Show generated AST
}

// ExplainConfig represents a single EXPLAIN configuration
type ExplainConfig struct {
	Type     ExplainType     `json:"type"`
	Settings ExplainSettings `json:"settings"`
	Enabled  bool            `json:"enabled"`
}

// ExplainResult stores the result of an EXPLAIN execution
type ExplainResult struct {
	Type   ExplainType `json:"type"`
	Output string      `json:"output"`
	Error  string      `json:"error,omitempty"`
}

// BuildExplainQuery constructs the EXPLAIN query string
func (c *ExplainConfig) BuildExplainQuery(query string, logComment string, forceAnalyzer bool) string {
	var parts []string

	// Add EXPLAIN keyword and type
	if c.Type == "" || c.Type == ExplainPlan {
		parts = append(parts, "EXPLAIN")
	} else {
		parts = append(parts, fmt.Sprintf("EXPLAIN %s", c.Type))
	}

	// Add settings
	settings := c.buildSettings()
	if len(settings) > 0 {
		parts = append(parts, settings)
	}

	// Add the actual query
	parts = append(parts, query)

	// Build SETTINGS clause
	var settingsClause []string
	if logComment != "" {
		settingsClause = append(settingsClause, fmt.Sprintf("log_comment='%s'", logComment))
	}
	if forceAnalyzer && c.Type == ExplainQueryTree {
		settingsClause = append(settingsClause, "enable_analyzer=1")
	}

	if len(settingsClause) > 0 {
		parts = append(parts, "SETTINGS", strings.Join(settingsClause, ", "))
	}

	return strings.Join(parts, " ")
}

// buildSettings constructs the settings string for EXPLAIN
func (c *ExplainConfig) buildSettings() string {
	var settings []string

	s := c.Settings

	// Add applicable settings based on type
	if s.Header != nil {
		settings = append(settings, fmt.Sprintf("header=%d", *s.Header))
	}
	if s.Description != nil && c.Type == ExplainPlan {
		settings = append(settings, fmt.Sprintf("description=%d", *s.Description))
	}
	if s.Indexes != nil && c.Type == ExplainPlan {
		settings = append(settings, fmt.Sprintf("indexes=%d", *s.Indexes))
	}
	if s.Projections != nil && c.Type == ExplainPlan {
		settings = append(settings, fmt.Sprintf("projections=%d", *s.Projections))
	}
	if s.Actions != nil && c.Type == ExplainPlan {
		settings = append(settings, fmt.Sprintf("actions=%d", *s.Actions))
	}
	if s.JSONFormat != nil && c.Type == ExplainPlan {
		settings = append(settings, fmt.Sprintf("json=%d", *s.JSONFormat))
	}
	if s.Graph != nil && c.Type == ExplainPipeline {
		settings = append(settings, fmt.Sprintf("graph=%d", *s.Graph))
	}
	if s.Compact != nil && c.Type == ExplainPipeline {
		settings = append(settings, fmt.Sprintf("compact=%d", *s.Compact))
	}
	if s.OneLine != nil && c.Type == ExplainSyntax {
		settings = append(settings, fmt.Sprintf("oneline=%d", *s.OneLine))
	}
	if s.RunQueryTreePasses != nil && c.Type == ExplainSyntax {
		settings = append(settings, fmt.Sprintf("run_query_tree_passes=%d", *s.RunQueryTreePasses))
	}
	if s.QueryTreePasses != nil && c.Type == ExplainSyntax {
		settings = append(settings, fmt.Sprintf("query_tree_passes=%d", *s.QueryTreePasses))
	}
	if s.RunPasses != nil && c.Type == ExplainQueryTree {
		settings = append(settings, fmt.Sprintf("run_passes=%d", *s.RunPasses))
	}
	if s.DumpPasses != nil && c.Type == ExplainQueryTree {
		settings = append(settings, fmt.Sprintf("dump_passes=%d", *s.DumpPasses))
	}
	if s.Passes != nil && c.Type == ExplainQueryTree {
		settings = append(settings, fmt.Sprintf("passes=%d", *s.Passes))
	}
	if s.DumpTree != nil && c.Type == ExplainQueryTree {
		settings = append(settings, fmt.Sprintf("dump_tree=%d", *s.DumpTree))
	}
	if s.DumpAST != nil && c.Type == ExplainQueryTree {
		settings = append(settings, fmt.Sprintf("dump_ast=%d", *s.DumpAST))
	}

	if len(settings) == 0 {
		return ""
	}

	return strings.Join(settings, ", ")
}

// GetDefaultExplainConfigs returns the default set of EXPLAIN configurations
func GetDefaultExplainConfigs() []ExplainConfig {
	one := 1
	zero := 0

	return []ExplainConfig{
		{
			Type: ExplainPlan,
			Settings: ExplainSettings{
				Indexes:     &one,
				Description: &one,
			},
			Enabled: true,
		},
		{
			Type: ExplainPipeline,
			Settings: ExplainSettings{
				Compact: &one,
			},
			Enabled: true,
		},
		{
			Type:     ExplainEstimate,
			Settings: ExplainSettings{},
			Enabled:  true,
		},
		{
			Type:     ExplainAST,
			Settings: ExplainSettings{},
			Enabled:  true,
		},
		{
			Type: ExplainSyntax,
			Settings: ExplainSettings{
				OneLine: &zero,
			},
			Enabled: true,
		},
		{
			Type: ExplainQueryTree,
			Settings: ExplainSettings{
				RunPasses: &one,
				DumpTree:  &one,
			},
			Enabled: true,
		},
	}
}
