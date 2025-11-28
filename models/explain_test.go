package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// intPtr is a helper to create int pointers for settings
func intPtr(i int) *int { return &i }

func TestBuildExplainQuery(t *testing.T) {
	tests := []struct {
		name               string
		config             ExplainConfig
		query              string
		logComment         string
		forceAnalyzer      bool
		maxExecutionTimeMs int
		want               string
	}{
		// Basic EXPLAIN types
		{
			name:   "empty type defaults to EXPLAIN",
			config: ExplainConfig{Type: ""},
			query:  "SELECT 1",
			want:   "EXPLAIN SELECT 1",
		},
		{
			name:   "PLAN type",
			config: ExplainConfig{Type: ExplainPlan},
			query:  "SELECT 1",
			want:   "EXPLAIN PLAN SELECT 1",
		},
		{
			name:   "PIPELINE type",
			config: ExplainConfig{Type: ExplainPipeline},
			query:  "SELECT 1",
			want:   "EXPLAIN PIPELINE SELECT 1",
		},
		{
			name:   "AST type",
			config: ExplainConfig{Type: ExplainAST},
			query:  "SELECT 1",
			want:   "EXPLAIN AST SELECT 1",
		},
		{
			name:   "SYNTAX type",
			config: ExplainConfig{Type: ExplainSyntax},
			query:  "SELECT 1",
			want:   "EXPLAIN SYNTAX SELECT 1",
		},
		{
			name:   "QUERY TREE type",
			config: ExplainConfig{Type: ExplainQueryTree},
			query:  "SELECT 1",
			want:   "EXPLAIN QUERY TREE SELECT 1",
		},
		{
			name:   "ESTIMATE type",
			config: ExplainConfig{Type: ExplainEstimate},
			query:  "SELECT 1",
			want:   "EXPLAIN ESTIMATE SELECT 1",
		},
		{
			name:   "TABLE OVERRIDE type",
			config: ExplainConfig{Type: ExplainTableOverride},
			query:  "SELECT 1",
			want:   "EXPLAIN TABLE OVERRIDE SELECT 1",
		},

		// PLAN-specific settings
		{
			name: "PLAN with indexes",
			config: ExplainConfig{
				Type:     ExplainPlan,
				Settings: ExplainSettings{Indexes: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PLAN indexes=1 SELECT 1",
		},
		{
			name: "PLAN with description",
			config: ExplainConfig{
				Type:     ExplainPlan,
				Settings: ExplainSettings{Description: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PLAN description=1 SELECT 1",
		},
		{
			name: "PLAN with projections",
			config: ExplainConfig{
				Type:     ExplainPlan,
				Settings: ExplainSettings{Projections: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PLAN projections=1 SELECT 1",
		},
		{
			name: "PLAN with actions",
			config: ExplainConfig{
				Type:     ExplainPlan,
				Settings: ExplainSettings{Actions: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PLAN actions=1 SELECT 1",
		},
		{
			name: "PLAN with json format",
			config: ExplainConfig{
				Type:     ExplainPlan,
				Settings: ExplainSettings{JSONFormat: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PLAN json=1 SELECT 1",
		},
		{
			name: "PLAN with header",
			config: ExplainConfig{
				Type:     ExplainPlan,
				Settings: ExplainSettings{Header: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PLAN header=1 SELECT 1",
		},
		{
			name: "PLAN with multiple settings",
			config: ExplainConfig{
				Type: ExplainPlan,
				Settings: ExplainSettings{
					Header:      intPtr(1),
					Description: intPtr(1),
					Indexes:     intPtr(1),
					JSONFormat:  intPtr(1),
				},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PLAN header=1, description=1, indexes=1, json=1 SELECT 1",
		},
		{
			name: "PLAN settings ignored for non-PLAN type",
			config: ExplainConfig{
				Type: ExplainAST,
				Settings: ExplainSettings{
					Indexes:     intPtr(1),
					Description: intPtr(1),
				},
			},
			query: "SELECT 1",
			want:  "EXPLAIN AST SELECT 1",
		},

		// PIPELINE-specific settings
		{
			name: "PIPELINE with graph",
			config: ExplainConfig{
				Type:     ExplainPipeline,
				Settings: ExplainSettings{Graph: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PIPELINE graph=1 SELECT 1",
		},
		{
			name: "PIPELINE with compact",
			config: ExplainConfig{
				Type:     ExplainPipeline,
				Settings: ExplainSettings{Compact: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PIPELINE compact=1 SELECT 1",
		},
		{
			name: "PIPELINE with header",
			config: ExplainConfig{
				Type:     ExplainPipeline,
				Settings: ExplainSettings{Header: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PIPELINE header=1 SELECT 1",
		},
		{
			name: "PIPELINE settings ignored for non-PIPELINE type",
			config: ExplainConfig{
				Type: ExplainPlan,
				Settings: ExplainSettings{
					Graph:   intPtr(1),
					Compact: intPtr(1),
				},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PLAN SELECT 1",
		},

		// SYNTAX-specific settings
		{
			name: "SYNTAX with oneline",
			config: ExplainConfig{
				Type:     ExplainSyntax,
				Settings: ExplainSettings{OneLine: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN SYNTAX oneline=1 SELECT 1",
		},
		{
			name: "SYNTAX with run_query_tree_passes",
			config: ExplainConfig{
				Type:     ExplainSyntax,
				Settings: ExplainSettings{RunQueryTreePasses: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN SYNTAX run_query_tree_passes=1 SELECT 1",
		},
		{
			name: "SYNTAX with query_tree_passes",
			config: ExplainConfig{
				Type:     ExplainSyntax,
				Settings: ExplainSettings{QueryTreePasses: intPtr(5)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN SYNTAX query_tree_passes=5 SELECT 1",
		},
		{
			name: "SYNTAX settings ignored for non-SYNTAX type",
			config: ExplainConfig{
				Type: ExplainPlan,
				Settings: ExplainSettings{
					OneLine:            intPtr(1),
					RunQueryTreePasses: intPtr(1),
				},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PLAN SELECT 1",
		},

		// QUERY TREE-specific settings
		{
			name: "QUERY TREE with run_passes",
			config: ExplainConfig{
				Type:     ExplainQueryTree,
				Settings: ExplainSettings{RunPasses: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN QUERY TREE run_passes=1 SELECT 1",
		},
		{
			name: "QUERY TREE with dump_passes",
			config: ExplainConfig{
				Type:     ExplainQueryTree,
				Settings: ExplainSettings{DumpPasses: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN QUERY TREE dump_passes=1 SELECT 1",
		},
		{
			name: "QUERY TREE with passes",
			config: ExplainConfig{
				Type:     ExplainQueryTree,
				Settings: ExplainSettings{Passes: intPtr(-1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN QUERY TREE passes=-1 SELECT 1",
		},
		{
			name: "QUERY TREE with dump_tree",
			config: ExplainConfig{
				Type:     ExplainQueryTree,
				Settings: ExplainSettings{DumpTree: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN QUERY TREE dump_tree=1 SELECT 1",
		},
		{
			name: "QUERY TREE with dump_ast",
			config: ExplainConfig{
				Type:     ExplainQueryTree,
				Settings: ExplainSettings{DumpAST: intPtr(1)},
			},
			query: "SELECT 1",
			want:  "EXPLAIN QUERY TREE dump_ast=1 SELECT 1",
		},
		{
			name: "QUERY TREE with multiple settings",
			config: ExplainConfig{
				Type: ExplainQueryTree,
				Settings: ExplainSettings{
					RunPasses: intPtr(1),
					DumpTree:  intPtr(1),
					DumpAST:   intPtr(1),
				},
			},
			query: "SELECT 1",
			want:  "EXPLAIN QUERY TREE run_passes=1, dump_tree=1, dump_ast=1 SELECT 1",
		},
		{
			name: "QUERY TREE settings ignored for non-QUERY TREE type",
			config: ExplainConfig{
				Type: ExplainPlan,
				Settings: ExplainSettings{
					RunPasses: intPtr(1),
					DumpTree:  intPtr(1),
				},
			},
			query: "SELECT 1",
			want:  "EXPLAIN PLAN SELECT 1",
		},

		// SETTINGS clause - log_comment
		{
			name:       "with log_comment",
			config:     ExplainConfig{Type: ExplainPlan},
			query:      "SELECT 1",
			logComment: `{"product":"test"}`,
			want:       `EXPLAIN PLAN SELECT 1 SETTINGS log_comment='{"product":"test"}'`,
		},
		{
			name:       "empty log_comment not added",
			config:     ExplainConfig{Type: ExplainPlan},
			query:      "SELECT 1",
			logComment: "",
			want:       "EXPLAIN PLAN SELECT 1",
		},

		// SETTINGS clause - max_execution_time
		{
			name:               "with max_execution_time 10 seconds",
			config:             ExplainConfig{Type: ExplainPlan},
			query:              "SELECT 1",
			maxExecutionTimeMs: 10000,
			want:               "EXPLAIN PLAN SELECT 1 SETTINGS max_execution_time=10.000",
		},
		{
			name:               "with max_execution_time 1ms",
			config:             ExplainConfig{Type: ExplainPlan},
			query:              "SELECT 1",
			maxExecutionTimeMs: 1,
			want:               "EXPLAIN PLAN SELECT 1 SETTINGS max_execution_time=0.001",
		},
		{
			name:               "with max_execution_time 1.5 seconds",
			config:             ExplainConfig{Type: ExplainPlan},
			query:              "SELECT 1",
			maxExecutionTimeMs: 1500,
			want:               "EXPLAIN PLAN SELECT 1 SETTINGS max_execution_time=1.500",
		},
		{
			name:               "max_execution_time 0 not added",
			config:             ExplainConfig{Type: ExplainPlan},
			query:              "SELECT 1",
			maxExecutionTimeMs: 0,
			want:               "EXPLAIN PLAN SELECT 1",
		},
		{
			name:               "negative max_execution_time not added",
			config:             ExplainConfig{Type: ExplainPlan},
			query:              "SELECT 1",
			maxExecutionTimeMs: -100,
			want:               "EXPLAIN PLAN SELECT 1",
		},

		// SETTINGS clause - forceAnalyzer
		{
			name:          "forceAnalyzer adds enable_analyzer for QUERY TREE",
			config:        ExplainConfig{Type: ExplainQueryTree},
			query:         "SELECT 1",
			forceAnalyzer: true,
			want:          "EXPLAIN QUERY TREE SELECT 1 SETTINGS enable_analyzer=1",
		},
		{
			name:          "forceAnalyzer ignored for PLAN",
			config:        ExplainConfig{Type: ExplainPlan},
			query:         "SELECT 1",
			forceAnalyzer: true,
			want:          "EXPLAIN PLAN SELECT 1",
		},
		{
			name:          "forceAnalyzer ignored for PIPELINE",
			config:        ExplainConfig{Type: ExplainPipeline},
			query:         "SELECT 1",
			forceAnalyzer: true,
			want:          "EXPLAIN PIPELINE SELECT 1",
		},
		{
			name:          "forceAnalyzer ignored for AST",
			config:        ExplainConfig{Type: ExplainAST},
			query:         "SELECT 1",
			forceAnalyzer: true,
			want:          "EXPLAIN AST SELECT 1",
		},
		{
			name:          "forceAnalyzer false does not add enable_analyzer",
			config:        ExplainConfig{Type: ExplainQueryTree},
			query:         "SELECT 1",
			forceAnalyzer: false,
			want:          "EXPLAIN QUERY TREE SELECT 1",
		},

		// SETTINGS clause - all combined
		{
			name:               "all SETTINGS combined",
			config:             ExplainConfig{Type: ExplainQueryTree},
			query:              "SELECT 1",
			logComment:         `{"test":true}`,
			forceAnalyzer:      true,
			maxExecutionTimeMs: 5000,
			want:               `EXPLAIN QUERY TREE SELECT 1 SETTINGS log_comment='{"test":true}', enable_analyzer=1, max_execution_time=5.000`,
		},
		{
			name: "settings and SETTINGS clause combined",
			config: ExplainConfig{
				Type: ExplainPlan,
				Settings: ExplainSettings{
					Indexes:    intPtr(1),
					JSONFormat: intPtr(1),
				},
			},
			query:              "SELECT * FROM table",
			logComment:         `{"v":"1"}`,
			maxExecutionTimeMs: 30000,
			want:               `EXPLAIN PLAN indexes=1, json=1 SELECT * FROM table SETTINGS log_comment='{"v":"1"}', max_execution_time=30.000`,
		},

		// Edge cases - query variations
		{
			name:   "empty query",
			config: ExplainConfig{Type: ExplainPlan},
			query:  "",
			want:   "EXPLAIN PLAN ",
		},
		{
			name:   "complex query with joins",
			config: ExplainConfig{Type: ExplainPlan},
			query:  "SELECT a.id, b.name FROM table_a a JOIN table_b b ON a.id = b.a_id WHERE a.status = 'active'",
			want:   "EXPLAIN PLAN SELECT a.id, b.name FROM table_a a JOIN table_b b ON a.id = b.a_id WHERE a.status = 'active'",
		},
		{
			name:   "query with special characters",
			config: ExplainConfig{Type: ExplainPlan},
			query:  "SELECT * FROM t WHERE name = 'O''Brien'",
			want:   "EXPLAIN PLAN SELECT * FROM t WHERE name = 'O''Brien'",
		},
		{
			name:   "query with newlines",
			config: ExplainConfig{Type: ExplainPlan},
			query:  "SELECT *\nFROM table\nWHERE id = 1",
			want:   "EXPLAIN PLAN SELECT *\nFROM table\nWHERE id = 1",
		},

		// Default configs test
		{
			name: "default PLAN config",
			config: ExplainConfig{
				Type: ExplainPlan,
				Settings: ExplainSettings{
					Indexes:     intPtr(1),
					Description: intPtr(1),
					JSONFormat:  intPtr(1),
				},
				Enabled: true,
			},
			query:              "SELECT 1",
			logComment:         `{"query_version":"abc123","product":"clicktelligence"}`,
			maxExecutionTimeMs: 30000,
			want:               `EXPLAIN PLAN description=1, indexes=1, json=1 SELECT 1 SETTINGS log_comment='{"query_version":"abc123","product":"clicktelligence"}', max_execution_time=30.000`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.BuildExplainQuery(tt.query, tt.logComment, tt.forceAnalyzer, tt.maxExecutionTimeMs)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetDefaultExplainConfigs(t *testing.T) {
	configs := GetDefaultExplainConfigs()

	assert.Len(t, configs, 6, "should return 6 default configs")

	// Verify each config type is present and enabled
	types := make(map[ExplainType]bool)
	for _, c := range configs {
		types[c.Type] = c.Enabled
	}

	assert.True(t, types[ExplainPlan], "PLAN should be enabled")
	assert.True(t, types[ExplainPipeline], "PIPELINE should be enabled")
	assert.True(t, types[ExplainEstimate], "ESTIMATE should be enabled")
	assert.True(t, types[ExplainAST], "AST should be enabled")
	assert.True(t, types[ExplainSyntax], "SYNTAX should be enabled")
	assert.True(t, types[ExplainQueryTree], "QUERY TREE should be enabled")
}

func TestBuildSettings(t *testing.T) {
	tests := []struct {
		name     string
		config   ExplainConfig
		expected string
	}{
		{
			name:     "empty settings",
			config:   ExplainConfig{Type: ExplainPlan},
			expected: "",
		},
		{
			name: "header setting applies to all types",
			config: ExplainConfig{
				Type:     ExplainPlan,
				Settings: ExplainSettings{Header: intPtr(1)},
			},
			expected: "header=1",
		},
		{
			name: "setting with value 0",
			config: ExplainConfig{
				Type:     ExplainSyntax,
				Settings: ExplainSettings{OneLine: intPtr(0)},
			},
			expected: "oneline=0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.buildSettings()
			assert.Equal(t, tt.expected, got)
		})
	}
}
