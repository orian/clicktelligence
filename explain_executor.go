package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/orian/clicktelligence/models"
)

// ExplainExecutor handles executing EXPLAIN queries against ClickHouse.
type ExplainExecutor struct {
	conn driver.Conn
}

// NewExplainExecutor creates a new ExplainExecutor with the given connection.
func NewExplainExecutor(conn driver.Conn) *ExplainExecutor {
	return &ExplainExecutor{conn: conn}
}

// ExplainOptions contains options for executing EXPLAIN queries.
type ExplainOptions struct {
	LogComment         string
	ForceAnalyzer      bool
	MaxExecutionTimeMs int
}

// ExecuteAll executes all enabled EXPLAIN configs and returns the results.
func (e *ExplainExecutor) ExecuteAll(ctx context.Context, configs []models.ExplainConfig, query string, opts ExplainOptions) []models.ExplainResult {
	var results []models.ExplainResult

	for _, config := range configs {
		if !config.Enabled {
			continue
		}

		result := e.ExecuteConfig(ctx, config, query, opts)
		results = append(results, result)
	}

	return results
}

// ExecuteConfig executes a single EXPLAIN config and returns the result.
func (e *ExplainExecutor) ExecuteConfig(ctx context.Context, config models.ExplainConfig, query string, opts ExplainOptions) models.ExplainResult {
	explainQuery := config.BuildExplainQuery(query, opts.LogComment, opts.ForceAnalyzer, opts.MaxExecutionTimeMs)
	log.Printf("Running: EXPLAIN %s: %s", config.Type, explainQuery)

	rows, err := e.conn.Query(ctx, explainQuery)
	if err != nil {
		errMsg := fmt.Sprintf("Query error: %v", err)
		log.Printf("Error executing EXPLAIN %s: %v", config.Type, err)
		return models.ExplainResult{
			Type:  config.Type,
			Error: errMsg,
		}
	}
	defer rows.Close()

	// ESTIMATE type returns structured data
	if config.Type == models.ExplainEstimate {
		estimateRows, err := scanEstimateRows(rows)
		if err != nil {
			return models.ExplainResult{
				Type:  config.Type,
				Error: fmt.Sprintf("Scan error: %v", err),
			}
		}
		return models.ExplainResult{
			Type:     config.Type,
			Estimate: estimateRows,
		}
	}

	// Other types return text output
	lines, err := scanTextRows(rows)
	if err != nil {
		return models.ExplainResult{
			Type:  config.Type,
			Error: fmt.Sprintf("Scan error: %v", err),
		}
	}

	return models.ExplainResult{
		Type:   config.Type,
		Output: strings.Join(lines, "\n"),
	}
}

// scanEstimateRows scans rows from EXPLAIN ESTIMATE query.
// Returns structured EstimateRow data with database, table, parts, rows, marks.
func scanEstimateRows(rows driver.Rows) ([]models.EstimateRow, error) {
	var result []models.EstimateRow

	for rows.Next() {
		var row models.EstimateRow
		if err := rows.Scan(&row.Database, &row.Table, &row.Parts, &row.Rows, &row.Marks); err != nil {
			return nil, err
		}
		result = append(result, row)
	}

	return result, nil
}

// scanTextRows scans rows from EXPLAIN queries that return single text column.
func scanTextRows(rows driver.Rows) ([]string, error) {
	var lines []string

	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}

	return lines, nil
}
