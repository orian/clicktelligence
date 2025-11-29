package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/orian/clicktelligence/models"
)

type DuckDBStorage struct {
	db *sql.DB
}

func NewDuckDBStorage(dbPath string) (*DuckDBStorage, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	storage := &DuckDBStorage{db: db}
	if err := storage.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Run migrations
	if err := RunMigrations(db); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	// Create default main branch if it doesn't exist
	if err := storage.ensureMainBranch(); err != nil {
		return nil, fmt.Errorf("failed to create main branch: %w", err)
	}

	return storage, nil
}

func (s *DuckDBStorage) initSchema() error {
	schema := `
		CREATE TABLE IF NOT EXISTS branches (
			id VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			parent_branch_id VARCHAR,
			current_version_id VARCHAR,
			branch_from_version_id VARCHAR,
			created_at TIMESTAMP NOT NULL
		);

		CREATE TABLE IF NOT EXISTS query_versions (
			id VARCHAR PRIMARY KEY,
			branch_id VARCHAR NOT NULL,
			query TEXT NOT NULL,
			query_hash VARCHAR NOT NULL,
			explain_results TEXT,
			execution_stats TEXT,
			timestamp TIMESTAMP NOT NULL,
			parent_version_id VARCHAR
		);

		CREATE TABLE IF NOT EXISTS version_tags (
			id VARCHAR PRIMARY KEY,
			version_id VARCHAR NOT NULL,
			tag_key VARCHAR NOT NULL,
			tag_value VARCHAR,
			created_at TIMESTAMP NOT NULL,
			FOREIGN KEY (version_id) REFERENCES query_versions(id)
		);
	`

	_, err := s.db.Exec(schema)
	return err
}

func (s *DuckDBStorage) ensureMainBranch() error {
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM branches WHERE name = 'main'").Scan(&count)
	if err != nil {
		return err
	}

	if count == 0 {
		_, err = s.db.Exec(
			"INSERT INTO branches (id, name, parent_branch_id, branch_from_version_id, current_version_id, created_at) VALUES (?, ?, NULL, NULL, NULL, ?)",
			generateID(), "main", time.Now(),
		)
		return err
	}

	return nil
}

func (s *DuckDBStorage) CreateBranch(name, parentBranchID, branchFromVersionID string) (*models.Branch, error) {
	branch := &models.Branch{
		ID:                  generateID(),
		Name:                name,
		ParentBranchID:      parentBranchID,
		BranchFromVersionID: branchFromVersionID,
		CreatedAt:           time.Now(),
	}

	_, err := s.db.Exec(
		"INSERT INTO branches (id, name, parent_branch_id, branch_from_version_id, current_version_id, created_at) VALUES (?, ?, ?, ?, NULL, ?)",
		branch.ID, branch.Name, nullString(branch.ParentBranchID), nullString(branch.BranchFromVersionID), branch.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	return branch, nil
}

func (s *DuckDBStorage) GetBranches() ([]*models.Branch, error) {
	rows, err := s.db.Query(`
		SELECT id, name, COALESCE(parent_branch_id, ''), COALESCE(branch_from_version_id, ''), COALESCE(current_version_id, ''), created_at
		FROM branches
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var branches []*models.Branch
	for rows.Next() {
		var b models.Branch
		if err := rows.Scan(&b.ID, &b.Name, &b.ParentBranchID, &b.BranchFromVersionID, &b.CurrentVersionID, &b.CreatedAt); err != nil {
			return nil, err
		}
		branches = append(branches, &b)
	}

	return branches, rows.Err()
}

func (s *DuckDBStorage) GetBranch(id string) (*models.Branch, bool) {
	var b models.Branch
	err := s.db.QueryRow(
		"SELECT id, name, COALESCE(parent_branch_id, ''), COALESCE(branch_from_version_id, ''), COALESCE(current_version_id, ''), created_at FROM branches WHERE id = ?",
		id,
	).Scan(&b.ID, &b.Name, &b.ParentBranchID, &b.BranchFromVersionID, &b.CurrentVersionID, &b.CreatedAt)

	if err != nil {
		return nil, false
	}

	return &b, true
}

func (s *DuckDBStorage) GetVersion(id string) (*models.QueryVersion, bool) {
	var v models.QueryVersion
	var explainResultsJSON string
	var statsJSON string

	err := s.db.QueryRow(`
		SELECT id, branch_id, query, query_hash, COALESCE(explain_results, '[]'), COALESCE(execution_stats, '{}'), timestamp, COALESCE(parent_version_id, '')
		FROM query_versions
		WHERE id = ?
	`, id).Scan(&v.ID, &v.BranchID, &v.Query, &v.QueryHash, &explainResultsJSON, &statsJSON, &v.Timestamp, &v.ParentVersionID)

	if err != nil {
		return nil, false
	}

	// Unmarshal explain results
	v.ExplainResults = []models.ExplainResult{}
	if explainResultsJSON != "" && explainResultsJSON != "[]" {
		if err := json.Unmarshal([]byte(explainResultsJSON), &v.ExplainResults); err != nil {
			fmt.Printf("Warning: failed to unmarshal explain results for version %s: %v\n", v.ID, err)
		}
	}

	// Initialize empty map if unmarshaling fails
	v.ExecutionStats = make(map[string]interface{})
	if statsJSON != "" && statsJSON != "{}" {
		if err := json.Unmarshal([]byte(statsJSON), &v.ExecutionStats); err != nil {
			fmt.Printf("Warning: failed to unmarshal stats for version %s: %v\n", v.ID, err)
		}
	}

	return &v, true
}

func (s *DuckDBStorage) SaveVersion(version *models.QueryVersion) error {
	statsJSON, err := json.Marshal(version.ExecutionStats)
	if err != nil {
		return fmt.Errorf("failed to marshal execution stats: %w", err)
	}

	explainResultsJSON, err := json.Marshal(version.ExplainResults)
	if err != nil {
		return fmt.Errorf("failed to marshal explain results: %w", err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert version
	_, err = tx.Exec(
		`INSERT INTO query_versions (id, branch_id, query, query_hash, explain_results, execution_stats, timestamp, parent_version_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		version.ID, version.BranchID, version.Query, version.QueryHash, string(explainResultsJSON),
		string(statsJSON), version.Timestamp, nullString(version.ParentVersionID),
	)
	if err != nil {
		return err
	}

	// Update branch's current version
	_, err = tx.Exec(
		"UPDATE branches SET current_version_id = ? WHERE id = ?",
		version.ID, version.BranchID,
	)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *DuckDBStorage) GetBranchHistory(branchID string) ([]*models.QueryVersion, error) {
	rows, err := s.db.Query(`
		SELECT id, branch_id, query, query_hash, COALESCE(explain_results, '[]'), COALESCE(execution_stats, '{}'), timestamp, COALESCE(parent_version_id, '')
		FROM query_versions
		WHERE branch_id = ?
		ORDER BY timestamp DESC
	`, branchID)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var versions []*models.QueryVersion
	var versionIDs []string
	for rows.Next() {
		var v models.QueryVersion
		var explainResultsJSON string
		var statsJSON string
		if err := rows.Scan(&v.ID, &v.BranchID, &v.Query, &v.QueryHash, &explainResultsJSON, &statsJSON, &v.Timestamp, &v.ParentVersionID); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		// Unmarshal explain results
		v.ExplainResults = []models.ExplainResult{}
		if explainResultsJSON != "" && explainResultsJSON != "[]" {
			if err := json.Unmarshal([]byte(explainResultsJSON), &v.ExplainResults); err != nil {
				fmt.Printf("Warning: failed to unmarshal explain results for version %s: %v\n", v.ID, err)
			}
		}

		// Initialize empty map if unmarshaling fails
		v.ExecutionStats = make(map[string]interface{})
		if statsJSON != "" && statsJSON != "{}" {
			if err := json.Unmarshal([]byte(statsJSON), &v.ExecutionStats); err != nil {
				// Log error but continue with empty stats
				fmt.Printf("Warning: failed to unmarshal stats for version %s: %v\n", v.ID, err)
			}
		}

		v.Tags = []*models.VersionTag{}
		versions = append(versions, &v)
		versionIDs = append(versionIDs, v.ID)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Load all tags for these versions in one query
	if len(versionIDs) > 0 {
		tags, err := s.getTagsForVersions(versionIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to load tags: %w", err)
		}

		// Map tags to versions
		tagsByVersion := make(map[string][]*models.VersionTag)
		for _, tag := range tags {
			tagsByVersion[tag.VersionID] = append(tagsByVersion[tag.VersionID], tag)
		}

		// Attach tags to versions
		for _, version := range versions {
			if tags, ok := tagsByVersion[version.ID]; ok {
				version.Tags = tags
			}
		}
	}

	return versions, nil
}

// Helper function to get tags for multiple versions in one query
func (s *DuckDBStorage) getTagsForVersions(versionIDs []string) ([]*models.VersionTag, error) {
	if len(versionIDs) == 0 {
		return []*models.VersionTag{}, nil
	}

	// Build placeholders for IN clause
	placeholders := make([]string, len(versionIDs))
	args := make([]interface{}, len(versionIDs))
	for i, id := range versionIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, version_id, tag_key, COALESCE(tag_value, ''), created_at
		FROM version_tags
		WHERE version_id IN (%s)
		ORDER BY created_at ASC
	`, string(placeholders[0]))

	// For multiple placeholders, we need to construct the query properly
	if len(versionIDs) > 1 {
		query = fmt.Sprintf(`
			SELECT id, version_id, tag_key, COALESCE(tag_value, ''), created_at
			FROM version_tags
			WHERE version_id IN (%s)
			ORDER BY created_at ASC
		`, joinPlaceholders(placeholders))
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tags []*models.VersionTag
	for rows.Next() {
		var tag models.VersionTag
		if err := rows.Scan(&tag.ID, &tag.VersionID, &tag.TagKey, &tag.TagValue, &tag.CreatedAt); err != nil {
			return nil, err
		}
		tags = append(tags, &tag)
	}

	return tags, rows.Err()
}

// Helper to join placeholders for SQL IN clause
func joinPlaceholders(placeholders []string) string {
	result := ""
	for i, p := range placeholders {
		if i > 0 {
			result += ", "
		}
		result += p
	}
	return result
}

func (s *DuckDBStorage) Close() error {
	return s.db.Close()
}

// Helper functions
func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func generateID() string {
	return uuid.New().String()
}
