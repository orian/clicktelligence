package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// VersionTag represents a tag on a query version
type VersionTag struct {
	ID        string    `json:"id"`
	VersionID string    `json:"versionId"`
	TagKey    string    `json:"tagKey"`
	TagValue  string    `json:"tagValue,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

// ParseTag parses a tag string into key and value
// Examples:
//   - "production" -> key="production", value=""
//   - "environment=staging" -> key="environment", value="staging"
//   - "system:starred" -> key="system:starred", value=""
func ParseTag(tag string) (key string, value string) {
	parts := strings.SplitN(tag, "=", 2)
	key = strings.TrimSpace(parts[0])
	if len(parts) == 2 {
		value = strings.TrimSpace(parts[1])
	}
	return key, value
}

// FormatTag formats a tag back to string representation
func (t *VersionTag) FormatTag() string {
	if t.TagValue == "" {
		return t.TagKey
	}
	return fmt.Sprintf("%s=%s", t.TagKey, t.TagValue)
}

// IsSystemTag checks if a tag is a system reserved tag
func (t *VersionTag) IsSystemTag() bool {
	return strings.HasPrefix(t.TagKey, "system:")
}

// Tag management methods for DuckDBStorage

// AddTag adds a tag to a version
func (s *DuckDBStorage) AddTag(versionID, tag string) (*VersionTag, error) {
	key, value := ParseTag(tag)

	// Check if tag already exists
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM version_tags
		WHERE version_id = ? AND tag_key = ? AND COALESCE(tag_value, '') = ?
	`, versionID, key, value).Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing tag: %w", err)
	}

	if count > 0 {
		return nil, fmt.Errorf("tag already exists on this version")
	}

	// Create new tag
	tagObj := &VersionTag{
		ID:        uuid.New().String(),
		VersionID: versionID,
		TagKey:    key,
		TagValue:  value,
		CreatedAt: time.Now(),
	}

	_, err = s.db.Exec(`
		INSERT INTO version_tags (id, version_id, tag_key, tag_value, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, tagObj.ID, tagObj.VersionID, tagObj.TagKey, nullString(tagObj.TagValue), tagObj.CreatedAt)

	if err != nil {
		return nil, fmt.Errorf("failed to insert tag: %w", err)
	}

	return tagObj, nil
}

// RemoveTag removes a tag from a version
func (s *DuckDBStorage) RemoveTag(tagID string) error {
	result, err := s.db.Exec("DELETE FROM version_tags WHERE id = ?", tagID)
	if err != nil {
		return fmt.Errorf("failed to delete tag: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("tag not found")
	}

	return nil
}

// GetVersionTags gets all tags for a version
func (s *DuckDBStorage) GetVersionTags(versionID string) ([]*VersionTag, error) {
	rows, err := s.db.Query(`
		SELECT id, version_id, tag_key, COALESCE(tag_value, ''), created_at
		FROM version_tags
		WHERE version_id = ?
		ORDER BY created_at ASC
	`, versionID)
	if err != nil {
		return nil, fmt.Errorf("failed to query tags: %w", err)
	}
	defer rows.Close()

	var tags []*VersionTag
	for rows.Next() {
		var tag VersionTag
		if err := rows.Scan(&tag.ID, &tag.VersionID, &tag.TagKey, &tag.TagValue, &tag.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan tag: %w", err)
		}
		tags = append(tags, &tag)
	}

	return tags, rows.Err()
}

// GetVersionsByTag finds versions that have a specific tag
func (s *DuckDBStorage) GetVersionsByTag(branchID, tag string) ([]*QueryVersion, error) {
	key, value := ParseTag(tag)

	query := `
		SELECT DISTINCT qv.id, qv.branch_id, qv.query, qv.query_hash,
		       COALESCE(qv.explain_results, '[]'), qv.explain_plan,
		       COALESCE(qv.execution_stats, '{}'), qv.timestamp,
		       COALESCE(qv.parent_version_id, '')
		FROM query_versions qv
		JOIN version_tags vt ON qv.id = vt.version_id
		WHERE qv.branch_id = ? AND vt.tag_key = ? AND COALESCE(vt.tag_value, '') = ?
		ORDER BY qv.timestamp DESC
	`

	rows, err := s.db.Query(query, branchID, key, value)
	if err != nil {
		return nil, fmt.Errorf("failed to query versions by tag: %w", err)
	}
	defer rows.Close()

	var versions []*QueryVersion
	for rows.Next() {
		var v QueryVersion
		var explainResultsJSON string
		var statsJSON string
		if err := rows.Scan(&v.ID, &v.BranchID, &v.Query, &v.QueryHash, &explainResultsJSON, &v.ExplainPlan, &statsJSON, &v.Timestamp, &v.ParentVersionID); err != nil {
			return nil, fmt.Errorf("failed to scan version: %w", err)
		}

		// Unmarshal JSON fields (same as GetBranchHistory)
		v.ExplainResults = []ExplainResult{}
		if explainResultsJSON != "" && explainResultsJSON != "[]" {
			if err := json.Unmarshal([]byte(explainResultsJSON), &v.ExplainResults); err != nil {
				fmt.Printf("Warning: failed to unmarshal explain results for version %s: %v\n", v.ID, err)
			}
		}

		v.ExecutionStats = make(map[string]interface{})
		if statsJSON != "" && statsJSON != "{}" {
			if err := json.Unmarshal([]byte(statsJSON), &v.ExecutionStats); err != nil {
				fmt.Printf("Warning: failed to unmarshal stats for version %s: %v\n", v.ID, err)
			}
		}

		versions = append(versions, &v)
	}

	return versions, rows.Err()
}

// ToggleStarred toggles the system:starred tag on a version
func (s *DuckDBStorage) ToggleStarred(versionID string) (bool, error) {
	// Check if starred tag exists
	var tagID string
	err := s.db.QueryRow(`
		SELECT id FROM version_tags
		WHERE version_id = ? AND tag_key = 'system:starred'
	`, versionID).Scan(&tagID)

	if err == sql.ErrNoRows {
		// Not starred, add the star
		_, err := s.AddTag(versionID, "system:starred")
		if err != nil {
			return false, fmt.Errorf("failed to star version: %w", err)
		}
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to check star status: %w", err)
	}

	// Already starred, remove the star
	if err := s.RemoveTag(tagID); err != nil {
		return false, fmt.Errorf("failed to unstar version: %w", err)
	}
	return false, nil
}
