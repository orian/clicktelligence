package models

import (
	"fmt"
	"strings"
	"time"
)

// VersionTag represents a tag on a query version.
// Tags can be simple (just a key) or key-value pairs.
//
// Examples:
//   - Simple tag: {TagKey: "production", TagValue: ""}
//   - Key-value tag: {TagKey: "environment", TagValue: "staging"}
//   - System tag: {TagKey: "system:starred", TagValue: ""}
type VersionTag struct {
	// ID is the unique identifier for this tag (UUID).
	ID string `json:"id"`

	// VersionID references the version this tag belongs to.
	VersionID string `json:"versionId"`

	// TagKey is the tag name or key.
	// System tags are prefixed with "system:" (e.g., "system:starred").
	TagKey string `json:"tagKey"`

	// TagValue is the optional tag value for key-value tags.
	// Empty for simple tags.
	TagValue string `json:"tagValue,omitempty"`

	// CreatedAt is when this tag was created.
	CreatedAt time.Time `json:"createdAt"`
}

// ParseTag parses a tag string into key and value components.
//
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

// FormatTag formats a tag back to its string representation.
// Returns "key" for simple tags or "key=value" for key-value tags.
func (t *VersionTag) FormatTag() string {
	if t.TagValue == "" {
		return t.TagKey
	}
	return fmt.Sprintf("%s=%s", t.TagKey, t.TagValue)
}

// IsSystemTag checks if a tag is a system reserved tag.
// System tags are prefixed with "system:" and are used for
// internal functionality like starring versions.
func (t *VersionTag) IsSystemTag() bool {
	return strings.HasPrefix(t.TagKey, "system:")
}
