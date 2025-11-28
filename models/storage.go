package models

// Storage defines the persistence layer for clicktelligence.
//
// It provides methods for managing query branches, versions, and tags.
// The primary implementation is DuckDBStorage which uses DuckDB for
// local persistent storage.
//
// The interface is organized into three categories:
//   - Branch management: CreateBranch, GetBranches, GetBranch
//   - Version management: GetVersion, SaveVersion, GetBranchHistory
//   - Tag management: AddTag, RemoveTag, GetVersionTags, GetVersionsByTag, ToggleStarred
//
// Thread Safety: Implementations should be safe for concurrent use.
type Storage interface {
	// CreateBranch creates a new branch with the given name.
	//
	// Parameters:
	//   - name: Human-readable branch name
	//   - parentBranchID: ID of the parent branch (empty for root branches)
	//   - branchFromVersionID: ID of the version this branch forks from
	//
	// Returns the created branch or an error if creation fails.
	CreateBranch(name, parentBranchID, branchFromVersionID string) (*Branch, error)

	// GetBranches returns all branches ordered by creation time (newest first).
	GetBranches() ([]*Branch, error)

	// GetBranch retrieves a branch by its ID.
	//
	// Returns the branch and true if found, nil and false otherwise.
	GetBranch(id string) (*Branch, bool)

	// GetVersion retrieves a query version by its ID.
	//
	// The returned version includes its ExplainResults but not Tags.
	// Use GetVersionTags to retrieve tags separately, or GetBranchHistory
	// which includes tags.
	//
	// Returns the version and true if found, nil and false otherwise.
	GetVersion(id string) (*QueryVersion, bool)

	// SaveVersion persists a new query version.
	//
	// This also updates the branch's CurrentVersionID to point to this
	// new version, making it the head of the branch.
	//
	// The version's ID must be set before calling this method.
	SaveVersion(version *QueryVersion) error

	// GetBranchHistory returns all versions for a branch.
	//
	// Versions are ordered by timestamp (newest first) and include
	// their associated tags.
	GetBranchHistory(branchID string) ([]*QueryVersion, error)

	// Close releases any resources held by the storage.
	//
	// After Close is called, the storage should not be used.
	Close() error

	// AddTag adds a tag to a version.
	//
	// Tag format can be:
	//   - Simple tag: "tagname" (e.g., "production", "optimized")
	//   - Key-value tag: "key=value" (e.g., "environment=staging")
	//
	// System tags (prefixed with "system:") are reserved for internal use.
	//
	// Returns the created tag or an error if:
	//   - Tag format is invalid
	//   - Version doesn't exist
	//   - Tag already exists on this version
	AddTag(versionID, tag string) (*VersionTag, error)

	// RemoveTag removes a tag by its ID.
	//
	// Returns an error if the tag doesn't exist.
	RemoveTag(tagID string) error

	// GetVersionTags returns all tags for a specific version.
	//
	// Returns an empty slice if the version has no tags.
	GetVersionTags(versionID string) ([]*VersionTag, error)

	// GetVersionsByTag returns versions matching a tag filter within a branch.
	//
	// Tag format:
	//   - "key": Matches any version with this tag key (any value)
	//   - "key=value": Matches versions with exact key-value pair
	//
	// Results are ordered by timestamp (newest first).
	GetVersionsByTag(branchID, tag string) ([]*QueryVersion, error)

	// ToggleStarred toggles the "system:starred" tag on a version.
	//
	// If the version is starred, it becomes unstarred and vice versa.
	// Returns the new starred state (true if now starred).
	ToggleStarred(versionID string) (bool, error)
}
