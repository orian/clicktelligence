package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

// Migration represents a database migration
type Migration struct {
	Version     int
	Description string
	SQL         string
}

// GetMigrations returns all migrations in order
func GetMigrations() []Migration {
	return []Migration{
		{
			Version:     1,
			Description: "Add version_tags table",
			SQL: `
				CREATE TABLE IF NOT EXISTS version_tags (
					id VARCHAR PRIMARY KEY,
					version_id VARCHAR NOT NULL,
					tag_key VARCHAR NOT NULL,
					tag_value VARCHAR,
					created_at TIMESTAMP NOT NULL,
					FOREIGN KEY (version_id) REFERENCES query_versions(id)
				);

				CREATE INDEX IF NOT EXISTS idx_tags_version ON version_tags(version_id);
				CREATE INDEX IF NOT EXISTS idx_tags_key ON version_tags(tag_key);
				CREATE INDEX IF NOT EXISTS idx_tags_key_value ON version_tags(tag_key, tag_value);
			`,
		},
		{
			Version:     2,
			Description: "Add branch_from_version_id to branches table",
			SQL: `
				ALTER TABLE branches ADD COLUMN IF NOT EXISTS branch_from_version_id VARCHAR;
			`,
		},
	}
}

// RunMigrations executes all pending migrations
func RunMigrations(db *sql.DB) error {
	// Create migrations table if it doesn't exist
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			description VARCHAR NOT NULL,
			applied_at TIMESTAMP NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Get current schema version
	var currentVersion int
	err = db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&currentVersion)
	if err != nil {
		return fmt.Errorf("failed to get current schema version: %w", err)
	}

	log.Printf("Current schema version: %d", currentVersion)

	// Apply pending migrations
	migrations := GetMigrations()
	appliedCount := 0

	for _, migration := range migrations {
		if migration.Version <= currentVersion {
			continue
		}

		log.Printf("Applying migration %d: %s", migration.Version, migration.Description)

		// Start transaction
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", migration.Version, err)
		}

		// Execute migration SQL
		_, err = tx.Exec(migration.SQL)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute migration %d: %w", migration.Version, err)
		}

		// Record migration
		_, err = tx.Exec(
			"INSERT INTO schema_migrations (version, description, applied_at) VALUES (?, ?, ?)",
			migration.Version, migration.Description, time.Now(),
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit migration %d: %w", migration.Version, err)
		}

		log.Printf("Successfully applied migration %d", migration.Version)
		appliedCount++
	}

	if appliedCount > 0 {
		log.Printf("Applied %d migration(s)", appliedCount)
	} else {
		log.Println("No pending migrations")
	}

	return nil
}
