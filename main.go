package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/orian/clicktelligence/models"
)

// Server handles HTTP requests and coordinates between ClickHouse and storage.
type Server struct {
	storage models.Storage
	chConn  driver.Conn
}

func NewServer(storage models.Storage, chConn driver.Conn) *Server {
	return &Server{
		storage: storage,
		chConn:  chConn,
	}
}

func (s *Server) handleGetBranches(w http.ResponseWriter, r *http.Request) {
	branches, err := s.storage.GetBranches()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(branches)
}

func (s *Server) handleCreateBranch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name                string `json:"name"`
		ParentBranchID      string `json:"parentBranchId"`
		BranchFromVersionID string `json:"branchFromVersionId,omitempty"`
		InitialQuery        string `json:"initialQuery,omitempty"`
		CreateInitialVer    bool   `json:"createInitialVersion,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	branch, err := s.storage.CreateBranch(req.Name, req.ParentBranchID, req.BranchFromVersionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create initial version if requested
	if req.CreateInitialVer {
		placeholderQuery := req.InitialQuery
		if placeholderQuery == "" {
			placeholderQuery = "-- New query branch\n-- Start writing your ClickHouse query here\n\nSELECT 1"
		}

		// Create a placeholder version
		queryHash := hashQuery(placeholderQuery)
		version := &models.QueryVersion{
			ID:             uuid.New().String(),
			BranchID:       branch.ID,
			Query:          placeholderQuery,
			QueryHash:      queryHash,
			ExplainResults: []models.ExplainResult{},
			ExecutionStats: make(map[string]interface{}),
			Timestamp:      time.Now(),
		}

		if err := s.storage.SaveVersion(version); err != nil {
			log.Printf("Warning: failed to create initial version: %v", err)
		} else {
			log.Printf("Created initial version for new tree branch '%s'", branch.Name)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(branch)
}

// Default max execution time for EXPLAIN queries (in milliseconds)
const DefaultMaxExecutionTimeMs = 1345 // 1.345 seconds

func (s *Server) handleExplainQuery(w http.ResponseWriter, r *http.Request) {
	// 1. Parse request
	var req ExplainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 2. Check auto-branching
	branchResult, err := checkAutoBranch(s.storage, req.BranchID, req.ParentVersionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 3. Get and filter configs
	configs := getExplainConfigs(req.ExplainConfigs)
	configs = filterExplainConfigs(configs, req.ServerSettings, req.ForceAnalyzer)

	// 4. Generate query hash
	queryHash := hashQuery(req.Query)

	// 5. Check cache - return early if query unchanged
	if cached, ok := checkCachedVersion(s.storage, req.ParentVersionID, queryHash); ok {
		response := buildExplainResponse(cached, false, nil, true)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	// 6. Prepare execution options
	maxExecutionTimeMs := req.MaxExecutionTimeMs
	if maxExecutionTimeMs <= 0 {
		maxExecutionTimeMs = DefaultMaxExecutionTimeMs
	}

	log.Printf("Executing %d EXPLAIN(s) for query hash: %s (forceAnalyzer=%v, maxExecutionTimeMs=%d)",
		len(configs), queryHash, req.ForceAnalyzer, maxExecutionTimeMs)

	// 7. Execute EXPLAINs
	executor := NewExplainExecutor(s.chConn)
	opts := ExplainOptions{
		LogComment:         buildLogComment(queryHash),
		ForceAnalyzer:      req.ForceAnalyzer,
		MaxExecutionTimeMs: maxExecutionTimeMs,
	}
	results := executor.ExecuteAll(r.Context(), configs, req.Query, opts)

	// 8. Create and save version
	version := createVersion(branchResult.TargetBranchID, &req, queryHash, results)
	if err := s.storage.SaveVersion(version); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 9. Build and send response
	response := buildExplainResponse(version, branchResult.AutoBranched, branchResult.NewBranch, false)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	branchID := r.URL.Query().Get("branchId")
	if branchID == "" {
		http.Error(w, "branchId required", http.StatusBadRequest)
		return
	}

	history, err := s.storage.GetBranchHistory(branchID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(history)
}

func (s *Server) handleGetExplainConfigs(w http.ResponseWriter, r *http.Request) {
	configs := models.GetDefaultExplainConfigs()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(configs)
}

func (s *Server) handleGetServerSettings(w http.ResponseWriter, r *http.Request) {
	// Query specific settings we need
	settings := make(map[string]string)

	// Check enable_analyzer setting
	var value string
	err := s.chConn.QueryRow(context.Background(),
		"SELECT value FROM system.settings WHERE name = 'enable_analyzer'").Scan(&value)

	if err != nil {
		log.Printf("Failed to get enable_analyzer setting: %v", err)
		// Default to 0 if we can't fetch it
		settings["enable_analyzer"] = "0"
	} else {
		settings["enable_analyzer"] = value
	}

	// Get connection host info from environment
	settings["host"] = os.Getenv("CLICKHOUSE_HOST")
	if settings["host"] == "" {
		settings["host"] = "localhost:9000"
	}

	// Get database name
	settings["database"] = os.Getenv("CLICKHOUSE_DATABASE")
	if settings["database"] == "" {
		settings["database"] = "default"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(settings)
}

func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	// Try to ping ClickHouse
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.chConn.Ping(ctx)

	response := map[string]interface{}{
		"connected": err == nil,
		"timestamp": time.Now().Unix(),
	}

	if err != nil {
		response["error"] = err.Error()
		log.Printf("ClickHouse ping failed: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetVersionTags(w http.ResponseWriter, r *http.Request) {
	versionID := chi.URLParam(r, "versionId")

	tags, err := s.storage.GetVersionTags(versionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tags)
}

func (s *Server) handleAddTag(w http.ResponseWriter, r *http.Request) {
	versionID := chi.URLParam(r, "versionId")

	var req struct {
		Tag string `json:"tag"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tag, err := s.storage.AddTag(versionID, req.Tag)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tag)
}

func (s *Server) handleDeleteTag(w http.ResponseWriter, r *http.Request) {
	tagID := chi.URLParam(r, "tagId")

	if err := s.storage.RemoveTag(tagID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleToggleStar(w http.ResponseWriter, r *http.Request) {
	versionID := chi.URLParam(r, "versionId")

	isStarred, err := s.storage.ToggleStarred(versionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"starred": isStarred})
}

func maskPassword(password string) string {
	if password == "" {
		return "<empty>"
	}
	if len(password) == 1 {
		return password
	}
	if len(password) == 2 {
		return password
	}
	return string(password[0]) + strings.Repeat("*", len(password)-2) + string(password[len(password)-1])
}

func hashQuery(query string) string {
	hash := sha256.Sum256([]byte(query))
	return hex.EncodeToString(hash[:])
}

func buildLogComment(queryHash string) string {
	comment := map[string]string{
		"query_version": queryHash,
		"product":       "clicktelligence",
	}
	commentJSON, _ := json.Marshal(comment)
	return string(commentJSON)
}

func main() {
	// Get ClickHouse credentials from environment
	chUser := os.Getenv("CLICKHOUSE_USER")
	chPassword := os.Getenv("CLICKHOUSE_PASSWORD")
	chHost := os.Getenv("CLICKHOUSE_HOST")
	chDatabase := os.Getenv("CLICKHOUSE_DATABASE")

	if chHost == "" {
		chHost = "localhost:9000"
	}
	if chUser == "" {
		chUser = "default"
	}
	if chDatabase == "" {
		chDatabase = "default"
	}

	// Detect if we need secure connection (port 9440 or CLICKHOUSE_SECURE=true)
	useSecure := strings.Contains(chHost, ":9440") || os.Getenv("CLICKHOUSE_SECURE") == "true"

	// Print connection details
	log.Println("=== ClickHouse Connection Details ===")
	log.Printf("Host: %s", chHost)
	log.Printf("Database: %s", chDatabase)
	log.Printf("User: %s", chUser)
	log.Printf("Password: %s", maskPassword(chPassword))
	log.Printf("Secure: %v", useSecure)
	log.Println("=====================================")

	// Configure ClickHouse connection options
	options := &clickhouse.Options{
		Addr: []string{chHost},
		Auth: clickhouse.Auth{
			Database: chDatabase,
			Username: chUser,
			Password: chPassword,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "clicktelligence", Version: "1.0"},
			},
		},
		// Disable debug logging which might expose workstation info
		Debug: false,
		// Disable sending workstation/OS metadata
		Settings: clickhouse.Settings{
			"send_logs_level": "none",
		},
	}

	// Configure TLS for secure connections
	if useSecure {
		options.TLS = &tls.Config{
			InsecureSkipVerify: true, // Equivalent to --accept-invalid-certificate
		}
		log.Printf("Using secure connection to ClickHouse (TLS enabled, accepting invalid certificates)")
	}

	// Connect to ClickHouse
	conn, err := clickhouse.Open(options)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}

	// Test connection
	if err := conn.Ping(context.Background()); err != nil {
		log.Printf("Warning: ClickHouse ping failed: %v", err)
	} else {
		log.Println("Successfully connected to ClickHouse")
	}

	// Initialize DuckDB storage
	dbPath := os.Getenv("DUCKDB_PATH")
	if dbPath == "" {
		dbPath = "./clicktelligence.db"
	}
	storage, err := NewDuckDBStorage(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()
	log.Printf("DuckDB storage initialized at: %s", dbPath)

	// Initialize server
	server := NewServer(storage, conn)

	// Setup chi router
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)

	// API routes
	r.Route("/api", func(r chi.Router) {
		// Branches
		r.Get("/branches", server.handleGetBranches)
		r.Post("/branches", server.handleCreateBranch)

		// Query execution
		r.Post("/query/explain", server.handleExplainQuery)
		r.Get("/explain/configs", server.handleGetExplainConfigs)
		r.Get("/history", server.handleGetHistory)
		r.Get("/server/settings", server.handleGetServerSettings)
		r.Get("/server/ping", server.handlePing)

		// Version tags
		r.Route("/versions/{versionId}", func(r chi.Router) {
			r.Get("/tags", server.handleGetVersionTags)
			r.Post("/tags", server.handleAddTag)
			r.Post("/star", server.handleToggleStar)
		})

		// Tag deletion
		r.Delete("/tags/{tagId}", server.handleDeleteTag)
	})

	// Static files
	r.Handle("/*", http.FileServer(http.Dir("./static")))

	port := "8080"
	log.Printf("Starting server on http://localhost:%s", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
