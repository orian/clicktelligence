# Clicktelligence

**ClickHouse Intelligence** - A version-controlled query optimization tool for ClickHouse workloads.

## Overview

Clicktelligence is a project designed to help users better understand and optimize their ClickHouse workloads. The name is a portmanteau of "ClickHouse" and "Intelligence."

## Purpose

The main purpose of clicktelligence is to provide an iterative, exploratory environment for query optimization through:

- **Interactive Query Editor**: Modify queries directly in the browser
- **Performance Tracking**: Track query performance across iterations using EXPLAIN plans and execution statistics
- **Version History**: Maintain a complete history of query changes with corresponding performance metrics
- **Branching & Exploration**: Create different optimization branches to explore alternative approaches
- **Branch Merging**: Merge successful optimizations back into your main query workflow

## Key Features

### Query Optimization Workflow

1. **Paste Initial Query**: Start with your current ClickHouse query
2. **Analyze Performance**: Run EXPLAIN or get execution statistics
3. **Iterate & Branch**: Create branches to explore different optimization strategies
4. **Compare Results**: View performance differences across versions and branches
5. **Merge Success**: Merge the best-performing optimization back

### Version Control for Queries

- Track every change to your queries
- See performance metrics for each version
- Create branches to explore different optimization paths without losing your work
- Merge successful experiments back into the main branch
- **Tag and star versions** for easy identification and organization
  - Star important versions with a single click
  - Add custom tags: simple tags (`production`), key-value tags (`environment=staging`), or system tags (`system:starred`)
  - Filter and search by tags

### Performance Metrics

- **Multiple EXPLAIN Types**: Run multiple EXPLAIN analyses on a single query
  - PLAN: Query execution steps (default, enabled)
  - PIPELINE: Data processing flow
  - ESTIMATE: Resource consumption predictions
  - AST: Abstract syntax tree
  - SYNTAX: Query after optimization
  - QUERY TREE: Optimized query tree
- Query execution statistics
- Historical performance comparison
- Tabbed interface to view different EXPLAIN results

### Query Identification

Every query executed through clicktelligence is automatically tagged with:
- **Query Hash**: SHA-256 hash of the query text for unique identification
- **Log Comment**: JSON metadata attached to queries via ClickHouse `log_comment` setting
  - `query_version`: Hash of the query for tracking
  - `product`: Identifies queries as coming from "clicktelligence"

This enables easy tracking of clicktelligence queries in ClickHouse system tables (e.g., `system.query_log`).

### Privacy

Clicktelligence is configured to **not send workstation metadata** (hostname, OS user, etc.) to ClickHouse servers. The client identifies itself only as "clicktelligence v1.0" without exposing local system information.

## Technology Stack

- **Primary Language**: Go
- **HTTP Router**: [chi](https://github.com/go-chi/chi) - lightweight, idiomatic router
- **Database Driver**: clickhouse-driver for ClickHouse connectivity
- **Local Storage**: DuckDB for persistent query history and metadata
- **UI**: HTML/JavaScript (served locally in browser)

## Configuration

The application uses environment variables for configuration:

- `CLICKHOUSE_HOST`: ClickHouse server address (default: `localhost:9000`)
- `CLICKHOUSE_DATABASE`: ClickHouse database name (default: `default`)
- `CLICKHOUSE_USER`: ClickHouse username (default: `default`)
- `CLICKHOUSE_PASSWORD`: ClickHouse password
- `CLICKHOUSE_SECURE`: Force secure TLS connection (default: `false`, automatically enabled for port `9440`)
- `DUCKDB_PATH`: Path to DuckDB database file (default: `./clicktelligence.db`)

### Secure Connections

The application automatically enables TLS when connecting to port `9440`. The TLS configuration accepts invalid certificates (equivalent to ClickHouse CLI's `--secure --accept-invalid-certificate` options).

For secure connections on other ports, set `CLICKHOUSE_SECURE=true`.

## Use Cases

- **Query Optimization**: Systematically explore different query optimizations
- **Performance Analysis**: Understand how changes affect query performance
- **Team Collaboration**: Share optimization branches and experiments
- **Documentation**: Keep a record of why certain optimizations were chosen

## Getting Started

### Prerequisites

- Go 1.21 or higher
- Access to a ClickHouse instance

### Installation

1. Clone the repository:
```bash
git clone https://github.com/orian/clicktelligence.git
cd clicktelligence
```

2. Set up environment variables (copy and edit .env.example):
```bash
cp .env.example .env
# Edit .env with your ClickHouse credentials
```

3. Install Go dependencies:
```bash
go mod download
```

### Running the PoC

1. Set your ClickHouse connection environment variables:
```bash
export CLICKHOUSE_HOST=localhost:9000
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=your_password
```

Or source from .env file:
```bash
source .env
```

2. Run the application:
```bash
go run main.go
```

3. Open your browser and navigate to:
```
http://localhost:8080
```

### Usage

1. **Select or Create a Branch**: Start with the default "main" branch or create a new branch to explore alternative optimizations

2. **Enter Your Query**: Paste your ClickHouse query into the editor

3. **Run EXPLAIN**: Click "Run EXPLAIN" to analyze the query execution plan

4. **Iterate**: Modify the query and run EXPLAIN again. Each version is saved in the branch history

5. **Branch for Experiments**: Create new branches to try different optimization approaches without losing your work

6. **Compare**: View the history to compare EXPLAIN plans across different versions

## Development

### Rules for AI Assistants

When working on this project, AI assistants should follow these guidelines:

1. **Planning**: Before implementing any plan, write it down to a markdown file in the `/docs` directory
2. **Testing**: Write tests when you want to verify logic works. If a function is complex, extract the logic and test it separately
3. **Python Enums**: Use enums (`from enum import Enum`) for enum types
4. **Python Types**: Where possible, add types (from the `typing` package)
5. **Architecture Changes**: For serious architecture changes, update `CLAUDE.md`
6. **Primary Language**: Go is the primary language for this project
7. **ClickHouse Connection**: Use clickhouse-driver to connect to ClickHouse
8. **Credentials**: Load user/password from environment variables

## Project Structure

(To be documented as the project develops)

## Contributing

(Contributing guidelines to be added)

## License

(License information to be added)
