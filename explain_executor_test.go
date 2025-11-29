package main

import (
	"encoding/json"
	"testing"

	"github.com/orian/clicktelligence/models"
	"github.com/stretchr/testify/assert"
)

func TestEstimateRowJSON(t *testing.T) {
	tests := []struct {
		name string
		rows []models.EstimateRow
		want string
	}{
		{
			name: "single row",
			rows: []models.EstimateRow{
				{Database: "default", Table: "my_table", Parts: 10, Rows: 1000, Marks: 50},
			},
			want: `[{"database":"default","table":"my_table","parts":10,"rows":1000,"marks":50}]`,
		},
		{
			name: "multiple rows",
			rows: []models.EstimateRow{
				{Database: "db1", Table: "table1", Parts: 5, Rows: 500, Marks: 25},
				{Database: "db2", Table: "table2", Parts: 3, Rows: 300, Marks: 15},
			},
			want: `[{"database":"db1","table":"table1","parts":5,"rows":500,"marks":25},{"database":"db2","table":"table2","parts":3,"rows":300,"marks":15}]`,
		},
		{
			name: "empty result",
			rows: []models.EstimateRow{},
			want: `[]`,
		},
		{
			name: "zero values",
			rows: []models.EstimateRow{
				{Database: "db", Table: "empty_table", Parts: 0, Rows: 0, Marks: 0},
			},
			want: `[{"database":"db","table":"empty_table","parts":0,"rows":0,"marks":0}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonBytes, err := json.Marshal(tt.rows)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, string(jsonBytes))
		})
	}
}

func TestExplainResultWithEstimate(t *testing.T) {
	result := models.ExplainResult{
		Type: models.ExplainEstimate,
		Estimate: []models.EstimateRow{
			{Database: "default", Table: "events", Parts: 100, Rows: 1000000, Marks: 5000},
		},
	}

	jsonBytes, err := json.Marshal(result)
	assert.NoError(t, err)

	// Verify Output is empty and Estimate is present
	var parsed map[string]interface{}
	err = json.Unmarshal(jsonBytes, &parsed)
	assert.NoError(t, err)

	assert.Equal(t, "ESTIMATE", parsed["type"])
	assert.Equal(t, "", parsed["output"])
	assert.NotNil(t, parsed["estimate"])

	estimates := parsed["estimate"].([]interface{})
	assert.Len(t, estimates, 1)

	first := estimates[0].(map[string]interface{})
	assert.Equal(t, "default", first["database"])
	assert.Equal(t, "events", first["table"])
	assert.Equal(t, float64(100), first["parts"])
	assert.Equal(t, float64(1000000), first["rows"])
	assert.Equal(t, float64(5000), first["marks"])
}
