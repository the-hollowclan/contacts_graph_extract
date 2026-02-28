package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/its-ernest/opentrace/sdk"
)

type Module struct{}

// ---------------- CONFIG ----------------

type config struct {
	Leak string `json:"leak"`
}

// ---------------- GRAPH TYPES ----------------

type Person struct {
	Name     string `json:"name"`
	Phone    string `json:"phone"`
	Location string `json:"location,omitempty"`
}

type Edge struct {
	OwnerPhone   string `json:"owner_phone"`
	ContactPhone string `json:"contact_phone"`
	Weight       int    `json:"weight"`
}

type Graph struct {
	Nodes []Person               `json:"nodes"`
	Edges []Edge                 `json:"edges"`
	Meta  map[string]interface{} `json:"meta"`
}

// ---------------- MODULE ----------------

func (m *Module) Name() string {
	return "contacts_graph_extract"
}

func (m *Module) Run(input sdk.Input) (sdk.Output, error) {
	// Parse config
	var cfg config
	rawCfg, _ := json.Marshal(input.Config)
	if err := json.Unmarshal(rawCfg, &cfg); err != nil {
		return sdk.Output{}, err
	}

	if cfg.Leak == "" {
		return sdk.Output{}, fmt.Errorf("leak path is required")
	}

	file, err := os.Open(cfg.Leak)
	if err != nil {
		return sdk.Output{}, fmt.Errorf("failed to open leak file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return sdk.Output{}, fmt.Errorf("failed to read csv: %w", err)
	}

	nodes := make(map[string]Person)
	edges := make(map[string]int)

	// Expected CSV schema:
	// owner_name,owner_phone,contact_name,contact_phone,owner_location

	for i, row := range rows {
		if i == 0 || len(row) < 5 {
			continue
		}

		ownerName := strings.TrimSpace(row[0])
		ownerPhone := strings.TrimSpace(row[1])
		contactName := strings.TrimSpace(row[2])
		contactPhone := strings.TrimSpace(row[3])
		ownerLocation := strings.TrimSpace(row[4])

		if ownerPhone == "" || contactPhone == "" {
			continue
		}

		// Register nodes
		if _, ok := nodes[ownerPhone]; !ok {
			nodes[ownerPhone] = Person{
				Name:     ownerName,
				Phone:    ownerPhone,
				Location: ownerLocation,
			}
		}

		if _, ok := nodes[contactPhone]; !ok {
			nodes[contactPhone] = Person{
				Name:  contactName,
				Phone: contactPhone,
			}
		}

		// Register edge
		key := ownerPhone + "->" + contactPhone
		edges[key]++
	}

	// Build edge list
	var edgeList []Edge
	for k, w := range edges {
		parts := strings.Split(k, "->")
		edgeList = append(edgeList, Edge{
			OwnerPhone:   parts[0],
			ContactPhone: parts[1],
			Weight:       w,
		})
	}

	// Build node list
	var nodeList []Person
	for _, p := range nodes {
		nodeList = append(nodeList, p)
	}

	graph := Graph{
		Nodes: nodeList,
		Edges: edgeList,
		Meta: map[string]interface{}{
			"source": cfg.Leak,
			"rows":   len(rows) - 1,
			"type":   "contact_graph",
		},
	}

	raw, _ := json.Marshal(graph)
	return sdk.Output{Result: string(raw)}, nil
}

// ---------------- MAIN ----------------

func main() {
	sdk.Run(&Module{})
}