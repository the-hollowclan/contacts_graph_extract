package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/its-ernest/osintrace/sdk"
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

func (m *Module) Run(input sdk.Input, ctx sdk.Context) error {
	// ---- Parse config ----
	var cfg config
	rawCfg, err := json.Marshal(input.Config)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(rawCfg, &cfg); err != nil {
		return err
	}

	if cfg.Leak == "" {
		return fmt.Errorf("config.leak is required")
	}

	fmt.Fprintln(os.Stderr, "reading leak file:", cfg.Leak)

	// ---- Read CSV ----
	file, err := os.Open(cfg.Leak)
	if err != nil {
		return fmt.Errorf("failed to open leak file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read csv: %w", err)
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

		// Register owner node
		if _, ok := nodes[ownerPhone]; !ok {
			nodes[ownerPhone] = Person{
				Name:     ownerName,
				Phone:    ownerPhone,
				Location: ownerLocation,
			}
		}

		// Register contact node
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

	// ---- Build edge list ----
	var edgeList []Edge
	for k, w := range edges {
		parts := strings.Split(k, "->")
		edgeList = append(edgeList, Edge{
			OwnerPhone:   parts[0],
			ContactPhone: parts[1],
			Weight:       w,
		})
	}

	// ---- Build node list ----
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

	// ---- Write graph artifact ----
	graphPath := filepath.Join(ctx.StepDir, "graph.json")
	rawGraph, err := json.MarshalIndent(graph, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(graphPath, rawGraph, 0o644); err != nil {
		return err
	}

	fmt.Fprintln(os.Stderr, "graph written:", graphPath)
	fmt.Fprintln(os.Stderr, "nodes:", len(nodeList), "edges:", len(edgeList))

	// ---- Write output index ----
	outputIndex := map[string]any{
		"artifacts": map[string]any{
			"graph": map[string]any{
				"path": "graph.json",
				"type": "application/json",
			},
		},
	}

	indexPath := filepath.Join(ctx.StepDir, "output.json")
	rawIndex, _ := json.MarshalIndent(outputIndex, "", "  ")

	if err := os.WriteFile(indexPath, rawIndex, 0o644); err != nil {
		return err
	}

	return nil
}

// ---------------- MAIN ----------------

func main() {
	sdk.Run(&Module{})
}