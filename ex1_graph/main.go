package main

import (
	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"
	"log"
)

func main() {
	g := simple.NewDirectedGraph()
	n1 := simple.Node(1)
	n2 := simple.Node(2)
	n3 := simple.Node(3)
	n4 := simple.Node(4)
	n5 := simple.Node(5)
	g.SetEdge(g.NewEdge(n1, n2))
	g.SetEdge(g.NewEdge(n1, n3))
	g.SetEdge(g.NewEdge(n1, n4))
	g.SetEdge(g.NewEdge(n1, n5))
	g.SetEdge(g.NewEdge(n3, n2))
	g.SetEdge(g.NewEdge(n3, n5))
	g.SetEdge(g.NewEdge(n4, n5))
	g.SetEdge(g.NewEdge(n5, n2))

	nodes, _ := topo.SortStabilized(g, nil)

	// reverse
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
	log.Printf("%v\n", nodes)
}
