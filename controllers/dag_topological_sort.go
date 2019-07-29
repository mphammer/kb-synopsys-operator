package controllers

import (
	"fmt"
	"github.com/goombaio/orderedmap"
	"github.com/goombaio/orderedset"
	ctrl "sigs.k8s.io/controller-runtime"
	"sync"
)

type Vertex struct {
	ID            string
	FuncToExecute func() (ctrl.Result, error)
	Parents       *orderedset.OrderedSet
	Children      *orderedset.OrderedSet
}

// NewVertex creates a new vertex.
func NewVertex(id string, funcToExecute func() (ctrl.Result, error)) *Vertex {
	v := &Vertex{
		ID:            id,
		Parents:       orderedset.NewOrderedSet(),
		Children:      orderedset.NewOrderedSet(),
		FuncToExecute: funcToExecute,
	}

	return v
}

// String implements stringer interface and prints an string representation
// of this instance.
//func (v *Vertex) String() string {
//	result := fmt.Sprintf("ID: %s - Parents: %d - Children: %d - Value: %v\n", v.ID, v.Parents.Size(), v.Children.Size(), v.FuncToExecute)
//
//	return result
//}

//func (v *Vertex) GetVertex(id string) *Vertex {
//
//
//	return result
//}

// Degree return the number of parents and children of the vertex
func (v *Vertex) Degree() int {
	return v.Parents.Size() + v.Children.Size()
}

// InDegree return the number of parents of the vertex or the number of edges
// entering on it.
func (v *Vertex) InDegree() int {
	return v.Parents.Size()
}

// OutDegree return the number of children of the vertex or the number of edges
// leaving it.
func (v *Vertex) OutDegree() int {
	return v.Children.Size()
}

// DAG type implements a Directed Acyclic DAG data structure.
type DAG struct {
	mu       sync.Mutex
	vertices orderedmap.OrderedMap
}

// NewDAG creates a new Directed Acyclic Graph or DAG.
func NewDAG() *DAG {
	d := &DAG{
		vertices: *orderedmap.NewOrderedMap(),
	}

	return d
}

// AddVertex adds a vertex to the graph.
func (d *DAG) AddVertex(v *Vertex) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.vertices.Put(v.ID, v)

	return nil
}

// DeleteVertex deletes a vertex and all the edges referencing it from the
// graph.
func (d *DAG) DeleteVertex(vertex *Vertex) error {
	existsVertex := false

	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if vertices exists.
	for _, v := range d.vertices.Values() {
		if v == vertex {
			existsVertex = true
		}
	}
	if !existsVertex {
		return fmt.Errorf("Vertex with ID %v not found", vertex.ID)
	}

	d.vertices.Remove(vertex.ID)

	return nil
}

// AddEdge adds a directed edge between two existing vertices to the graph.
func (d *DAG) AddEdge(tailVertex *Vertex, headVertex *Vertex) error {
	tailExists := false
	headExists := false

	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if vertices exists.
	for _, vertex := range d.vertices.Values() {
		if vertex == tailVertex {
			tailExists = true
		}
		if vertex == headVertex {
			headExists = true
		}
	}
	if !tailExists {
		return fmt.Errorf("Vertex with ID %v not found", tailVertex.ID)
	}
	if !headExists {
		return fmt.Errorf("Vertex with ID %v not found", headVertex.ID)
	}

	// Check if edge already exists.
	for _, childVertex := range tailVertex.Children.Values() {
		if childVertex == headVertex {
			return fmt.Errorf("Edge (%v,%v) already exists", tailVertex.ID, headVertex.ID)
		}
	}

	// Add edge.
	tailVertex.Children.Add(headVertex)
	headVertex.Parents.Add(tailVertex)

	return nil
}

// DeleteEdge deletes a directed edge between two existing vertices from the
// graph.
func (d *DAG) DeleteEdge(tailVertex *Vertex, headVertex *Vertex) error {
	for _, childVertex := range tailVertex.Children.Values() {
		if childVertex == headVertex {
			tailVertex.Children.Remove(childVertex)
		}
	}

	return nil
}

// GetVertex return a vertex from the graph given a vertex ID.
func (d *DAG) GetVertex(id interface{}) (*Vertex, error) {
	var vertex *Vertex

	v, found := d.vertices.Get(id)
	if !found {
		return vertex, fmt.Errorf("vertex %s not found in the graph", id)
	}

	vertex = v.(*Vertex)

	return vertex, nil
}

// Order return the number of vertices in the graph.
func (d *DAG) Order() int {
	numVertices := d.vertices.Size()

	return numVertices
}

// Size return the number of edges in the graph.
func (d *DAG) Size() int {
	numEdges := 0
	for _, vertex := range d.vertices.Values() {
		numEdges = numEdges + vertex.(*Vertex).Children.Size()
	}

	return numEdges
}

// SinkVertices return vertices with no children defined by the graph edges.
func (d *DAG) SinkVertices() []*Vertex {
	var sinkVertices []*Vertex

	for _, vertex := range d.vertices.Values() {
		if vertex.(*Vertex).Children.Size() == 0 {
			sinkVertices = append(sinkVertices, vertex.(*Vertex))
		}
	}

	return sinkVertices
}

// SourceVertices return vertices with no parent defined by the graph edges.
func (d *DAG) SourceVertices() []*Vertex {
	var sourceVertices []*Vertex

	for _, vertex := range d.vertices.Values() {
		if vertex.(*Vertex).Parents.Size() == 0 {
			sourceVertices = append(sourceVertices, vertex.(*Vertex))
		}
	}

	return sourceVertices
}

// Successors return vertices that are children of a given vertex.
func (d *DAG) Successors(vertex *Vertex) ([]*Vertex, error) {
	var successors []*Vertex

	_, found := d.GetVertex(vertex.ID)
	if found != nil {
		return successors, fmt.Errorf("vertex %s not found in the graph", vertex.ID)
	}

	for _, v := range vertex.Children.Values() {
		successors = append(successors, v.(*Vertex))
	}

	return successors, nil
}

// Predecessors return vertices that are parent of a given vertex.
func (d *DAG) Predecessors(vertex *Vertex) ([]*Vertex, error) {
	var predecessors []*Vertex

	_, found := d.GetVertex(vertex.ID)
	if found != nil {
		return predecessors, fmt.Errorf("vertex %s not found in the graph", vertex.ID)
	}

	for _, v := range vertex.Parents.Values() {
		predecessors = append(predecessors, v.(*Vertex))
	}

	return predecessors, nil
}

// String implements stringer interface.
//
// Prints an string representation of this instance.
func (d *DAG) String() string {
	result := fmt.Sprintf("DAG Vertices: %d - Edges: %d\n", d.Order(), d.Size())
	result += fmt.Sprintf("Vertices:\n")
	for _, vertex := range d.vertices.Values() {
		vertex = vertex.(*Vertex)
		result += fmt.Sprintf("%s", vertex)
	}

	return result
}

// GetLevelsFromDependencyMap returns an ordered list of sets of nodes (levels) that
// are dependent on each other (i.e. set 2 needs to be complete before set 1)
func (d *DAG) GetLevelsFromDependencyMap() [][]*Vertex {
	var levels [][]*Vertex

	// Determine the number or inbound edges for each Vertex
	numOfInboundEdgesInEachNode := map[*Vertex]int{}
	for _, node := range d.vertices.Keys() {
		vertex, _ := d.GetVertex(node)
		if numOfInboundEdgesInEachNode[vertex] == 0 {
			numOfInboundEdgesInEachNode[vertex] = 0
		}
		dependencies, _ := d.Successors(vertex)
		for _, neighbor := range dependencies {
			numOfInboundEdgesInEachNode[neighbor]++
		}
	}

	// processNext stores Nodes that currently have 0 inbound edges
	var processNext []*Vertex
	var level []*Vertex

	// Find all Nodes with no inbound edges and add them to the first level
	// Nothing is dependent on these nodes (However, they are possibly dependent on other Nodes)
	for node, numOfInboundEdges := range numOfInboundEdgesInEachNode {
		if numOfInboundEdges == 0 {
			level = append(level, node)
			processNext = append(processNext, node)
			numOfInboundEdgesInEachNode[node] = -1
		}
	}
	// add to levels and clear out for the next level
	levels = append(levels, level)
	level = []*Vertex{}

	// Add Nodes to the level based on how many inbound edges it has. Every time you process a node
	// decrement the number of inbound edges that it's neightbor has
	for len(processNext) > 0 {
		// Get the next node with 0 inbound edges
		var node *Vertex
		node = processNext[len(processNext)-1]
		processNext = processNext[:len(processNext)-1] // remove first node from processNext

		// Increment the level after all Nodes that currently have 0 inbound edges have been processed
		if len(processNext) == 0 {
			// add to levels and clear out for the next level
			levels = append(levels, level)
			level = []*Vertex{}
		}

		// Decrement the number of inbound edges from each neighbor
		dependency, _ := d.Successors(node)
		for _, neighbor := range dependency {
			numOfInboundEdgesInEachNode[neighbor]--
			// Add the neighbor to the level if it now has 0 inbound edges
			if numOfInboundEdgesInEachNode[neighbor] == 0 {
				level = append(level, neighbor)
				processNext = append(processNext, neighbor)
				numOfInboundEdgesInEachNode[neighbor] = -1
			}
		}
	}

	return levels
}
