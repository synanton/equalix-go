package domain

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// HierarchyConfig configures the fairness tree (Java
// HierarchicalProperties, EQX-7). Zero value = flat-equivalent
// (disabled); Validate enforces the enabled-state requirements.
type HierarchyConfig struct {
	Enabled      bool
	Separator    string
	Layers       []HierarchyLayer
	Weights      map[string]float64
	MetricsDepth int
}

// HierarchyLayer is one tree layer from the root down.
type HierarchyLayer struct {
	Name          string
	DefaultWeight float64
}

func (c HierarchyConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Separator == "" {
		return fmt.Errorf("domain: hierarchy separator must not be blank in hierarchical mode")
	}
	if len(c.Layers) == 0 {
		return fmt.Errorf("domain: hierarchy layers must not be empty in hierarchical mode")
	}
	for i, l := range c.Layers {
		if l.Name == "" {
			return fmt.Errorf("domain: hierarchy layer %d needs a name", i)
		}
		if l.DefaultWeight <= 0 {
			return fmt.Errorf("domain: hierarchy layer %q needs a positive default weight", l.Name)
		}
	}
	for path, w := range c.Weights {
		if w <= 0 {
			return fmt.Errorf("domain: hierarchy weight override %q must be positive", path)
		}
	}
	if c.MetricsDepth < 0 {
		return fmt.Errorf("domain: hierarchy metrics depth must be >= 0")
	}
	return nil
}

// HierarchyRoot is the root node key; also the CMS key counting all
// in-flight tasks in hierarchical mode. FlatLayer labels metrics in
// flat mode.
const (
	HierarchyRoot = ""
	FlatLayer     = "key"
	rootLayer     = "root"
)

// HierarchyNode is one node on a fairness key's path (Java
// HierarchyNode, EQX-7).
type HierarchyNode struct {
	Key       string
	ParentKey string
	Layer     int
	LayerName string
	Leaf      bool
}

// HierarchyNodeState is one node's persisted scheduling state (Java
// HierarchyNodeState): service received in the parent's virtual time
// (CFS vruntime) plus the floor for the node's children (CFS
// min_vruntime analog — idle children restart here, never banking
// credit).
type HierarchyNodeState struct {
	Key                 string
	VirtualTime         float64
	ChildrenVirtualTime float64
	NaN                 bool // true when no stored state exists (fresh node)
}

// QueuedLeaf is one fairness key's dispatchable backlog (Java
// QueuedLeaf).
type QueuedLeaf struct {
	FairnessKey string
	Queued      int
	Promoted    int
	MaxWeight   float64
	InFlight    int
}

// FairnessHierarchy maps fairness keys onto the configured tenant
// tree (Java FairnessHierarchy, EQX-7). Pure: no I/O.
type FairnessHierarchy struct {
	cfg      HierarchyConfig
	splitter func(string) []string
}

// NewFairnessHierarchy builds the mapper, validating config.
func NewFairnessHierarchy(cfg HierarchyConfig) (*FairnessHierarchy, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	sep := cfg.Separator
	return &FairnessHierarchy{cfg: cfg, splitter: func(s string) []string {
		return strings.Split(s, sep)
	}}, nil
}

func (h *FairnessHierarchy) Enabled() bool { return h.cfg.Enabled }

// Path returns nodes from the root's child down to the leaf; a single
// leaf node in flat mode (downstream code needs no mode branch).
func (h *FairnessHierarchy) Path(fairnessKey string) []HierarchyNode {
	if !h.cfg.Enabled {
		return []HierarchyNode{{Key: fairnessKey, ParentKey: HierarchyRoot, Layer: 0, LayerName: FlatLayer, Leaf: true}}
	}
	segments := h.splitter(fairnessKey)
	depth := len(segments)
	if depth > len(h.cfg.Layers) {
		depth = len(h.cfg.Layers)
	}
	path := make([]HierarchyNode, 0, depth)
	parent := HierarchyRoot
	var prefix strings.Builder
	for layer := 0; layer < depth-1; layer++ {
		prefix.WriteString(segments[layer])
		prefix.WriteString(h.cfg.Separator)
		key := prefix.String()
		path = append(path, HierarchyNode{Key: key, ParentKey: parent, Layer: layer, LayerName: h.cfg.Layers[layer].Name})
		parent = key
	}
	path = append(path, HierarchyNode{Key: fairnessKey, ParentKey: parent, Layer: depth - 1, LayerName: h.cfg.Layers[depth-1].Name, Leaf: true})
	return path
}

// InternalNodeKeys returns the internal nodes above a fairness key,
// from the top; empty in flat mode.
func (h *FairnessHierarchy) InternalNodeKeys(fairnessKey string) []string {
	path := h.Path(fairnessKey)
	out := make([]string, 0, len(path)-1)
	for _, n := range path[:len(path)-1] {
		out = append(out, n.Key)
	}
	return out
}

// Weight returns the override for the node's path, else the task
// weight for leaves or the layer default. Internal nodes look up the
// path WITHOUT the trailing separator.
func (h *FairnessHierarchy) Weight(node HierarchyNode, leafTaskWeight float64) float64 {
	path := node.Key
	if !node.Leaf {
		path = strings.TrimSuffix(path, h.cfg.Separator)
	}
	if w, ok := h.cfg.Weights[path]; ok {
		return w
	}
	if node.Leaf {
		return leafTaskWeight
	}
	return h.cfg.Layers[node.Layer].DefaultWeight
}

// WithAncestors adds every internal node's and the root's counts to
// per-fairness-key counts; identity in flat mode.
func (h *FairnessHierarchy) WithAncestors(counts map[string]int64) map[string]int64 {
	if !h.cfg.Enabled {
		return counts
	}
	expanded := map[string]int64{}
	for key, count := range counts {
		expanded[key] += count
		for _, node := range h.InternalNodeKeys(key) {
			expanded[node] += count
		}
		expanded[HierarchyRoot] += count
	}
	return expanded
}

// ValidationError explains why a fairness key cannot be placed in the
// tree, or "" when it can (always "" in flat mode).
func (h *FairnessHierarchy) ValidationError(fairnessKey string) string {
	if !h.cfg.Enabled {
		return ""
	}
	for _, seg := range h.splitter(fairnessKey) {
		if seg == "" {
			return "fairnessKey must not start or end with '" + h.cfg.Separator + "' or contain empty segments"
		}
	}
	return ""
}

// SelectionPlan is one hierarchical selection tick (Java
// HierarchicalSelector.Plan).
type SelectionPlan struct {
	PickOrder      []string
	TasksPerLeaf   map[string]int
	NodeWeights    map[string]float64
	NodeFloors     map[string]float64
	ChildrenFloors map[string]float64
}

type planNode struct {
	key          string
	weight       float64
	leaf         bool
	children     []*planNode
	parent       *planNode
	virtualTime  float64
	pressure     float64
	remaining    int
	promoted     int
	activeLeaves int
}

func (n *planNode) isBacklogged() bool { return n.activeLeaves > 0 }

func (n *planNode) bestChild(quantum float64) *planNode {
	var best *planNode
	bestKey := math.Inf(1)
	for _, child := range n.children {
		if !child.isBacklogged() {
			continue
		}
		key := child.virtualTime + quantum/child.weight + child.pressure
		if best == nil || key < bestKey ||
			(key == bestKey && child.key < best.key) {
			best, bestKey = child, key
		}
	}
	return best
}

// Plan selects up to freeSlots dispatchable tasks (Java
// HierarchicalSelector.plan, EQX-7): promoted leaves first (sorted
// by key), then root-to-leaf descent picking the backlogged child
// with the least vt + quantum/w + pressure each slot. Missing node
// states are fresh (NaN → parent floor, never stale runtime).
func Plan(leaves []QueuedLeaf, h *FairnessHierarchy, states map[string]HierarchyNodeState,
	inFlight func(string) int64, penaltyFactor, quantum float64, freeSlots, maxPerClient int) SelectionPlan {
	root := &planNode{key: HierarchyRoot, weight: 1.0}
	nodes := map[string]*planNode{HierarchyRoot: root}
	var promotedLeaves []*planNode
	for _, leaf := range leaves {
		capacity := leaf.Queued
		if maxPerClient > 0 {
			if room := maxPerClient - leaf.InFlight; room < capacity {
				capacity = room
			}
		}
		if capacity <= 0 {
			continue
		}
		parent := root
		for _, pathNode := range h.Path(leaf.FairnessKey) {
			current, ok := nodes[pathNode.Key]
			if !ok {
				current = &planNode{key: pathNode.Key, weight: h.Weight(pathNode, leaf.MaxWeight), leaf: pathNode.Leaf}
				if st, ok := states[pathNode.Key]; ok && !st.NaN {
					current.virtualTime = st.VirtualTime
				} else {
					current.virtualTime = math.NaN()
				}
				current.parent = parent
				parent.children = append(parent.children, current)
				nodes[pathNode.Key] = current
			}
			parent = current
		}
		parent.remaining = capacity
		parent.promoted = leaf.Promoted
		if parent.promoted > capacity {
			parent.promoted = capacity
		}
		for node := parent; node != nil; node = node.parent {
			node.activeLeaves++
		}
		if parent.promoted > 0 {
			promotedLeaves = append(promotedLeaves, parent)
		}
	}

	nodeFloors := map[string]float64{}
	applyFloors(root, states, nodeFloors)
	for _, node := range nodes {
		if node == root {
			continue
		}
		node.pressure = penaltyFactor * float64(inFlight(node.key)) / node.weight
	}

	var pickOrder []string
	sort.Slice(promotedLeaves, func(i, j int) bool { return promotedLeaves[i].key < promotedLeaves[j].key })
	for _, leaf := range promotedLeaves {
		for leaf.promoted > 0 && len(pickOrder) < freeSlots {
			leaf.promoted--
			charge(leaf, quantum, &pickOrder)
		}
	}
	for len(pickOrder) < freeSlots && root.isBacklogged() {
		node := root
		for !node.leaf {
			next := node.bestChild(quantum)
			if next == nil {
				break
			}
			node = next
		}
		if node.leaf && node.isBacklogged() {
			charge(node, quantum, &pickOrder)
		} else {
			break
		}
	}

	// Floors are taken after this tick's charges: a child returning
	// next tick starts level with its busiest sibling, not one tick
	// of service behind it.
	childrenFloors := map[string]float64{}
	raiseChildrenFloors(root, states, childrenFloors)

	tasksPerLeaf := map[string]int{}
	for _, key := range pickOrder {
		tasksPerLeaf[key]++
	}
	weights := map[string]float64{}
	for _, node := range nodes {
		weights[node.key] = node.weight
	}
	return SelectionPlan{PickOrder: pickOrder, TasksPerLeaf: tasksPerLeaf,
		NodeWeights: weights, NodeFloors: nodeFloors, ChildrenFloors: childrenFloors}
}

func applyFloors(parent *planNode, states map[string]HierarchyNodeState, nodeFloors map[string]float64) {
	floor := storedChildrenFloor(parent, states)
	for _, child := range parent.children {
		if math.IsNaN(child.virtualTime) {
			child.virtualTime = floor
		} else if child.virtualTime < floor {
			child.virtualTime = floor
		}
		nodeFloors[child.key] = floor
		applyFloors(child, states, nodeFloors)
	}
}

func raiseChildrenFloors(parent *planNode, states map[string]HierarchyNodeState, childrenFloors map[string]float64) {
	if len(parent.children) == 0 {
		return
	}
	lowest := math.Inf(1)
	for _, child := range parent.children {
		if child.virtualTime < lowest {
			lowest = child.virtualTime
		}
		raiseChildrenFloors(child, states, childrenFloors)
	}
	if stored := storedChildrenFloor(parent, states); stored > lowest {
		lowest = stored
	}
	childrenFloors[parent.key] = lowest
}

func storedChildrenFloor(parent *planNode, states map[string]HierarchyNodeState) float64 {
	if st, ok := states[parent.key]; ok && !st.NaN {
		return st.ChildrenVirtualTime
	}
	return 0.0
}

func charge(leaf *planNode, quantum float64, pickOrder *[]string) {
	leaf.remaining--
	exhausted := leaf.remaining == 0
	for node := leaf; node != nil; node = node.parent {
		if node.parent != nil {
			node.virtualTime += quantum / node.weight
		}
		if exhausted {
			node.activeLeaves--
		}
	}
	*pickOrder = append(*pickOrder, leaf.key)
}

// Charges computes the virtual-time charge per node for dispatched
// tasks: quantum/weight for every node on each task's path (root
// excluded — same rule as the in-tick charge above).
func Charges(dispatched []DispatchedTask, plan SelectionPlan, h *FairnessHierarchy, quantum float64) map[string]float64 {
	charges := map[string]float64{}
	for _, task := range dispatched {
		for _, node := range h.Path(task.FairnessKey) {
			weight, ok := plan.NodeWeights[node.Key]
			if !ok {
				weight = h.Weight(node, task.Weight)
			}
			charges[node.Key] += quantum / weight
		}
	}
	return charges
}

// DispatchedTask is one dispatched task for charge computation.
type DispatchedTask struct {
	FairnessKey string
	Weight      float64
}
