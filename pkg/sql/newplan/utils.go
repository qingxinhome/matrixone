package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func CalcNodeStats(nodeId int32, builder *QueryBuilder, recursive bool) {
	node := builder.qry.Nodes[nodeId]
	if recursive {
		panic("unimplement")
	}

	var childStats *plan.Stats
	if len(node.Children) == 1 {
		childStats = builder.qry.Nodes[node.Children[0]].Stats
	}

	switch node.NodeType {
	case plan.Node_JOIN:
		panic("unimplement")
	case plan.Node_AGG:
		panic("unimplement")
	case plan.Node_UNIQUE:
		panic("unimplement")
	case plan.Node_UNION_ALL:
		panic("unimplement")
	case plan.Node_INTERSECT:
		panic("unimplement")
	case plan.Node_INTERSECT_ALL:
		panic("unimplement")
	case plan.Node_MINUS:
		panic("unimplement")
	case plan.Node_MINUS_ALL:
		panic("unimplement")
	case plan.Node_TABLE_SCAN:
		if node.ObjRef != nil {
			node.Stats = builder.compCtx.Stats(node.ObjRef, nil)
		}
	default:
		if len(node.Children) > 0 {
			node.Stats = &plan.Stats{
				Outcnt: childStats.Outcnt,
				Cost:   childStats.Outcnt,
			}
		} else if node.Stats == nil {
			node.Stats = &plan.Stats{
				Outcnt: 1000,
				Cost:   1000000,
			}
		}
	}
}

// checkNoNeedCast
// if constant's type higher than column's type
// and constant's value in range of column's type, then no cast was needed
func checkNoNeedCast(t types.T, t2 types.T, expr *plan.Expr_C) bool {
	return false
}
