package newplan

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	JoinSideNode      int8 = iota
	JoinSideLeft      int  = 1 << iota
	JoinSideRight     int  = 1 << iota
	JoinSizeBoth      int  = JoinSideLeft | JoinSideRight
	JoinSideCorrelate int  = 1 << iota
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

// 应用等值式模式中分配律：
// （A ∧ B）∨（A ∧ C）⇔ A ∧（B ∨ C）
func applyDistributivity(ctx context.Context, expr *plan.Expr) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = applyDistributivity(ctx, arg)
		}
		if exprImpl.F.Func.ObjName != "or" {
			break
		}

		// 拆分合取式  B and C
		leftConditions := splitConjunction(exprImpl.F.Args[0])  // B
		rightConditions := splitConjunction(exprImpl.F.Args[1]) // C
		conditionMap := make(map[string]int)

		for _, condition := range rightConditions {
			conditionMap[condition.String()] = JoinSideRight
		}

		var commonConds []*plan.Expr
		var leftOnlyConds []*plan.Expr
		var rightOnlyConds []*plan.Expr

		for _, condition := range leftConditions {
			exprStr := condition.String()
			if conditionMap[exprStr] == JoinSideRight {
				commonConds = append(commonConds, condition)
				conditionMap[exprStr] = JoinSizeBoth
			} else {
				leftOnlyConds = append(leftOnlyConds, condition)
				conditionMap[exprStr] = JoinSideLeft
			}
		}

		for _, condition := range rightConditions {
			if conditionMap[condition.String()] == JoinSideRight {
				rightOnlyConds = append(rightOnlyConds, condition)
			}
		}
		if len(commonConds) == 0 {
			return expr
		}
		expr = combineConjunction(ctx, commonConds)
		if len(leftOnlyConds) == 0 || len(rightOnlyConds) == 0 {
			return expr
		}

		leftExpr := combineConjunction(ctx, leftOnlyConds)
		rightExpr := combineConjunction(ctx, rightOnlyConds)
		resExpr, _ := bindFuncExprImplByPlanExpr(ctx, "or", []*plan.Expr{leftExpr, rightExpr})

		expr, _ = bindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{expr, resExpr})
	}
	return expr
}

// 执行分配律中的结合操作
// （A∨B）∧（A∨C）⇔ A ∨（B∧C）
func combineConjunction(ctx context.Context, exprs []*plan.Expr) *plan.Expr {
	var expr *plan.Expr
	var err error
	for i := 0; i < len(exprs); i++ {
		args := []*plan.Expr{expr, exprs[i]}
		expr, err = bindFuncExprImplByPlanExpr(ctx, "and", args)
		if err != nil {
			break
		}
	}
	return expr
}

// 拆分合取式：
// 设p,q为两个命题,复合命题"p并且q"(或 "p与q")称为 p与q的合取式，记作 p∧q, ∧ 称为合取连接词，
// 规定p∧q为真 当且仅当p与q同时为真
func splitConjunction(expr *plan.Expr) []*plan.Expr {
	//"SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA'"
	var exprs []*plan.Expr
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "and" {
			exprs = append(exprs, splitConjunction(exprImpl.F.Args[0])...)
			exprs = append(exprs, splitConjunction(exprImpl.F.Args[1])...)
		} else {
			exprs = append(exprs, expr)
		}
	default:
		exprs = append(exprs, expr)
	}
	return exprs
}
