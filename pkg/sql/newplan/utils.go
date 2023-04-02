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

func increaseRefCnt(expr *plan.Expr, colRefCnt map[[2]int32]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colRefCnt[[2]int32{exprImpl.Col.RelPos, exprImpl.Col.ColPos}]++
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			increaseRefCnt(arg, colRefCnt)
		}
	}
}

func decreaseRefCnt(expr *plan.Expr, colRefCnt map[[2]int32]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colRefCnt[[2]int32{exprImpl.Col.RelPos, exprImpl.Col.ColPos}]--
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			decreaseRefCnt(arg, colRefCnt)
		}
	}
}

// 这段代码的作用是替换一个表达式中的列引用。代码中的输入包括一个表达式 expr，一个整数 tag，
// 和一个表达式切片 projects。代码遍历 expr 中的每个节点，如果遇到列引用节点且该列引用的 RelPos 属性等于输入的 tag，
// 则将该节点替换为 projects 中对应位置的表达式。最后返回替换后的表达式。
func replaceColRefs(expr *plan.Expr, tag int32, projects []*plan.Expr) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = replaceColRefs(arg, tag, projects)
		}
	case *plan.Expr_Col:
		colRef := exprImpl.Col
		if colRef.RelPos == tag {
			expr = DeepCopyExpr(projects[colRef.ColPos])
		}
	}
	return expr
}

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
		if len(node.GroupBy) > 0 {
			node.Stats = &plan.Stats{
				Outcnt:      childStats.Outcnt * 0.1,
				Cost:        childStats.Cost,
				HashmapSize: childStats.HashmapSize,
			}
		} else {
			node.Stats = &plan.Stats{
				Outcnt: 1,
				Cost:   childStats.Cost,
			}
		}
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

func containsTag(expr *plan.Expr, tag int32) bool {
	var ret bool
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			ret = ret || containsTag(arg, tag)
		}
	case *plan.Expr_Col:
		return exprImpl.Col.RelPos == tag
	}
	return ret
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

/*
这是一个用于合并两个字符串切片的函数。函数的输入是两个字符串切片 left 和 right，函数返回一个合并后的字符串切片。
函数的具体实现是，首先判断两个切片中是否有任何一个为空，如果有一个为空，则直接返回另一个切片。否则，将两个切片中的所有元素都存入一个 map[string]bool 类型的映射中，
以去重并统计元素个数。最后，遍历这个映射，将所有的键（即字符串元素）存入一个新的字符串切片 ret 中，并返回 ret。
*/
func unionSlice(left, right []string) []string {
	if len(left) < 1 {
		return right
	}
	if len(right) < 1 {
		return left
	}
	m := make(map[string]bool, len(left)+len(right))
	for _, s := range left {
		m[s] = true
	}
	for _, s := range right {
		m[s] = true
	}
	ret := make([]string, 0)
	for key, _ := range m {
		ret = append(ret, key)
	}
	return ret
}

// 下面的代码是一个取两个字符串切片的交集的函数。如果两个切片中有任何一个为空，函数将直接返回一个空切片。
// 否则，函数将使用一个 map 来记录左切片中的元素，并遍历右切片，如果右切片中的元素也在 map 中出现，则将该元素添加到返回的切片中
func intersectSlice(left, right []string) []string {
	if len(left) < 1 || len(right) < 1 {
		return left
	}
	m := make(map[string]bool, len(left)+len(right))
	for _, s := range left {
		m[s] = true
	}
	ret := make([]string, 0)
	for _, s := range right {
		if _, ok := m[s]; ok {
			ret = append(ret, s)
		}
	}
	return ret
}

/*
DNF表示析取范式，例如(a and b) or (c and d) or (e and f)
如果我们有一个DNF滤波器，例如(c1=1 and c2=1) or (c1=2 and c2=2)
我们可以有额外的过滤器：(c1=1 or c1=2) and (c2=1 or c2=2)，可以向下推以优化JOIN
checkDNF扫描expr并返回cond的所有组
例如（ (c1=1 and c2=1) or (c1=2 and c3=2),，c1是一个组，因为它出现在所有析取词中
并且c2、c3不是一个组
函数，接受一个 Expr 结构体的指针作为输入，返回一个字符串切片。该函数的目的是检查给定的表达式是否为析取范式（DNF）。
该函数使用递归方法遍历表达式树。它首先检查给定的表达式是否为函数 (Expr_F)，如果该函数是一个“或”操作，则递归检查“或”函数的左右参数，并返回结果的交集。
如果该函数不是一个“或”操作，则递归检查每个参数，并返回结果的并集。
如果表达式不是一个函数，则函数检查它是否为关联 (Expr_Corr) 或列 (Expr_Col)，并将其字符串表示附加到输出切片中。最后，函数返回输出切片。
需要注意的是，该函数不修改输入表达式，它只检查其结构并根据结构返回结果。
walkThroughDNF接受关键字字符串，遍历expr，并提取包含关键字的所有cond
*/
func checkDNF(expr *plan.Expr) []string {
	var ret []string
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "or" {
			left := checkDNF(exprImpl.F.Args[0])
			right := checkDNF(exprImpl.F.Args[1])
			return intersectSlice(left, right)
		}
		for _, arg := range exprImpl.F.Args {
			ret = unionSlice(ret, checkDNF(arg))
		}
		return ret
	case *plan.Expr_Corr: // 针对关联子查询的关联列
		ret = append(ret, exprImpl.Corr.String())
	case *plan.Expr_Col:
		ret = append(ret, exprImpl.Col.String())
	}
	return ret
}

// 这个函数的功能是遍历一个,
func walkThroughDNF(ctx context.Context, expr *plan.Expr, keywords string) *plan.Expr {
	var retExpr *plan.Expr
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "or" {
			left := walkThroughDNF(ctx, exprImpl.F.Args[0], keywords)
			right := walkThroughDNF(ctx, exprImpl.F.Args[1], keywords)
			if left != nil && right != nil {
				retExpr, _ = bindFuncExprImplByPlanExpr(ctx, "or", []*plan.Expr{left, right})
				return retExpr
			}
		} else if exprImpl.F.Func.ObjName == "and" {
			left := walkThroughDNF(ctx, exprImpl.F.Args[0], keywords)
			right := walkThroughDNF(ctx, exprImpl.F.Args[1], keywords)
			if left == nil {
				return right
			} else if right == nil {
				return left
			} else {
				retExpr, _ = bindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{left, right})
				return retExpr
			}
		} else {
			for _, arg := range exprImpl.F.Args {
				if walkThroughDNF(ctx, arg, keywords) == nil {
					return nil
				}
			}
			return expr
		}
	case *plan.Expr_Corr:
		if exprImpl.Corr.String() == keywords {
			return expr
		} else {
			return nil
		}
	case *plan.Expr_Col:
		if exprImpl.Col.String() == keywords {
			return expr
		} else {
			return nil
		}
	}
	return expr
}
