package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// BindExpr ->
// 1.If astExpr in GroupByAst -> (groupByTag,colPos) 此时的ast表达式只能是列表达式
// 2.If astExpr in AggregateByAst -> (aggregateTag,colPos)
//
// BindColRef -> Only inside Agg Func, Do base.colRef
// BindAggFunc -> need update aggregateByAst,aggregates
// Except Win,Subquery
func NewHavingBinder(builder *QueryBuilder, bindConext *BindContext) *HavingBinder {
	havingBinder := &HavingBinder{insideAgg: false}
	havingBinder.builder = builder
	havingBinder.bindContext = bindConext
	havingBinder.impl = havingBinder
	return havingBinder
}

// If astExpr in GroupByAst -> (groupTag,colPos) , 此时的ast表达式只能是列表达式
// If astExpr in AggregateByAst -> (aggregateTag,colPos)
// Virtual ColRef (groupTag,groups[colPos]),(aggregateTag,aggregates[colPos])
func (havingBinder HavingBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	exprStr := tree.String(expr, dialect.MYSQL)

	if !havingBinder.insideAgg {
		// RelPos has been changed to groupTag
		if colPos, ok := havingBinder.bindContext.groupByAst[exprStr]; ok {
			return &plan.Expr{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: havingBinder.bindContext.groupTag,
						ColPos: colPos,
					},
				},
				Typ: havingBinder.bindContext.groups[colPos].Typ,
			}, nil
		}
	}

	if colPos, ok := havingBinder.bindContext.aggregateByAst[exprStr]; ok {
		if !havingBinder.insideAgg {
			//RelPos has been changed to aggregateTag
			return &plan.Expr{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: havingBinder.bindContext.aggregateTag,
						ColPos: colPos,
					},
				},
				Typ: havingBinder.bindContext.aggregates[colPos].Typ,
			}, nil
		} else {
			return nil, moerr.NewInvalidInput(havingBinder.GetContext(), "nestted aggregate function")
		}
	}
	return havingBinder.baseBindExpr(expr, i, b)
}

func (havingBinder HavingBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (havingBinder HavingBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (havingBinder HavingBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (havingBinder HavingBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}
