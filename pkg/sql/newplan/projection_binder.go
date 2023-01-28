package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// BindExpr ->
// 1.If astExpr in GroupByAst -> (groupTag,colPos)
// 2.If astExpr in AggregateByAst -> (aggregateTag,colPos)
//
// BindColRef -> baseBindColRef
// BindAggFunc -> havingBinder.BindAggFunc
// Except Win,
// Subquery -> baseBindSubquery
func NewProjectionBinder(builder *QueryBuilder, bindContext *BindContext, havingBinder *HavingBinder) *ProjectionBinder {
	projectBinder := &ProjectionBinder{havingBinder: havingBinder}
	projectBinder.builder = builder
	projectBinder.bindContext = bindContext
	projectBinder.impl = projectBinder
	return projectBinder
}

func (projectBinder *ProjectionBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	exprStr := tree.String(expr, dialect.MYSQL)

	if pos, ok := projectBinder.bindContext.groupByAst[exprStr]; ok {
		return &plan.Expr{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: projectBinder.bindContext.groupTag,
					ColPos: pos,
				},
			},
			Typ: projectBinder.bindContext.groups[pos].Typ,
		}, nil
	}

	if pos, ok := projectBinder.bindContext.aggregateByAst[exprStr]; ok {
		return &plan.Expr{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: projectBinder.bindContext.aggregateTag,
					ColPos: pos,
				},
			},
			Typ: projectBinder.bindContext.aggregates[pos].Typ,
		}, nil
	}
	return projectBinder.baseBindExpr(expr, i, b)
}

func (projectBinder *ProjectionBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	return projectBinder.baseBindColRef(name, i, b)
}

func (projectBinder *ProjectionBinder) BindAggFunc(funcName string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return projectBinder.havingBinder.BindAggFunc(funcName, expr, i, b)
}

func (projectBinder *ProjectionBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI(projectBinder.GetContext(), "window functions")
}

func (projectBinder *ProjectionBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	return projectBinder.baseBindSubquery(subquery, b)
}
