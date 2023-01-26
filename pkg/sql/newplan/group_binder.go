package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewGroupBinder(builder *QueryBuilder, bindContext *BindContext) *GroupBinder {
	groupBinder := &GroupBinder{}
	groupBinder.builder = builder
	groupBinder.bindContext = bindContext
	groupBinder.impl = groupBinder
	return groupBinder
}

func (groupBinder GroupBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	planExpr, err := groupBinder.baseBindExpr(expr, i, b)
	if err != nil {
		return nil, err
	}

	if b {
		exprStr := tree.String(expr, dialect.MYSQL)
		if _, ok := groupBinder.bindContext.groupByAst[exprStr]; ok {
			return nil, nil
		}
		groupBinder.bindContext.groupByAst[exprStr] = int32(len(groupBinder.bindContext.groups))
		groupBinder.bindContext.groups = append(groupBinder.bindContext.groups, planExpr)
	}
	return planExpr, err
}

func (groupBinder GroupBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	colExpr, err := groupBinder.baseBindColRef(name, i, b)
	if err != nil {
		return nil, err
	}

	if _, ok := colExpr.Expr.(*plan.Expr_Corr); ok {
		return nil, moerr.NewNYI(groupBinder.GetContext(), "correlated columns in GROUP BY clause")
	}
	return colExpr, nil
}

func (groupBinder GroupBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput(groupBinder.GetContext(), "GROUP BY clause cannot contain aggregate functions")
}

func (groupBinder GroupBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput(groupBinder.GetContext(), "GROUP BY clause cannot contain window functions")
}

func (groupBinder GroupBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI(groupBinder.GetContext(), "subquery in GROUP BY clause")
}
