package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewWhereBinder(builder *QueryBuilder, bindContext *BindContext) *WhereBinder {
	whereBinder := &WhereBinder{}

	whereBinder.builder = builder
	whereBinder.bindContext = bindContext
	// 这里有可能出现引用无限递归的情况
	whereBinder.impl = whereBinder

	return whereBinder
}

// WhereBinder.BindExpr翻译整个where表达式
func (w WhereBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	return w.baseBindExpr(expr, i, b)
}

func (w WhereBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	return w.baseBindColRef(name, i, b)
}

func (w WhereBinder) BindAggFunc(funcName string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(w.GetContext(), "aggregate function %s not allowed in WHERE clause", funcName)
}

func (w WhereBinder) BindWinFunc(funcName string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(w.GetContext(), "window function %s not allowed in WHERE clause", funcName)
}

func (w WhereBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	return w.baseBindSubquery(subquery, b)
}
