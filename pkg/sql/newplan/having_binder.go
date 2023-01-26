package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (h HavingBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (h HavingBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (h HavingBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (h HavingBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (h HavingBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}
