package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (p ProjectionBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (p ProjectionBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (p ProjectionBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (p ProjectionBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (p ProjectionBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}
