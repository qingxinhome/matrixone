package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (g GroupBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (g GroupBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (g GroupBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (g GroupBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (g GroupBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}
