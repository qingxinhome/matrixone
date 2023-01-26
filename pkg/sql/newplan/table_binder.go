package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (t TableBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (t TableBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (t TableBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (t TableBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (t TableBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}
