package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewOrderByBinder(projectBinder *ProjectionBinder, selectList tree.SelectExprs) *OrderByBinder {
	return &OrderByBinder{
		ProjectionBinder: projectBinder,
		selectList:       selectList,
	}
}

func (orderByBinder *OrderByBinder) BindExpr(astExpr tree.Expr) (*plan.Expr, error) {
	return nil, nil
}
