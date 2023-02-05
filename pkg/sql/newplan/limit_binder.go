package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// BindExpr -> baseBindExpr
func NewLimitBinder(builder *QueryBuilder, bindContext *BindContext) *LimitBinder {
	limitBinder := &LimitBinder{}
	limitBinder.builder = builder
	limitBinder.bindContext = bindContext
	limitBinder.impl = limitBinder
	return limitBinder
}

func (l *LimitBinder) BindExpr(astExpr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	switch astExpr.(type) {
	case *tree.VarExpr, *tree.UnqualifiedStar:
		return nil, moerr.NewSyntaxError(l.GetContext(), "unsupported expr in limit clause")
	}

	expr, err := l.baseBindExpr(astExpr, i, b)
	if err != nil {
		return nil, err
	}
	if expr.Typ.Id != int32(types.T_int64) {
		if expr.Typ.Id == int32(types.T_varchar) {
			targetType := types.T_int64.ToType()
			plan2TargetType := makePlan2Type(&targetType)
			expr, err = appendCastBeforeExpr(l.GetContext(), expr, plan2TargetType)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, moerr.NewSyntaxError(l.GetContext(), "limit clause parameter must be zero or a positive integer")
		}
	}
	return expr, nil
}

func (l *LimitBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(l.GetContext(), "column not allowed in limit clause")
}

func (l *LimitBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(l.GetContext(), "aggregate function not allowed in limit clause")
}

func (l *LimitBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(l.GetContext(), "window function not allowed in limit clause")
}

func (l *LimitBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(l.GetContext(), "subquery not allowed in limit clause")
}
