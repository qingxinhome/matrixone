package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"go/constant"
)

func NewOrderByBinder(projectBinder *ProjectionBinder, selectList tree.SelectExprs) *OrderByBinder {
	return &OrderByBinder{
		ProjectionBinder: projectBinder,
		selectList:       selectList,
	}
}

// BindExpr -> Virtual ColRef (projectTag, projects[colPos])
// UnresolvedName[0] alias, NumVal(index of bound project list)
func (orderBinder *OrderByBinder) BindExpr(astExpr tree.Expr) (*plan.Expr, error) {
	if column, ok := astExpr.(*tree.UnresolvedName); ok && column.NumParts == 1 {
		if colPos, ok1 := orderBinder.bindContext.aliasMap[column.Parts[0]]; ok1 {
			return &plan.Expr{
				Typ: orderBinder.bindContext.projects[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: orderBinder.bindContext.projectTag,
						ColPos: colPos,
					},
				},
			}, nil
		}
	}

	if numVal, ok := astExpr.(*tree.NumVal); ok {
		switch numVal.Value.Kind() {
		case constant.Int:
			colPos, _ := constant.Int64Val(numVal.Value)
			if numVal.Negative() {
				colPos = -colPos
			}
			if colPos < 1 || int(colPos) > len(orderBinder.bindContext.projects) {
				return nil, moerr.NewSyntaxError(orderBinder.GetContext(), "Order by column position %v is not in select list", colPos)
			}

			colPos = colPos - 1
			return &plan.Expr{
				Typ: orderBinder.bindContext.projects[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: orderBinder.bindContext.projectTag,
						ColPos: int32(colPos),
					},
				},
			}, nil
		default:
			return nil, moerr.NewSyntaxError(orderBinder.GetContext(), "non-integer constant int order by")
		}
	}

	// TODO next .....
	// expand alias
	// order by may use the alias in project list first
	astExpr, err := orderBinder.bindContext.qualifyColumnNames(astExpr, orderBinder.selectList, true)
	if err != nil {
		return nil, err
	}
	expr, err := orderBinder.ProjectionBinder.BindExpr(astExpr, 0, true)
	if err != nil {
		return nil, err
	}

	var colPos int32
	var ok bool
	exprStr := expr.String()
	if colPos, ok = orderBinder.bindContext.projectByExpr[exprStr]; !ok {
		if orderBinder.bindContext.isDistinct {
			return nil, moerr.NewSyntaxError(orderBinder.GetContext(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
		}
		colPos = int32(len(orderBinder.bindContext.projects))
		orderBinder.bindContext.projectByExpr[exprStr] = colPos
		orderBinder.bindContext.projects = append(orderBinder.bindContext.projects, expr)
	}

	return &plan.Expr{
		Typ: orderBinder.bindContext.projects[colPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: orderBinder.bindContext.projectTag,
				ColPos: colPos,
			},
		},
	}, err
}
