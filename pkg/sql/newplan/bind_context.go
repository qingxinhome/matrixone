package newplan

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"sync/atomic"
)

var (
	bindContextCounter atomic.Uint32
)

func getBindCtxCounter() uint32 {
	return bindContextCounter.Add(1)
}

func NewBindContext(parent *BindContext) *BindContext {
	bindContext := &BindContext{
		parent:         parent,
		id:             getBindCtxCounter(),
		bindingByTag:   make(map[int32]*Binding),
		bindingByTable: make(map[string]*Binding),
		bindingByCol:   make(map[string]*Binding),
		aliasMap:       make(map[string]int32),
		groupByAst:     make(map[string]int32),
		aggregateByAst: make(map[string]int32),
	}
	fmt.Println("NewBindContext:", bindContext.id)
	if parent != nil {
		bindContext.defaultDatabase = parent.defaultDatabase
	}
	return bindContext
}

// make every column name have table prefix.
// astExpr - unqualified
// selectList - qualified select list
func (bindContext *BindContext) qualifyColumnNames(astExpr tree.Expr, selectList tree.SelectExprs, expandAlias bool) (tree.Expr, error) {
	var err error
	switch exprImpl := astExpr.(type) {
	case *tree.ParenExpr:
		astExpr, err = bindContext.qualifyColumnNames(exprImpl.Expr, selectList, expandAlias)
	case *tree.UnresolvedName:
		if exprImpl.NumParts == 1 {
			colName := exprImpl.Parts[0]
			// orderBy use the alias in project list first
			if expandAlias {
				if exprPos, ok := bindContext.aliasMap[colName]; ok {
					astExpr = selectList[exprPos].Expr
					break
				}
			}

			if binding, ok := bindContext.bindingByCol[colName]; ok {
				if binding != nil {
					exprImpl.NumParts = 2
					exprImpl.Parts[1] = binding.tableName
				} else {
					return nil, moerr.NewInvalidInput(bindContext.binder.GetContext(), "ambiguouse column reference to '%s'", colName)
				}
			}
		}
	case *tree.OrExpr:
		return nil, moerr.NewInternalError(bindContext.binder.GetContext(), "not implement qualifyColumnNames 01")
	case *tree.XorExpr:
		return nil, moerr.NewInternalError(bindContext.binder.GetContext(), "not implement qualifyColumnNames 02")
	case *tree.NotExpr:
		return nil, moerr.NewInternalError(bindContext.binder.GetContext(), "not implement qualifyColumnNames 03")
	case *tree.AndExpr:
		return nil, moerr.NewInternalError(bindContext.binder.GetContext(), "not implement qualifyColumnNames 04")
	case *tree.ComparisonExpr:
		exprImpl.Left, err = bindContext.qualifyColumnNames(exprImpl.Left, selectList, expandAlias)
		if err != nil {
			return nil, err
		}
		exprImpl.Right, err = bindContext.qualifyColumnNames(exprImpl.Right, selectList, expandAlias)
		if err != nil {
			return nil, err
		}
	case *tree.UnaryExpr:
		exprImpl.Expr, err = bindContext.qualifyColumnNames(exprImpl.Expr, selectList, expandAlias)
		if err != nil {
			return nil, err
		}
	case *tree.BinaryExpr:
		exprImpl.Left, err = bindContext.qualifyColumnNames(exprImpl.Left, selectList, expandAlias)
		if err != nil {
			return nil, err
		}
		exprImpl.Right, err = bindContext.qualifyColumnNames(exprImpl.Right, selectList, expandAlias)
		if err != nil {
			return nil, err
		}
	case *tree.RangeCond:
		return nil, moerr.NewInternalError(bindContext.binder.GetContext(), "not implement qualifyColumnNames 05")
	case *tree.FuncExpr:
		for i := range exprImpl.Exprs {
			exprImpl.Exprs[i], err = bindContext.qualifyColumnNames(exprImpl.Exprs[i], selectList, expandAlias)
			if err != nil {
				return nil, err
			}
		}
	case *tree.CaseExpr:
		return nil, moerr.NewInternalError(bindContext.binder.GetContext(), "not implement qualifyColumnNames 06")
	case *tree.CastExpr:
		return nil, moerr.NewInternalError(bindContext.binder.GetContext(), "not implement qualifyColumnNames 07")
	case *tree.IsNullExpr:
		return nil, moerr.NewInternalError(bindContext.binder.GetContext(), "not implement qualifyColumnNames 08")
	case *tree.Tuple:
		return nil, moerr.NewInternalError(bindContext.binder.GetContext(), "not implement qualifyColumnNames 09")
	}
	return astExpr, err
}
