package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func isNullExpr(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch exprimpl := expr.Expr.(type) {
	case *plan.Expr_C:
		if expr.Typ.Id == int32(types.T_any) && exprimpl.C.Isnull {
			return true
		}
	}
	return false
}

func getFunctionObjRef(funcId int64, name string) *plan.ObjectRef {
	return &plan.ObjectRef{
		Obj:     funcId,
		ObjName: name,
	}
}

func convertValueIntoBool(name string, args []*plan.Expr, isLogic bool) error {
	if !isLogic {
		if len(args) != 2 ||
			(args[0].Typ.Id != int32(types.T_bool) && args[1].Typ.Id != int32(types.T_bool)) {
			return nil
		}
	}

	for _, arg := range args {
		if arg.Typ.Id == int32(types.T_bool) {
			continue
		} else {
			switch expr := arg.Expr.(type) {
			case *plan.Expr_C:
				switch value := expr.C.Value.(type) {
				case *plan.Const_I64Val:
					if value.I64Val == 0 {
						expr.C.Value = &plan.Const_Bval{
							Bval: false,
						}
					} else {
						expr.C.Value = &plan.Const_Bval{
							Bval: true,
						}
					}
					arg.Typ.Id = int32(types.T_bool)
				}
			}
		}
	}
	return nil
}
