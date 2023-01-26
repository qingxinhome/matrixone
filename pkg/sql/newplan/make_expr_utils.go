package newplan

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func makePlan2Int64ConstExpr(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_C{
			C: &plan.Const{
				Isnull: false,
				Value: &plan.Const_I64Val{
					I64Val: v,
				},
			},
		},
		Typ: &plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
			Size:        8,
		},
	}
}

func makePlan2StringConstExpr(v string, isBinary ...bool) *plan.Expr {
	c := &plan.Expr_C{
		C: &plan.Const{
			Isnull: false,
			Value: &plan.Const_Sval{
				Sval: v,
			},
		},
	}

	if len(isBinary) > 0 {
		c.C.IsBin = isBinary[0]
	}

	return &plan.Expr{
		Expr: c,
		Typ: &plan.Type{
			Id:          int32(types.T_varchar),
			NotNullable: true,
			Size:        4,
			Width:       int32(len(v)),
		},
	}
}

func appendCastBeforeExpr(ctx context.Context, srcExpr *plan.Expr, destTyp *plan.Type, isBinary ...bool) (*plan.Expr, error) {
	if srcExpr.Typ.Id == int32((types.T_any)) {
		return srcExpr, nil
	}
	destTyp.NotNullable = srcExpr.Typ.NotNullable
	argsType := []types.Type{
		makeTypeByPlan2Expr(srcExpr),
		makeTypeByPlan2Type(destTyp),
	}
	funcId, _, _, err := function.GetFunctionByName(ctx, "cast", argsType)
	if err != nil {
		return nil, err
	}
	typ := *destTyp
	if len(isBinary) == 2 && isBinary[0] && isBinary[1] {
		typ.Id = int32(types.T_uint64)
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcId, "cast"),
				Args: []*plan.Expr{srcExpr, {
					Expr: &plan.Expr_T{
						T: &plan.TargetType{
							Typ: &typ,
						},
					}},
				},
			},
		},
		Typ: &typ,
	}, nil
}

func makeTypeByPlan2Expr(expr *plan.Expr) types.Type {
	var size int32 = 0
	oid := types.T(expr.Typ.Id)
	if oid != types.T_any && oid != types.T_interval {
		size = int32(oid.TypeLen())
	}
	return types.Type{
		Oid:       oid,
		Size:      size,
		Width:     expr.Typ.Width,
		Scale:     expr.Typ.Scale,
		Precision: expr.Typ.Precision,
	}
}

func makeTypeByPlan2Type(typ *plan.Type) types.Type {
	return types.Type{
		Oid:       types.T(typ.Id),
		Size:      typ.Size,
		Width:     typ.Width,
		Scale:     typ.Scale,
		Precision: typ.Precision,
	}
}

func makePlan2Type(typ *types.Type) *plan.Type {
	return &plan.Type{
		Id:        int32(typ.Oid),
		Width:     typ.Width,
		Precision: typ.Precision,
		Size:      typ.Size,
		Scale:     typ.Scale,
	}
}

func makePlan2NullConstExpr() *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_C{
			C: &plan.Const{
				Isnull: true,
			},
		},
		Typ: &plan.Type{
			Id:          int32(types.T_any),
			NotNullable: false,
		},
	}
}
