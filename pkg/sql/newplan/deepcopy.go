package newplan

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

func DeepCopyTyp(typ *plan.Type) *plan.Type {
	return nil
}

func DeepCopyExpr(expr *plan.Expr) *plan.Expr {
	if expr == nil {
		return nil
	}
	newExpr := &plan.Expr{
		Typ: DeepCopyTyp(expr.Typ),
	}

	switch exprItem := expr.Expr.(type) {
	case *plan.Expr_C:
		pc := &plan.Const{
			Isnull: exprItem.C.GetIsnull(),
		}
		switch value := exprItem.C.Value.(type) {
		case *plan.Const_I8Val:
			pc.Value = &plan.Const_I8Val{value.I8Val}
		case *plan.Const_I16Val:
			pc.Value = &plan.Const_I16Val{value.I16Val}
		case *plan.Const_I32Val:
			pc.Value = &plan.Const_I32Val{I32Val: value.I32Val}
		case *plan.Const_I64Val:
			pc.Value = &plan.Const_I64Val{I64Val: value.I64Val}
		case *plan.Const_Dval:
			pc.Value = &plan.Const_Dval{Dval: value.Dval}
		case *plan.Const_Sval:
			pc.Value = &plan.Const_Sval{Sval: value.Sval}
		case *plan.Const_Bval:
			pc.Value = &plan.Const_Bval{Bval: value.Bval}
		case *plan.Const_U8Val:
			pc.Value = &plan.Const_U8Val{U8Val: value.U8Val}
		case *plan.Const_U16Val:
			pc.Value = &plan.Const_U16Val{U16Val: value.U16Val}
		case *plan.Const_U32Val:
			pc.Value = &plan.Const_U32Val{U32Val: value.U32Val}
		case *plan.Const_U64Val:
			pc.Value = &plan.Const_U64Val{U64Val: value.U64Val}
		case *plan.Const_Fval:
			pc.Value = &plan.Const_Fval{Fval: value.Fval}
		case *plan.Const_Dateval:
			pc.Value = &plan.Const_Dateval{Dateval: value.Dateval}
		case *plan.Const_Timeval:
			pc.Value = &plan.Const_Timeval{Timeval: value.Timeval}
		case *plan.Const_Datetimeval:
			pc.Value = &plan.Const_Datetimeval{Datetimeval: value.Datetimeval}
		case *plan.Const_Decimal64Val:
			pc.Value = &plan.Const_Decimal64Val{
				Decimal64Val: &plan.Decimal64{
					A: value.Decimal64Val.A,
				},
			}
		case *plan.Const_Decimal128Val:
			pc.Value = &plan.Const_Decimal128Val{
				Decimal128Val: &plan.Decimal128{
					A: value.Decimal128Val.A,
					B: value.Decimal128Val.B,
				},
			}
		case *plan.Const_Timestampval:
			pc.Value = &plan.Const_Timestampval{
				Timestampval: value.Timestampval,
			}
		case *plan.Const_Defaultval:
			pc.Value = &plan.Const_Defaultval{
				Defaultval: value.Defaultval,
			}
		case *plan.Const_UpdateVal:
			pc.Value = &plan.Const_UpdateVal{
				UpdateVal: value.UpdateVal,
			}
		}
		newExpr.Expr = &plan.Expr_C{
			C: pc,
		}
	case *plan.Expr_P:
		newExpr.Expr = &plan.Expr_P{
			P: &plan.ParamRef{
				Pos: exprItem.P.Pos,
			},
		}
	case *plan.Expr_V:
		newExpr.Expr = &plan.Expr_V{
			V: &plan.VarRef{
				Name:   exprItem.V.Name,
				System: exprItem.V.System,
				Global: exprItem.V.Global,
			},
		}
	case *plan.Expr_Col:
		newExpr.Expr = &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: exprItem.Col.RelPos,
				ColPos: exprItem.Col.ColPos,
				Name:   exprItem.Col.GetName(),
			},
		}
	case *plan.Expr_F:
		newArgs := make([]*plan.Expr, len(exprItem.F.Args))
		for i, arg := range exprItem.F.Args {
			newArgs[i] = DeepCopyExpr(arg)
		}
		newExpr.Expr = &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Server:     exprItem.F.Func.GetServer(),
					Db:         exprItem.F.Func.GetDb(),
					Schema:     exprItem.F.Func.GetSchema(),
					Obj:        exprItem.F.Func.GetObj(),
					ServerName: exprItem.F.Func.GetServerName(),
					DbName:     exprItem.F.Func.GetDbName(),
					SchemaName: exprItem.F.Func.GetSchemaName(),
					ObjName:    exprItem.F.Func.GetObjName(),
				},
				Args: newArgs,
			},
		}
	case *plan.Expr_Sub:
		newExpr.Expr = &plan.Expr_Sub{
			Sub: &plan.SubqueryRef{
				NodeId: exprItem.Sub.GetNodeId(),
				Op:     exprItem.Sub.GetOp(),
				Child:  exprItem.Sub.GetChild(),
			},
		}
	case *plan.Expr_Corr:
		newExpr.Expr = &plan.Expr_Corr{
			Corr: &plan.CorrColRef{
				ColPos: exprItem.Corr.GetColPos(),
				RelPos: exprItem.Corr.GetRelPos(),
				Depth:  exprItem.Corr.GetDepth(),
			},
		}
	case *plan.Expr_T:
		newExpr.Expr = &plan.Expr_T{
			T: &plan.TargetType{
				Typ: DeepCopyTyp(exprItem.T.Typ),
			},
		}
	case *plan.Expr_Max:
		newExpr.Expr = &plan.Expr_Max{
			Max: &plan.MaxValue{
				Value: exprItem.Max.GetValue(),
			},
		}
	case *plan.Expr_List:
		e := &plan.ExprList{
			List: make([]*plan.Expr, len(exprItem.List.List)),
		}
		for i, e2 := range exprItem.List.List {
			e.List[i] = DeepCopyExpr(e2)
		}
		newExpr.Expr = &plan.Expr_List{
			List: e,
		}
	}
	return newExpr
}
