package newplan

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"go/constant"
)

func (b *baseBinder) GetContext() context.Context {
	return b.context
}

func (b *baseBinder) baseBindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	//TODO implement me
	panic("implement me")
}

func (b *baseBinder) baseBindSubquery(astExpr *tree.Subquery, isRoot bool) (expr *plan.Expr, err error) {
	return nil, moerr.NewInternalError(b.GetContext(), "not implement")
}

func (b *baseBinder) bindFuncExpr(astExpr *tree.FuncExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	funcRef, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, moerr.NewNYI(b.GetContext(), "function expr '%v'", astExpr)
	}
	funcName := funcRef.Parts[0]
	if function.GetFunctionIsAggregateByName(funcName) {
		return b.impl.BindAggFunc(funcName, astExpr, depth, isRoot)
	} else if function.GetFunctionIsWinfunByName(funcName) {
		return b.impl.BindWinFunc(funcName, astExpr, depth, isRoot)
	}
	return b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
}

func (b *baseBinder) baseBindExpr(astExpr tree.Expr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	switch exprImpl := astExpr.(type) {
	case *tree.NumVal:
		if defaultbinder, ok := b.impl.(*DefaultBinder); ok {
			expr, err = b.bindNumVal(exprImpl, defaultbinder.typ)
		} else {
			expr, err = b.bindNumVal(exprImpl, nil)
		}
	case *tree.ParenExpr:
		expr, err = b.baseBindExpr(exprImpl.Expr, depth, isRoot)
	case *tree.OrExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.NotExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.AndExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.UnaryExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.BinaryExpr:
		expr, err = b.bindBinaryExpr(exprImpl, depth, isRoot)
	case *tree.ComparisonExpr:
		expr, err = b.bindComparisonExpr(exprImpl, depth, isRoot)
	case *tree.FuncExpr:
		expr, err = b.bindFuncExpr(exprImpl, depth, isRoot)
	case *tree.RangeCond:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.UnresolvedName:
		expr, err = b.impl.BindColRef(exprImpl, depth, isRoot)
	case *tree.CastExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.IsNullExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.IsNotNullExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.Tuple:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.CaseExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.IntervalExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.XorExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.Subquery:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.DefaultVal:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.MaxValue:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.VarExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.ParamExpr:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.StrVal:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.ExprList:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	case *tree.UnqualifiedStar:
		err = moerr.NewInternalError(b.GetContext(), "unimplement")
	default:
		moerr.NewNYI(b.GetContext(), "expr '%+v'", exprImpl)
	}
	return
}

// ------------------------------------------------------------------------------------------------------------------------
func (b *baseBinder) bindNumVal(astExpr *tree.NumVal, typ *plan.Type) (*plan.Expr, error) {
	switch astExpr.ValType {
	case tree.P_null:
		return &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: true,
				},
			},
			Typ: &plan.Type{
				Id:          int32(types.T_any),
				NotNullable: true,
			},
		}, nil
	case tree.P_bool:
		val := constant.BoolVal(astExpr.Value)
		return &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Bval{
						Bval: val,
					},
				},
			},
			Typ: &plan.Type{
				Id:          int32(types.T_bool),
				NotNullable: false,
				Size:        1,
			},
		}, nil
	case tree.P_int64:
		val, ok := constant.Int64Val(astExpr.Value)
		if !ok {
			return nil, moerr.NewInvalidInput(b.GetContext(), "invalid int value '%s'", astExpr.Value.String())
		}
		expr := makePlan2Int64ConstExpr(val)
		if typ != nil && typ.Id == int32(types.T_varchar) {
			return appendCastBeforeExpr(b.GetContext(), expr, typ)
		}
		return expr, nil
	case tree.P_uint64:
		return nil, moerr.NewInvalidInput(b.GetContext(), "unsupport value 1 '%s'", astExpr.String())
	case tree.P_float64:
		return nil, moerr.NewInvalidInput(b.GetContext(), "unsupport value 2 '%s'", astExpr.String())
	case tree.P_char:
		expr := makePlan2StringConstExpr(astExpr.String())
		return expr, nil
	case tree.P_decimal:
		return nil, moerr.NewInvalidInput(b.GetContext(), "unsupport value 3 '%s'", astExpr.String())
	case tree.P_bit:
		return nil, moerr.NewInvalidInput(b.GetContext(), "unsupport value 4 '%s'", astExpr.String())
	case tree.P_ScoreBinary:
		return nil, moerr.NewInvalidInput(b.GetContext(), "unsupport value 5 '%s'", astExpr.String())
	case tree.P_nulltext:
		return nil, moerr.NewInvalidInput(b.GetContext(), "unsupport value 6 '%s'", astExpr.String())
	default:
		return nil, moerr.NewInvalidInput(b.GetContext(), "unsupport value '%s'", astExpr.String())
	}
}

func (b *baseBinder) bindBinaryExpr(astExpr *tree.BinaryExpr, depth int32, root bool) (*plan.Expr, error) {
	switch astExpr.Op {
	case tree.PLUS:
		return b.bindFuncExprImplByAstExpr("+", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MINUS:
		return b.bindFuncExprImplByAstExpr("-", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MULTI:
		return b.bindFuncExprImplByAstExpr("*", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MOD: // %
		return b.bindFuncExprImplByAstExpr("%", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.DIV: // /
		return b.bindFuncExprImplByAstExpr("/", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.INTEGER_DIV: //
		return b.bindFuncExprImplByAstExpr("div", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_OR: // |
		return b.bindFuncExprImplByAstExpr("|", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_AND: // &
		return b.bindFuncExprImplByAstExpr("&", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_XOR: // ^
		return b.bindFuncExprImplByAstExpr("^", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.LEFT_SHIFT: // <<
		return b.bindFuncExprImplByAstExpr("<<", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.RIGHT_SHIFT: // >>
		return b.bindFuncExprImplByAstExpr(">>", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	}
	return nil, moerr.NewNYI(b.GetContext(), "'%v' operator", astExpr.Op.ToString())
}

func (b *baseBinder) bindComparisonExpr(astExpr *tree.ComparisonExpr, depth int32, root bool) (*plan.Expr, error) {
	var operator string
	switch astExpr.Op {
	case tree.EQUAL:
		operator = ""
	case tree.LESS_THAN:
		operator = "<"
	case tree.LESS_THAN_EQUAL:
		operator = "<="
	case tree.GREAT_THAN:
		operator = ">"
	case tree.GREAT_THAN_EQUAL:
		operator = ">="
	case tree.NOT_EQUAL:
		operator = "<>"
	case tree.LIKE:
		operator = "like"
	case tree.NOT_LIKE:
		return nil, moerr.NewInternalError(b.GetContext(), "not implement bindComparisonExpr 1")
	case tree.NOT_IN:
		return nil, moerr.NewInternalError(b.GetContext(), "not implement bindComparisonExpr 2")
	case tree.REG_MATCH:
		operator = "reg_match"
	case tree.NOT_REG_MATCH:
		operator = "not_reg_match"
	default:
		return nil, moerr.NewNYI(b.GetContext(), "'%v'", astExpr)
	}
	if astExpr.SubOp >= tree.ANY {
		return nil, moerr.NewInternalError(b.GetContext(), "not implement bindComparisonExpr 3")
	}
	return b.bindFuncExprImplByAstExpr(operator, []tree.Expr{astExpr.Left, astExpr.Right}, depth)
}

// ------------------------------------------------------------------------------------------------------------------------
func (b *baseBinder) bindFuncExprImplByAstExpr(name string, astArgs []tree.Expr, depth int32) (*plan.Expr, error) {
	switch name {
	case "nullif":
		return nil, moerr.NewInternalError(b.GetContext(), "not implement baseBinder 1")
	case "ifnull":
		return nil, moerr.NewInternalError(b.GetContext(), "not implement baseBinder 2")
	case "count":
		if b.bindContext == nil {
			return nil, moerr.NewInvalidInput(b.GetContext(), "invalid field reference to COUNT")
		}
		// we will rewrite "count(*)" to "starcount(col)"
		// count(*) : astExprs[0].(type) is *tree.NumVal
		// count(col_name) : astExprs[0].(type) is *tree.UnresolvedName
		switch nval := astArgs[0].(type) {
		case *tree.NumVal:
			if nval.String() == "*" {
				if len(b.bindContext.bindings) == 0 || len(b.bindContext.bindings[0].columns) == 0 {
					// sql: 'select count(*)' without from clause. we do nothing
				} else {
					// sql: 'select count(*) from t1',
					// rewrite count(*) to starcount(col_name)
					name = "starcount"
					astArgs[0] = tree.NewNumValWithType(constant.MakeInt64(1), "1", false, tree.P_int64)
				}
			}
		}
	}
	// bind ast function's args
	args := make([]*plan.Expr, len(astArgs))
	for i, arg := range astArgs {
		expr, err := b.impl.BindExpr(arg, depth, false)
		if err != nil {
			return nil, err
		}
		args[i] = expr
	}
	return bindFuncExprImplByPlanExpr(b.GetContext(), name, args)
}

func bindFuncExprImplByPlanExpr(ctx context.Context, name string, args []*plan.Expr) (*plan.Expr, error) {
	var err error
	switch name {
	case "date":
		// rewrite date function to cast function, and retrun directly
		if len(args) == 0 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		if args[0].Typ.Id != int32(types.T_varchar) && args[0].Typ.Id != int32(types.T_char) {
			return appendCastBeforeExpr(ctx, args[0], &plan.Type{
				Id: int32(types.T_date),
			})
		}
	case "interval":
		// rewrite interval function to ListExpr, and retrun directly
		return &plan.Expr{
			Expr: &plan.Expr_List{
				List: &plan.ExprList{
					List: args,
				},
			},
			Typ: &plan.Type{
				Id: int32(types.T_interval),
			},
		}, nil
	case "and", "or", "not", "xor":
		return nil, moerr.NewInternalError(ctx, "not implement bindFuncExprImplByPlanExpr 1")
	case "=", "<", "<=", ">", ">=", "<>":
		if err = convertValueIntoBool(name, args, false); err != nil {
			return nil, err
		}
	case "date_add", "date_sub":
		return nil, moerr.NewInternalError(ctx, "not implement bindFuncExprImplByPlanExpr 2")
	case "adddate", "subdate":
		return nil, moerr.NewInternalError(ctx, "not implement bindFuncExprImplByPlanExpr 3")
	case "+":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "operator '+' need two args, but is ", len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}

		if args[0].Typ.Id == int32(types.T_date) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_date) {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[1], args[0])
		} else if args[0].Typ.Id == int32(types.T_datetime) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_datetime) {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[1], args[0])
		} else if args[0].Typ.Id == int32(types.T_varchar) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_varchar) {
			name = "date_add"
			args, err = resetDateFunctionArgs(args[1], args[0])
		} else if args[0].Typ.Id == int32(types.T_varchar) && args[1].Typ.Id == int32(types.T_varchar) {
			name = "concat"
		}
		if err != nil {
			return nil, err
		}
	case "-":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "operator '-' need two args, but is ", len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}
		// rewrite "date '2001' - interval '1 day'" to date_sub(date '2001', 1, day(unit))
		if args[0].Typ.Id == int32(types.T_date) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_datetime) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_varchar) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(args[0], args[1])
		}
		if err != nil {
			return nil, err
		}
	case "*", "/", "%":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, fmt.Sprintf("operator %s need two args", name), len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}
	case "unary_minus":
		return nil, moerr.NewInternalError(ctx, "not implement bindFuncExprImplByPlanExpr 4")
	case "oct", "bit_add", "bit_or", "bit_xor":
		return nil, moerr.NewInternalError(ctx, "not implement bindFuncExprImplByPlanExpr 5")
	case "like":
		return nil, moerr.NewInternalError(ctx, "not implement bindFuncExprImplByPlanExpr 6")
	case "timediff":
		return nil, moerr.NewInternalError(ctx, "not implement bindFuncExprImplByPlanExpr 7")
	case "str_to_date", "to_date":
		return nil, moerr.NewInternalError(ctx, "not implement bindFuncExprImplByPlanExpr 8")
	case "unix_timestamp":
		return nil, moerr.NewInternalError(ctx, "not implement bindFuncExprImplByPlanExpr 9")
	}

	argsNum := len(args)
	argsType := make([]types.Type, argsNum)
	for i, arg := range args {
		argsType[i] = makeTypeByPlan2Expr(arg)
	}

	var funcId int64
	var returnType types.Type
	var argsCastType []types.Type

	funcId, returnType, argsCastType, err := function.GetFunctionByName(name, argsType)
	if err != nil {
		return nil, err
	}

	if function.GetFunctionIsAggregateByName(name) {
		if constExpr, ok := args[0].Expr.(*plan.Expr_C); ok && constExpr.C.Isnull {
			args[0].Typ = makePlan2Type(&returnType)
		}
	}

	switch name {
	case "=", "<", "<=", ">", ">=", "<>":
		switch leftExpr := args[0].Expr.(type) {
		case *plan.Expr_C:
			if _, ok := args[1].Expr.(*plan.Expr_Col); ok {
				if checkNoNeedCast(types.T(args[0].Typ.Id), types.T(args[1].Typ.Id), leftExpr) {
					tempType := types.T(args[1].Typ.Id).ToType()
					argsCastType = []types.Type{tempType, tempType}
					// need to update function id
					funcId, _, _, err := function.GetFunctionByName(ctx, name, argsCastType)
					if err != nil {
						return nil, err
					}
				}
			}
		case *plan.Expr_Col:
			if rightExpr, ok := args[1].Expr.(*plan.Expr_C); ok {
				if checkNoNeedCast(types.T(args[1].Typ.Id), types.T(args[0].Typ.Id), rightExpr) {
					tmpType := types.T(args[0].Typ.Id).ToType() // cast const_expr as column_expr's type
					argsCastType = []types.Type{tmpType, tmpType}
					funcId, _, _, err = function.GetFunctionByName(ctx, name, argsCastType)
					if err != nil {
						return nil, err
					}
				}
			}
		}
	case "timediff":
		return nil, moerr.NewInternalError(ctx, "not implement bindFuncExprImplByPlanExpr 10")
	}

	return nil, err
}

func resetDateFunctionArgs(dateExpr *plan.Expr, intervalExpr *plan.Expr) ([]*plan.Expr, error) {
	return nil, nil
}
