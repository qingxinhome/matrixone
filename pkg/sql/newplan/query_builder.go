package newplan

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"strings"
)

func NewQueryBuilder(queryType plan.Query_StatementType, compilerContext plan2.CompilerContext) *QueryBuilder {
	return &QueryBuilder{
		qry:               &plan.Query{StmtType: queryType},
		compCtx:           compilerContext,
		bindContextByNode: []*BindContext{},
		nameByColRef:      make(map[[2]int32]string),
		nextTag:           0,
	}
}

type ColRefRemapping struct {
	// 映射： 在此节点上要变化的列引用 -> 在此节点上所有要变化的列引用中序号, 即localToGlobal的序号。
	globalToLocal map[[2]int32][2]int32
	//在此节点上要变化的列引用
	localToGlobal [][2]int32
}

// 记录要变化的列引用
func (m *ColRefRemapping) addColRef(colRef [2]int32) {
	// global colRef -> [0, index of the localToGlobal] = global colRef
	m.globalToLocal[colRef] = [2]int32{0, int32(len(m.localToGlobal))}
	m.localToGlobal = append(m.localToGlobal, colRef)
}

func (builder *QueryBuilder) remapExpr(expr *plan.Expr, globalTolocalMap map[[2]int32][2]int32) error {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		globalKey := [2]int32{exprImpl.Col.RelPos, exprImpl.Col.ColPos}
		if localValue, ok := globalTolocalMap[globalKey]; ok {
			exprImpl.Col.RelPos = localValue[0]
			exprImpl.Col.ColPos = localValue[1]
			exprImpl.Col.Name = builder.nameByColRef[globalKey]
		} else {
			return moerr.NewParseError(builder.GetContext(), "can't find column %v in context's map %v", globalKey, globalTolocalMap)
		}
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			err := builder.remapExpr(arg, globalTolocalMap)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// 函数remapAllColRefs用于对列引用进行重映射。
// 刚进入函数时，colRefCnt记录的是从树根节点到当点节点所有列的引用的计数信息
// 每个节点上的处理类似。
// 先对表达式增加引用计数, 其次对子节点递归执行重映射, 之后对表达式减少引用计数, 用子节点变化的列引用，重映射表达式中的列引用。
// 生成新的projectlist。 每个节点都会生成projectList。
func (builder *QueryBuilder) remapAllColRefs(nodeId int32, colRefCnt map[[2]int32]int) (*ColRefRemapping, error) {
	node := builder.qry.Nodes[nodeId]

	remapping := &ColRefRemapping{
		globalToLocal: make(map[[2]int32][2]int32),
	}

	switch node.NodeType {
	case plan.Node_TABLE_SCAN,
		plan.Node_MATERIAL_SCAN,
		plan.Node_EXTERNAL_SCAN:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, colRefCnt)
		}
		internalRemapping := &ColRefRemapping{
			globalToLocal: make(map[[2]int32][2]int32),
		}
		nodeTag := node.BindingTags[0]
		newTableDef := &plan.TableDef{
			Name:          node.TableDef.Name,
			Defs:          node.TableDef.Defs,
			TableType:     node.TableDef.TableType,
			Createsql:     node.TableDef.Createsql,
			Name2ColIndex: node.TableDef.Name2ColIndex,
			CompositePkey: node.TableDef.CompositePkey,
			TblFunc:       node.TableDef.TblFunc,
			Indices:       node.TableDef.Indices,
		}

		for i, col := range node.TableDef.Cols {
			globalRef := [2]int32{nodeTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}
			internalRemapping.addColRef(globalRef)
			newTableDef.Cols = append(newTableDef.Cols, col)
		}

		if len(newTableDef.Cols) == 0 {
			internalRemapping.addColRef([2]int32{nodeTag, 0})
			newTableDef.Cols = append(newTableDef.Cols, node.TableDef.Cols[0])
		}
		node.TableDef = newTableDef
		for _, expr := range node.FilterList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, internalRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		for i, col := range node.TableDef.Cols {
			if colRefCnt[internalRemapping.localToGlobal[i]] == 0 {
				continue
			}
			remapping.addColRef(internalRemapping.localToGlobal[i])

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					// 改为相对位置，即相对子节点的坐标
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[internalRemapping.localToGlobal[i]],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(node.TableDef.Cols) == 0 {
				globalRef := [2]int32{nodeTag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.TableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						// 改为相对位置，即相对子节点的坐标
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: 0,
							Name:   builder.nameByColRef[globalRef],
						},
					},
				})
			} else {
				remapping.addColRef(internalRemapping.localToGlobal[0])
				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.TableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						// 改为相对位置，即相对子节点的坐标
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: 0,
							Name:   builder.nameByColRef[internalRemapping.localToGlobal[0]],
						},
					},
				})
			}
		}
	case plan.Node_INTERSECT,
		plan.Node_INTERSECT_ALL,
		plan.Node_UNION,
		plan.Node_UNION_ALL,
		plan.Node_MINUS,
		plan.Node_MINUS_ALL:
	case plan.Node_JOIN:
		return nil, moerr.NewInternalError(builder.GetContext(), "not implement qb 3")
	case plan.Node_AGG:
		//对groupby增加colRefCnt列引用计数。
		for _, expr := range node.GroupBy {
			increaseRefCnt(expr, colRefCnt)
		}
		//对agglist增加colRefCnt列引用计数。
		for _, expr := range node.AggList {
			increaseRefCnt(expr, colRefCnt)
		}

		//采用递归的方式对子节点的列引用进行重映射。递归完成后，得到子节点变化的列引用childRemapping。
		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		groupTag := node.BindingTags[0]
		aggTag := node.BindingTags[1]

		for idx, expr := range node.GroupBy {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{groupTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}
			remapping.addColRef(globalRef)
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		for idx, expr := range node.AggList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{aggTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}
			remapping.addColRef(globalRef)
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -2,
						ColPos: int32(idx),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(node.GroupBy) > 0 {
				globalRef := [2]int32{groupTag, 0}
				remapping.addColRef(globalRef)
				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.GroupBy[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -1,
							ColPos: 0,
							Name:   builder.nameByColRef[globalRef],
						},
					},
				})
			} else {
				globalRef := [2]int32{aggTag, 0}
				remapping.addColRef(globalRef)
				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.AggList[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -2,
							ColPos: 0,
						},
					},
				})
			}
		}
	case plan.Node_SORT:
		// 对orderby expr list增加colRefCnt列引用计数。
		for _, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, colRefCnt)
		}
		// 使用递归的方式对子节点列引用重映射。递归完成后，得到子节点变化的列引用childRemapping。
		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		for _, orderBy := range node.OrderBy {
			decreaseRefCnt(orderBy.Expr, colRefCnt)
			err := builder.remapExpr(orderBy.Expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		// 处理子节点变化的列引用
		childProjectList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}
			remapping.addColRef(globalRef)
			// 使用子节点的projectlist为Sort节点构造projectlist
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjectList[i].Typ,
				Expr: &plan.Expr_Col{
					// 改为相对位置，即相对子节点的坐标
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			})
		}

		if len(node.ProjectList) == 0 && len(childRemapping.localToGlobal) > 0 {
			globalRef := childRemapping.localToGlobal[0]
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjectList[0].Typ,
				Expr: &plan.Expr_Col{
					// 改为相对位置，即相对子节点的坐标
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}
	case plan.Node_FILTER:
		// 对filterlist增加colRefCnt列引用计数。
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, colRefCnt)
		}
		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}
		for _, expr := range node.FilterList {
			// 减少colRefCnt列引用计数。
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		//处理子节点变化的列引用(childRemapping.localToGlobal)。
		childProjectList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}
			remapping.addColRef(globalRef)
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjectList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(childRemapping.localToGlobal) > 0 {
				remapping.addColRef(childRemapping.localToGlobal[0])
			}
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjectList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}
	case plan.Node_PROJECT, plan.Node_MATERIAL:
		projectTag := node.BindingTags[0]
		var needProject []int32
		// 对projectList排除掉不需要的表达式，对需要的表达式增加colRefCnt列引用计数。
		for i, expr := range node.ProjectList {
			globalRef := [2]int32{projectTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}
			needProject = append(needProject, int32(i))
			increaseRefCnt(expr, colRefCnt)
		}

		if len(needProject) == 0 {
			increaseRefCnt(node.ProjectList[0], colRefCnt)
			needProject = append(needProject, 0)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		var newProjectList []*plan.Expr
		for _, need := range needProject {
			expr := node.ProjectList[need]
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalTag := [2]int32{projectTag, need}
			remapping.addColRef(globalTag)

			newProjectList = append(newProjectList, expr)
		}
		node.ProjectList = newProjectList
	case plan.Node_DISTINCT:
		return nil, moerr.NewInternalError(builder.GetContext(), "not implement qb 6")
	case plan.Node_VALUE_SCAN:
		return nil, moerr.NewInternalError(builder.GetContext(), "not implement qb 7")
	default:
		return nil, moerr.NewInternalError(builder.GetContext(), "not implement qb 8")
	}

	node.BindingTags = nil

	return remapping, nil
}

func (builder *QueryBuilder) GetContext() context.Context {
	if builder == nil {
		return context.TODO()
	}
	return builder.compCtx.GetContext()
}

// tag -> binding
// table name or alias -> binding
// column -> binding
func (builder *QueryBuilder) addBinding(nodeId int32, alias tree.AliasClause, bindcontext *BindContext) error {
	node := builder.qry.Nodes[nodeId]
	fmt.Printf("addBinding, nodeId:%v \n", nodeId)
	if node.NodeType == plan.Node_VALUE_SCAN {
		return nil
	}

	var binding *Binding
	var columns []string
	var colTypes []*plan.Type
	if node.NodeType == plan.Node_TABLE_SCAN ||
		node.NodeType == plan.Node_MATERIAL_SCAN ||
		node.NodeType == plan.Node_EXTERNAL_SCAN {
		if len(alias.Cols) > len(node.TableDef.Cols) {
			return moerr.NewSyntaxError(builder.GetContext(), "table %q has %d columns available but %d columns specified", alias.Alias, len(node.TableDef.Cols), len(alias.Cols))
		}

		var tableAlias string
		if alias.Alias != "" {
			tableAlias = string(alias.Alias)
		} else {
			tableAlias = node.TableDef.Name
		}
		fmt.Printf("table Alias: %s \n", tableAlias)

		if _, ok := bindcontext.bindingByTable[tableAlias]; ok {
			return moerr.NewSyntaxError(builder.GetContext(), "table name %q specified more than once", tableAlias)
		}

		columns = make([]string, len(node.TableDef.Cols))
		colTypes = make([]*plan.Type, len(node.TableDef.Cols))

		nodeTag := node.BindingTags[0]

		for i, col := range node.TableDef.Cols {
			if i < len(alias.Cols) {
				columns[i] = string(alias.Cols[i])
			} else {
				columns[i] = col.Name
			}
			colTypes[i] = col.Typ

			// table column qualified name
			qualifyColName := tableAlias + "." + columns[i]
			// <binding tag, columnIdx> -> (table name or alias).columnName
			builder.nameByColRef[[2]int32{nodeTag, int32(i)}] = qualifyColName
		}
		binding = NewBinding(nodeTag, nodeId, tableAlias, columns, colTypes)
	} else {
		// subquery
	}
	bindcontext.bindings = append(bindcontext.bindings, binding)
	// tag -> binding
	bindcontext.bindingByTag[binding.tag] = binding
	// table Alias -> binding
	bindcontext.bindingByTable[binding.tableName] = binding
	// column qualified name -> binding
	for _, column := range columns {
		if _, ok := bindcontext.bindingByCol[column]; ok {
			// I think we should report mistakes directly here
			bindcontext.bindingByCol[column] = nil
		} else {
			bindcontext.bindingByCol[column] = binding
		}
	}

	bindcontext.bindingTree = &BindingTreeNode{
		binding: binding,
	}
	return nil
}

// 构建查询语句的执行计划
// is Root: 如果子查询是false, 如果最外层的sql是true
func (builder *QueryBuilder) buildSelect(stmt *tree.Select, bindcontext *BindContext, isRoot bool) (int32, error) {
	var clause *tree.SelectClause
	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		clause = selectClause
	}
	nodeId, err := builder.buildFrom(clause.From.Tables, bindcontext)
	if err != nil {
		return -1, err
	}
	fmt.Printf("build From return nodeId:%v \n", nodeId)

	// handle select list
	var selectList tree.SelectExprs
	for _, selectExpr := range clause.Exprs {
		switch expr := selectExpr.Expr.(type) {
		case tree.UnqualifiedStar:
			panic("unimplement")
		case *tree.UnresolvedName:
			if expr.Star {
				panic("unimplement")
			} else {
				if len(selectExpr.As) > 0 {
					bindcontext.headings = append(bindcontext.headings, string(selectExpr.As))
				} else {
					bindcontext.headings = append(bindcontext.headings, expr.Parts[0])
				}

				newExpr, err := bindcontext.qualifyColumnNames(expr, nil, false)
				if err != nil {
					return -1, err
				}

				selectList = append(selectList, tree.SelectExpr{
					Expr: newExpr,
					As:   selectExpr.As,
				})
			}
		default:
			if len(selectExpr.As) > 0 {
				bindcontext.headings = append(bindcontext.headings, string(selectExpr.As))
			} else {
				for {
					if parenExpr, ok := expr.(*tree.ParenExpr); ok {
						expr = parenExpr.Expr
					} else {
						break
					}
				}
				bindcontext.headings = append(bindcontext.headings, tree.String(expr, dialect.MYSQL))
			}
			newExpr, err := bindcontext.qualifyColumnNames(expr, nil, false)
			if err != nil {
				return -1, err
			}

			selectList = append(selectList, tree.SelectExpr{
				Expr: newExpr,
				As:   selectExpr.As,
			})
		}
	}
	if len(selectList) == 0 {
		return -1, moerr.NewSyntaxError(builder.GetContext(), "No tables used")
	}

	// handle where clause
	bindcontext.binder = NewWhereBinder(builder, bindcontext)
	if clause.Where != nil {
		whereList, err := splitAndBindCondition(clause.Where.Expr, bindcontext)
		if err != nil {
			return -1, err
		}

		var expr *plan.Expr
		var filterList []*plan.Expr
		//do nothing in tpch.q1
		for _, condition := range whereList {
			nodeId, expr, err = builder.flattenSubqueries(nodeId, condition, bindcontext)
			if err != nil {
				return -1, err
			}
			if expr != nil {
				filterList = append(filterList, expr)
			}
		}
		nodeId = builder.appendNode(&plan.Node{
			NodeType:   plan.Node_FILTER,
			Children:   []int32{nodeId},
			FilterList: filterList,
		}, bindcontext)
	}

	//------------------------------------------------------------------------------------------------------------
	bindcontext.groupTag = builder.genNewTag()
	bindcontext.aggregateTag = builder.genNewTag()
	bindcontext.projectTag = builder.genNewTag()
	//------------------------------------------------------------------------------------------------------------

	// handle group by clause
	if clause.GroupBy != nil {
		groupBinder := NewGroupBinder(builder, bindcontext)
		for _, groupExpr := range clause.GroupBy {
			groupExpr, err = bindcontext.qualifyColumnNames(groupExpr, nil, false)
			if err != nil {
				return -1, err
			}

			_, err = groupBinder.BindExpr(groupExpr, 0, true)
			if err != nil {
				return -1, err
			}
		}
	}

	// handle having clause
	var havingList []*plan.Expr
	havingBinder := NewHavingBinder(builder, bindcontext)
	if clause.Having != nil {
		bindcontext.binder = havingBinder
		havingList, err = splitAndBindCondition(clause.Having.Expr, bindcontext)
		if err != nil {
			return -1, err
		}
	}

	projectBinder := NewProjectionBinder(builder, bindcontext, havingBinder)
	bindcontext.binder = projectBinder
	for i, selectExpr := range selectList {
		astExpr, err := bindcontext.qualifyColumnNames(selectExpr.Expr, nil, false)
		if err != nil {
			return -1, err
		}
		expr, err := projectBinder.BindExpr(astExpr, 0, true)
		if err != nil {
			return -1, err
		}
		key := [2]int32{bindcontext.projectTag, int32(i)}
		builder.nameByColRef[key] = tree.String(astExpr, dialect.MYSQL)

		projectAlias := string(selectExpr.As)
		if len(projectAlias) > 0 {
			bindcontext.aliasMap[projectAlias] = int32(len(bindcontext.projects))
		}
		bindcontext.projects = append(bindcontext.projects, expr)
	}

	resultLen := len(bindcontext.projects)
	for i, project := range bindcontext.projects {
		projectStr := project.String()
		if _, ok := bindcontext.projectByExpr[projectStr]; !ok {
			bindcontext.projectByExpr[projectStr] = int32(i)
		}
	}
	bindcontext.isDistinct = clause.Distinct

	var orderBys []*plan.OrderBySpec
	if stmt.OrderBy != nil {
		// astOrderBy is different from selectList
		// ast in astOrderBy have not been qualified.
		orderbyBinder := NewOrderByBinder(projectBinder, selectList)
		orderBys = make([]*plan.OrderBySpec, 0, len(stmt.OrderBy))

		for _, order := range stmt.OrderBy {
			expr, err := orderbyBinder.BindExpr(order.Expr)
			if err != nil {
				return -1, err
			}

			orderby := &plan.OrderBySpec{
				Expr: expr,
				Flag: plan.OrderBySpec_INTERNAL,
			}

			switch order.Direction {
			case tree.Ascending:
				orderby.Flag |= plan.OrderBySpec_ASC
			case tree.Descending:
				orderby.Flag |= plan.OrderBySpec_DESC
			}

			switch order.NullsPosition {
			case tree.NullsFirst:
				orderby.Flag |= plan.OrderBySpec_NULLS_FIRST
			case tree.NullsLast:
				orderby.Flag |= plan.OrderBySpec_NULLS_LAST
			}
			orderBys = append(orderBys, orderby)
		}
	}

	var limitExpr *plan.Expr
	var offsetExpr *plan.Expr
	if stmt.Limit != nil {
		limitBinder := NewLimitBinder(builder, bindcontext)
		if stmt.Limit.Offset != nil {
			offsetExpr, err = limitBinder.BindExpr(stmt.Limit.Offset, 0, true)
			if err != nil {
				return -1, err
			}
		}

		if stmt.Limit.Count != nil {
			limitExpr, err = limitBinder.BindExpr(stmt.Limit.Count, 0, true)
			if err != nil {
				return -1, err
			}

			if exprC, ok := limitExpr.Expr.(*plan.Expr_C); ok {
				if c, ok2 := exprC.C.Value.(*plan.Const_I64Val); ok2 {
					bindcontext.hasSingleRow = c.I64Val == 1
				}
			}
		}
	}

	if (len(bindcontext.groups) > 0 || len(bindcontext.aggregates) > 0) && len(projectBinder.boundCols) > 0 {
		mode, err := builder.compCtx.ResolveVariable("sql_mode", true, false)
		if err != nil {
			return -1, err
		}

		// ONLY_FULL_GROUP_BY -> sql中select后面的字段必须出现在group by后面，或者被聚合函数包裹，不然会抛出错误
		if strings.Contains(mode.(string), "ONLY_FULL_GROUP_BY") {
			// projectionBinder.boundCols 远宁，是用来做什么的，存的什么信息？
			return -1, moerr.NewSyntaxError(builder.GetContext(), "column %q must appear in the GROUP BY clause or be used in an aggregate function", projectBinder.boundCols[0])
		}
	}

	if len(bindcontext.groups) == 0 && len(bindcontext.aggregates) > 0 {
		// true, when return result is a single row, such as('dual' or without From or without groupby but with aggregates)
		bindcontext.hasSingleRow = true
	}

	// with group or aggreate
	if len(bindcontext.groups) > 0 || len(bindcontext.aggregates) > 0 {
		newNode := &plan.Node{
			NodeType:    plan.Node_AGG,
			Children:    []int32{nodeId},
			GroupBy:     bindcontext.groups,
			AggList:     bindcontext.aggregates,
			BindingTags: []int32{bindcontext.groupTag, bindcontext.aggregateTag},
		}
		nodeId = builder.appendNode(newNode, bindcontext)

		// 展开having子句中的子查询
		if len(havingList) > 0 {
			var newFilterList []*plan.Expr
			var expr *plan.Expr

			for _, cond := range havingList {
				// having 子句中也可以有子查询吗?
				nodeId, expr, err = builder.flattenSubqueries(nodeId, cond, bindcontext)
				if err != nil {
					return -1, err
				}

				if expr != nil {
					newFilterList = append(newFilterList, expr)
				}
			}

			node := &plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{nodeId},
				FilterList: newFilterList,
			}
			nodeId = builder.appendNode(node, bindcontext)
		}

		// add groupByAst info to querybuilder nameByColRef
		for name, idx := range bindcontext.groupByAst {
			key := [2]int32{
				bindcontext.groupTag,
				idx,
			}
			builder.nameByColRef[key] = name
		}
		// add aggregateByAst info querybuilder nameByColRef
		for name, idx := range bindcontext.aggregateByAst {
			key := [2]int32{
				bindcontext.aggregateTag,
				idx,
			}
			builder.nameByColRef[key] = name
		}
	}

	// 展开 selectList 中的子查询
	for i, project := range bindcontext.projects {
		nodeId, project, err = builder.flattenSubqueries(nodeId, project, bindcontext)
		if err != nil {
			return -1, err
		}

		if project == nil {
			return -1, moerr.NewNYI(builder.GetContext(), "non-scalar subquery in SELECT clause")
		}
		bindcontext.projects[i] = project
	}

	fmt.Println("projectlist:", bindcontext.projects)

	// append project node
	nodeId = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: bindcontext.projects,
		Children:    []int32{nodeId},
		BindingTags: []int32{bindcontext.projectTag},
	}, bindcontext)

	// append distinct node
	if clause.Distinct {
		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_DISTINCT,
			Children: []int32{nodeId},
		}, bindcontext)
	}

	// append sort node
	if len(orderBys) > 0 {
		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{nodeId},
			OrderBy:  orderBys,
		}, bindcontext)
	}

	// append limit info to current node
	if limitExpr != nil || offsetExpr != nil {
		currNode := builder.qry.Nodes[nodeId]
		currNode.Limit = limitExpr
		currNode.Offset = offsetExpr
	}

	// 如果plan tree根节点不是project节点，则追加一个project节点
	if builder.qry.Nodes[nodeId].NodeType != plan.Node_PROJECT {
		for i := 0; i < resultLen; i++ {
			bindcontext.results = append(bindcontext.results, &plan.Expr{
				Typ: bindcontext.projects[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: bindcontext.projectTag,
						ColPos: int32(i),
					},
				},
			})
		}

		bindcontext.resultTag = builder.genNewTag()
		nodeId = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: bindcontext.results,
			Children:    []int32{nodeId},
			BindingTags: []int32{bindcontext.resultTag},
		}, bindcontext)
	} else {
		/*
			bindContext的projects  和 queryBuilder.results 区别:
			queryBuilder.results是查询结果的投影列
			bindContext的projects是在计划构建过程中,[查询结果的投影列] + [order by使用列]的并集，
			queryBuilder.projects结果中可能隐式会多处order by列, 因此queryBuilder.projects和queryBuilder.results长度不一定相等
			例如：
				select empno,ename from emp order by sal;
			bindContext的projects:  [empno,ename,sal]
			queryBuilder.results: [empno,ename]
		*/
		bindcontext.results = bindcontext.projects
	}

	if isRoot {
		builder.qry.Headings = append(builder.qry.Headings, bindcontext.headings...)
	}
	return nodeId, nil
}

func (builder *QueryBuilder) buildFrom(tableExprs tree.TableExprs, bindcontext *BindContext) (int32, error) {
	if len(tableExprs) == 1 {
		return builder.buildTable(tableExprs[0], bindcontext)
	}
	return -1, nil
}

func (builder *QueryBuilder) buildTable(tableExpr tree.TableExpr, bindcontext *BindContext) (int32, error) {
	switch tblExpr := tableExpr.(type) {
	case *tree.TableName:
		schemaName := string(tblExpr.SchemaName)
		tableName := string(tblExpr.ObjectName)

		// handle special table name
		if tableName == "" || tableName == "dual" {
			builder.appendNode(&plan.Node{
				NodeType: plan.Node_VALUE_SCAN,
			}, bindcontext)
			bindcontext.hasSingleRow = true
			break
		}

		if len(schemaName) == 0 {
			schemaName = bindcontext.defaultDatabase
		}

		objectRef, tableDef := builder.compCtx.Resolve(schemaName, tableName)
		if tableDef == nil {
			return -1, moerr.NewParseError(builder.GetContext(), "table %T does not exist", tblExpr)
		}
		tableDef.Name2ColIndex = make(map[string]int32)
		for i := 0; i < len(tableDef.Cols); i++ {
			tableDef.Name2ColIndex[tableDef.Cols[i].Name] = int32(i)
		}

		nodeType := plan.Node_TABLE_SCAN
		if tableDef.TableType == catalog.SystemExternalRel {
			nodeType = plan.Node_EXTERNAL_SCAN
		}

		nodeId := builder.appendNode(&plan.Node{
			NodeType:    nodeType,
			Stats:       builder.compCtx.Stats(objectRef, nil),
			ObjRef:      objectRef,
			TableDef:    tableDef,
			BindingTags: []int32{builder.genNewTag()},
		}, bindcontext)

		return nodeId, nil
	case *tree.JoinTableExpr:
		if tblExpr.Right == nil {
			return builder.buildTable(tblExpr.Left, bindcontext)
		}
		return builder.buildJoinTable(tblExpr, bindcontext)
	case *tree.ParenTableExpr:
		return builder.buildTable(tblExpr.Expr, bindcontext)
	case *tree.AliasedTableExpr:
		if _, ok := tblExpr.Expr.(*tree.Select); ok {
			if tblExpr.As.Alias == "" {
				return -1, moerr.NewSyntaxError(builder.GetContext(), "subquery in from clause must have an alias: %T", tblExpr)
			}
		}
		nodeId, err := builder.buildTable(tblExpr.Expr, bindcontext)
		if err != nil {
			return -1, err
		}

		// 只会对tree.AliasedTableExpr的语义检查结果做addBinding操作
		err = builder.addBinding(nodeId, tblExpr.As, bindcontext)
		if err != nil {
			return -1, err
		}
		return nodeId, nil
	default:
		return 0, moerr.NewParseError(builder.GetContext(), "unsupport table expr:%T", tableExpr)
	}
	return -1, nil
}

func (builder *QueryBuilder) createQuery() (*plan.Query, error) {
	for i, rootId := range builder.qry.Steps {
		rootId, _ := builder.pushdownFilters(rootId, nil)
		builder.qry.Steps[i] = rootId

		// 执行计划树的列引用计数map
		colRefCnt := make(map[[2]int32]int)
		rootNode := builder.qry.Nodes[rootId]
		resultTag := rootNode.BindingTags[0]
		for i, _ := range rootNode.ProjectList {
			colRefCnt[[2]int32{resultTag, int32(i)}] = 1
		}
		// 通过列引用计数的方式。对没有被引用的列进行裁剪。对留下来的列引用重新编号，并用新编号替换plan树中的旧编号。
		_, err := builder.remapAllColRefs(rootId, colRefCnt)
		if err != nil {
			return nil, err
		}
	}

	return builder.qry, nil
}

// 遍历plan节点，做谓词下推操作
func (builder *QueryBuilder) pushdownFilters(nodeId int32, filters []*plan.Expr) (int32, []*plan.Expr) {
	node := builder.qry.Nodes[nodeId]
	var canPushdown []*plan.Expr
	var cantPushdown []*plan.Expr
	switch node.NodeType {
	case plan.Node_AGG:
		groupTag := node.BindingTags[0]
		aggregateTag := node.BindingTags[1]
		for _, filter := range filters {
			if !containsTag(filter, aggregateTag) {
				canPushdown = append(canPushdown, replaceColRefs(filter, groupTag, node.GroupBy))
			} else {
				cantPushdown = append(cantPushdown, filter)
			}
		}
		childId, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown)
		if len(cantPushdownChild) > 0 {
			childId = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}
		node.Children[0] = childId
	case plan.Node_FILTER:
		canPushdown = filters
		for _, filterExpr := range node.FilterList {
			canPushdown = append(canPushdown, splitConjunction(applyDistributivity(builder.GetContext(), filterExpr))...)
		}
		childNodeId, cantPushDownChild := builder.pushdownFilters(node.Children[0], canPushdown)
		var extraFilters []*plan.Expr
		for _, filter := range cantPushDownChild {
			switch exprImpl := filter.Expr.(type) {
			case *plan.Expr_F:
				if exprImpl.F.Func.ObjName == "or" {
					keys := checkDNF(filter)
					for _, key := range keys {
						extraFilter := walkThroughDNF(builder.GetContext(), filter, key)
						if extraFilter != nil {
							extraFilters = append(extraFilters, DeepCopyExpr(extraFilter))
						}
					}
				}
			}
		}
		builder.pushdownFilters(node.Children[0], extraFilters)
		if len(cantPushDownChild) > 0 {
			node.Children[0] = childNodeId
			node.FilterList = cantPushDownChild
		} else {
			nodeId = childNodeId
		}
	case plan.Node_JOIN:
		panic("pushdownFilter not implement 3")
	case plan.Node_UNION, plan.Node_UNION_ALL, plan.Node_MINUS, plan.Node_MINUS_ALL, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
		panic("pushdownFilter not implement 4")
	case plan.Node_PROJECT:
		child := builder.qry.Nodes[node.Children[0]]
		if (child.NodeType == plan.Node_VALUE_SCAN || child.NodeType == plan.Node_EXTERNAL_SCAN) && child.RowsetData == nil {
			cantPushdown = filters
			break
		}

		projectTag := node.BindingTags[0]
		for _, filter := range filters {
			canPushdown = append(canPushdown, replaceColRefs(filter, projectTag, node.ProjectList))
		}
		childId, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown)
		if len(cantPushdownChild) > 0 {
			builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}
		node.Children[0] = childId
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN:
		node.FilterList = append(node.FilterList, filters...)
	default:
		if len(node.Children) > 0 {
			childId, cantPushdownChild := builder.pushdownFilters(node.Children[0], filters)
			if len(cantPushdownChild) > 0 {
				childId = builder.appendNode(&plan.Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{node.Children[0]},
					FilterList: cantPushdownChild,
				}, nil)
			}
			node.Children[0] = childId
		} else {
			cantPushdown = filters
		}
	}
	return nodeId, cantPushdown
}

func (builder *QueryBuilder) buildJoinTable(joinTableExpr *tree.JoinTableExpr, bindcontext *BindContext) (int32, error) {
	return -1, nil
}

// 展开子查询
func (builder *QueryBuilder) flattenSubqueries(nodeId int32, expr *plan.Expr, bindcontext *BindContext) (int32, *plan.Expr, error) {
	var err error
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			nodeId, exprImpl.F.Args[i], err = builder.flattenSubqueries(nodeId, arg, bindcontext)
			if err != nil {
				return -1, nil, err
			}
		}
	case *plan.Expr_Sub:
		nodeId, expr, err = builder.flattenSubquery(nodeId, exprImpl.Sub, bindcontext)
		if err != nil {
			return -1, nil, err
		}
	}
	return nodeId, expr, err
}

// 展开子查询
func (builder *QueryBuilder) flattenSubquery(nodeId int32, subquery *plan.SubqueryRef, bindcontext *BindContext) (int32, *plan.Expr, error) {
	return 0, nil, moerr.NewInternalError(bindcontext.binder.GetContext(), "flattenSubquery is not implemented")
}

func (builder *QueryBuilder) appendNode(node *plan.Node, bindcontext *BindContext) int32 {
	nodeId := int32(len(builder.qry.Nodes))
	node.NodeId = nodeId
	builder.qry.Nodes = append(builder.qry.Nodes, node)
	builder.bindContextByNode = append(builder.bindContextByNode, bindcontext)
	CalcNodeStats(nodeId, builder, false)
	return nodeId
}

func (builder *QueryBuilder) genNewTag() int32 {
	builder.nextTag++
	return builder.nextTag
}

func splitAndBindCondition(astExpr tree.Expr, bindContext *BindContext) ([]*plan.Expr, error) {
	conjuncts := splitAstConjunction(astExpr)
	exprs := make([]*plan.Expr, len(conjuncts))

	for i, conjunct := range conjuncts {
		conjunctExpr, err := bindContext.qualifyColumnNames(conjunct, nil, false)
		if err != nil {
			return nil, err
		}
		expr, err := bindContext.binder.BindExpr(conjunctExpr, 0, true)
		if err != nil {
			return nil, err
		}
		if expr.GetSub() == nil {
			//add CAST
			expr, err = makePlan2CastExpr(bindContext.binder.GetContext(), expr, &plan.Type{
				Id: int32(types.T_bool),
			})
			if err != nil {
				return nil, err
			}
		}
		exprs[i] = expr
	}
	return exprs, nil
}

func splitAstConjunction(astExpr tree.Expr) []tree.Expr {
	var exprs []tree.Expr
	switch expr := astExpr.(type) {
	case nil:
	case *tree.AndExpr:
		exprs = append(exprs, splitAstConjunction(expr.Left)...)
		exprs = append(exprs, splitAstConjunction(expr.Right)...)
	case *tree.ParenExpr:
		exprs = append(exprs, splitAstConjunction(expr.Expr)...)
	default:
		exprs = append(exprs, expr)
	}
	return exprs
}
