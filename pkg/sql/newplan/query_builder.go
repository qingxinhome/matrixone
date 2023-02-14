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
		NodeType: plan.Node_PROJECT,
		Children: []int32{nodeId},
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
		builder.appendNode(&plan.Node{
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
		builder.appendNode(&plan.Node{
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
	return nil, nil
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
