package newplan

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
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
	if clause.Where != nil {
		panic("unimplement")
	}

	//------------------------------------------------------------------------------------------------------------
	bindcontext.groupTag = builder.genNewTag()
	bindcontext.aggregateTag = builder.genNewTag()
	bindcontext.projectTag = builder.genNewTag()
	//------------------------------------------------------------------------------------------------------------

	// handle group by clause
	if clause.GroupBy != nil {
		panic("unimplement")
	}

	// handle having clause
	if clause.Having != nil {
		panic("unimplement")
	}

	return -1, nil
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
