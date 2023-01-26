package newplan

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"math"
)

const (
	NotFound      int32 = math.MaxInt32
	AmbiguousName int32 = math.MaxInt32
)

type baseBinder struct {
	context     context.Context
	builder     *QueryBuilder // current query builder
	bindContext *BindContext  // current bindContext
	impl        Binder        // current implement
	//Set in baseBindColRef
	//Found Columns(table+"."+col) in any Binding
	boundCols []string // columns that have be found in a table in a Binding
}

type Binder interface {
	BindExpr(tree.Expr, int32, bool) (*plan.Expr, error)
	BindColRef(*tree.UnresolvedName, int32, bool) (*plan.Expr, error)
	BindAggFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	BindWinFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	BindSubquery(*tree.Subquery, bool) (*plan.Expr, error)
	GetContext() context.Context
}

type DefaultBinder struct {
	baseBinder
	typ *plan.Type
}
type TableBinder struct {
	baseBinder
}
type WhereBinder struct {
	baseBinder
}

type GroupBinder struct {
	baseBinder
}

type HavingBinder struct {
	baseBinder
	insideAgg bool
}
type ProjectionBinder struct {
	baseBinder
	havingBinder *HavingBinder
}
type OrderByBinder struct {
	*ProjectionBinder
	selectList tree.SelectExprs
}

type LimitBinder struct {
	baseBinder
}

var _ Binder = &DefaultBinder{}
var _ Binder = &TableBinder{}
var _ Binder = &WhereBinder{}
var _ Binder = &GroupBinder{}
var _ Binder = &HavingBinder{}
var _ Binder = &ProjectionBinder{}
var _ Binder = &LimitBinder{}

// -------------------------------------------------------------------------------------------------------------------------
// 一个BindContext实例对应一个查询子句
type BindContext struct {
	// current binder of current clause(from, where, group by, having, project, order by, limit)
	binder          Binder
	parent          *BindContext
	id              uint32
	defaultDatabase string

	// Node_TABLE_SCAN or Node_MATERIAL_SCAN or Node_EXTERNAL_SCAN or subquery mappings a 'Binding' instance
	// tag,nodeID,table,columns,types
	// addBinding appends new one.
	bindings       []*Binding
	bindingByTag   map[int32]*Binding  // tag -> binding
	bindingByTable map[string]*Binding // table alias/table Name -> binding
	bindingByCol   map[string]*Binding // column name -> binding

	// for join tables
	bindingTree *BindingTreeNode

	// origin name of the select expr
	// 1. UnresolvedName -> alias or just parts[0]
	// 2. Others -> alias or expression string
	headings []string
	// the alias of project expr name  -> index of bound project expr in project list
	aliasMap map[string]int32

	groupTag     int32
	aggregateTag int32
	projectTag   int32

	//groupByExpr -> the index of bound groupByExpr
	groupByAst map[string]int32
	groups     []*plan.Expr

	// aggregateByExpr -> the index of bound aggregateByExpr
	aggregateByAst map[string]int32
	aggregates     []*plan.Expr

	// true, when return result is a single row, such as('dual' or without From or without groupby but with aggregates)
	hasSingleRow bool
}

// Node_TABLE_SCAN or Node_MATERIAL_SCAN or Node_EXTERNAL_SCAN or subquery mappings a 'Binding' instance
type Binding struct {
	tag    int32
	nodeId int32
	// table alias or table name if has no alias
	tableName string
	columns   []string
	colTypes  []*plan.Type
	// init with count of column
	refCnts []uint
	// column name -> column index in the table
	colIdByName map[string]int32
}

type NameTuple struct {
	table string
	col   string
}

// for join tables
type BindingTreeNode struct {
	using   []NameTuple
	binding *Binding
	left    *BindingTreeNode
	right   *BindingTreeNode
}

type QueryBuilder struct {
	qry               *plan.Query
	compCtx           plan2.CompilerContext
	bindContextByNode []*BindContext
	nameByColRef      map[[2]int32]string
	nextTag           int32
}
