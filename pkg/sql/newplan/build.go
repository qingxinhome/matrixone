package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func BuildPlan(compilerContext plan2.CompilerContext, statement tree.Statement) (*plan.Plan, error) {
	switch stmt := statement.(type) {
	case *tree.Select:
		return buildSelectByBinder(plan.Query_SELECT, compilerContext, stmt)
	}
	return nil, nil
}

func buildSelectByBinder(stmtType plan.Query_StatementType, compilerContext plan2.CompilerContext, stmt *tree.Select) (*plan.Plan, error) {
	queryBuilder := NewQueryBuilder(stmtType, compilerContext)
	bindContext := NewBindContext(nil)

	rootNodeId, err := queryBuilder.buildSelect(stmt, bindContext, true)
	if err != nil {
		return nil, err
	}
	queryBuilder.qry.Steps = append(queryBuilder.qry.Steps, rootNodeId)

	query, err := queryBuilder.createQuery()
	if err != nil {
		return nil, err
	}

	return &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, nil
}
