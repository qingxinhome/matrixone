package newplan

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	"github.com/smartystreets/goconvey/convey"
	"os"
	"strings"
	"testing"
)

func toJSON(v any) []byte {
	byteArr, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out bytes.Buffer
	err = json.Indent(&out, byteArr, "", "  ")
	if err != nil {
		panic(err)
	}
	return out.Bytes()
}

func runOneStmt(compilerContext plan.CompilerContext, t *testing.T, sql string) error {
	t.Logf("SQL: %v\n", sql)
	stmts, err := mysql.Parse(compilerContext.GetContext(), sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	ctx := context.TODO()
	if stmt, ok := stmts[0].(*tree.ExplainStmt); ok {
		es := explain.NewExplainDefaultOptions()
		for _, v := range stmt.Options {
			if strings.EqualFold(v.Name, "VERBOSE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Verbose = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Verbose = false
				} else {
					return moerr.NewInvalidInput(ctx, "boolean value %v", v.Value)
				}
			} else if strings.EqualFold(v.Name, "ANALYZE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Analyze = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Analyze = false
				} else {
					return moerr.NewInvalidInput(ctx, "boolean value %v", v.Value)
				}
			} else if strings.EqualFold(v.Name, "FORMAT") {
				if v.Name == "NULL" {
					return moerr.NewInvalidInput(ctx, "parameter name %v", v.Name)
				} else if strings.EqualFold(v.Value, "TEXT") {
					es.Format = explain.EXPLAIN_FORMAT_TEXT
				} else if strings.EqualFold(v.Value, "JSON") {
					es.Format = explain.EXPLAIN_FORMAT_JSON
				} else {
					return moerr.NewInvalidInput(ctx, "explain format %v", v.Value)
				}
			} else {
				return moerr.NewInvalidInput(ctx, "EXPLAIN option %v", v.Name)
			}
		}

		// this sql always return one stmt
		logicPlan1, err := BuildPlan(compilerContext, stmt.Statement)
		if err != nil {
			t.Errorf("Build Query Plan error: '%v'", tree.String(stmt, dialect.MYSQL))
			return err
		}
		buffer1 := explain.NewExplainDataBuffer()
		explainQuery1 := explain.NewExplainQueryImpl(logicPlan1.GetQuery())
		err = explainQuery1.ExplainPlan(compilerContext.GetContext(), buffer1, es)
		if err != nil {
			t.Errorf("explain Query Plan error: '%v'", tree.String(stmt, dialect.MYSQL))
			return err
		}

		// this sql always return one stmt
		logicPlan2, err := plan.BuildPlan(compilerContext, stmt.Statement)
		if err != nil {
			t.Errorf("Build Query Plan error: '%v'", tree.String(stmt, dialect.MYSQL))
			return err
		}
		buffer2 := explain.NewExplainDataBuffer()
		explainQuery2 := explain.NewExplainQueryImpl(logicPlan2.GetQuery())
		err = explainQuery2.ExplainPlan(compilerContext.GetContext(), buffer2, es)
		if err != nil {
			t.Errorf("explain Query Plan error: '%v'", tree.String(stmt, dialect.MYSQL))
			return err
		}
	}
	return nil
}

func executeSql(sql string) (bool, error) {
	mockCompilerCtx := plan.NewMockCompilerContext(false)
	one, err := parsers.ParseOne(mockCompilerCtx.GetContext(), dialect.MYSQL, sql)
	if err != nil {
		return false, err
	}
	plan1, err := BuildPlan(mockCompilerCtx, one)
	if err != nil {
		return false, err
	}
	fmt.Printf("plan1:%s \n", plan1.String())
	err = os.WriteFile("plan1.json", toJSON(plan1), 0777)
	if err != nil {
		return false, err
	}
	//--------------------------------------------------------------------------------
	two, err := parsers.ParseOne(mockCompilerCtx.GetContext(), dialect.MYSQL, sql)
	if err != nil {
		return false, err
	}
	plan2, err := plan.BuildPlan(mockCompilerCtx, two)
	if err != nil {
		return false, err
	}
	fmt.Printf("plan2:%s \n", plan2.String())
	err = os.WriteFile("plan2.json", toJSON(plan2), 0777)
	if err != nil {
		return false, err
	}
	return plan1.String() == plan2.String(), nil
}

func explainSql(sql string, t *testing.T) (bool, error) {
	mockCompilerCtx := plan.NewMockCompilerContext(false)
	runOneStmt(mockCompilerCtx, t, sql)
	return true, nil
}

func TestBuild01(t *testing.T) {
	convey.Convey("test01", t, func() {
		sql := "select l_returnflag from lineitem;"
		//res, err := executeSql(sql)
		res, err := explainSql("explain verbose "+sql, t)
		convey.So(err, convey.ShouldBeNil)
		convey.So(res, convey.ShouldBeTrue)
	})
}

func TestBuild02(t *testing.T) {
	convey.Convey("test02", t, func() {
		sql := "select t.l_returnflag from lineitem t;"
		//res, err := executeSql(sql)
		res, err := explainSql("explain verbose "+sql, t)
		convey.So(err, convey.ShouldBeNil)
		convey.So(res, convey.ShouldBeTrue)
	})
}

func TestBuild03(t *testing.T) {
	convey.Convey("test03", t, func() {
		sql := "select l_returnflag,l_linestatus,l_quantity,l_extendedprice,l_quantity,l_discount,l_tax from lineitem;"
		res, err := executeSql(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(res, convey.ShouldBeTrue)
	})
}

func TestBuild04(t *testing.T) {
	convey.Convey("test04", t, func() {
		sql := "select l_returnflag as a,l_linestatus as b,l_quantity as b,l_extendedprice as c,l_quantity as d,l_discount as e,l_tax as f from lineitem;"
		res, err := executeSql(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(res, convey.ShouldBeTrue)
	})
}

func TestBuild05(t *testing.T) {
	convey.Convey("test05", t, func() {
		sql := "select l_extendedprice * (1 - l_discount) from lineitem;"
		res, err := executeSql(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(res, convey.ShouldBeTrue)
	})
}

func TestBuild06(t *testing.T) {
	convey.Convey("test06", t, func() {
		sql := "select l_extendedprice * (1 - l_discount) * (1 + l_tax) from lineitem;"
		res, err := executeSql(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(res, convey.ShouldBeTrue)
	})
}

func TestBuild07(t *testing.T) {
	convey.Convey("test07", t, func() {
		sql := "select l_extendedprice * (1 - l_discount) from lineitem where l_orderkey = 2000;"
		res, err := executeSql(sql)
		convey.ShouldBeTrue(res)
		convey.ShouldBeNil(err)
	})
}
