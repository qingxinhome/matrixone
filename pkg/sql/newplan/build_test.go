package newplan

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/smartystreets/goconvey/convey"
	"os"
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

func TestBuild01(t *testing.T) {
	convey.Convey("test01", t, func() {
		sql := "select l_returnflag from lineitem;"
		res, err := executeSql(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(res, convey.ShouldBeTrue)
	})
}

func TestBuild02(t *testing.T) {
	convey.Convey("test02", t, func() {
		sql := "select t.l_returnflag from lineitem t;"
		res, err := executeSql(sql)
		convey.So(err, convey.ShouldBeNil)
		convey.So(res, convey.ShouldBeTrue)
	})
}
