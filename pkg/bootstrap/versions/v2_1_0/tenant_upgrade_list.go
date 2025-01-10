// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v2_1_0

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upg_information_schema_test,
}

var InformationSchemaTableTestDDL = "CREATE TABLE information_schema.test (" +
	"CONSTRAINT_CATALOG varchar(64)," +
	"CONSTRAINT_SCHEMA varchar(64)," +
	"CONSTRAINT_NAME varchar(64)," +
	"TABLE_SCHEMA varchar(64)," +
	"TABLE_NAME varchar(64)," +
	"CONSTRAINT_TYPE varchar(11) NOT NULL DEFAULT ''," +
	"ENFORCED varchar(3) NOT NULL DEFAULT ''" +
	")"

var upg_information_schema_test = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "test",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    InformationSchemaTableTestDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "test")
	},
}
