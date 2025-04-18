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

package v2_0_0

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

func getSubInfoFromSql(sql string) (subName, pubAccountName, pubName string, err error) {
	var ast []tree.Statement
	if ast, err = mysql.Parse(context.TODO(), sql, 1); err != nil {
		return
	}
	defer func() {
		for _, s := range ast {
			s.Free()
		}
	}()
	subName = string(ast[0].(*tree.CreateDatabase).Name)
	pubAccountName = string(ast[0].(*tree.CreateDatabase).SubscriptionOption.From)
	pubName = string(ast[0].(*tree.CreateDatabase).SubscriptionOption.Publication)
	return
}

// getPubSubscribedInfos returns map[pubAccountName#pubName] -> subscribedInfos
var getPubSubscribedInfos = func(txn executor.TxnExecutor) (subscribedInfos map[string][]*pubsub.SubInfo, err error) {
	sql := "select dat_createsql, created_time, account_id from mo_catalog.mo_database where dat_type = 'subscription'"

	res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(0))
	if err != nil {
		getLogger(txn.Txn().TxnOptions().CN).Error("pubSubscribedInfos error", zap.Error(err))
		return
	}
	defer res.Close()

	subscribedInfos = make(map[string][]*pubsub.SubInfo)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			var subInfo pubsub.SubInfo
			createSql := cols[0].GetStringAt(i)
			subInfo.SubName, subInfo.PubAccountName, subInfo.PubName, _ = getSubInfoFromSql(createSql)
			subInfo.SubTime = vector.GetFixedAtWithTypeCheck[types.Timestamp](cols[1], i).String2(time.Local, cols[1].GetType().Scale)
			subInfo.SubAccountId = int32(vector.GetFixedAtWithTypeCheck[uint32](cols[2], i))

			key := subInfo.PubAccountName + "#" + subInfo.PubName
			subscribedInfos[key] = append(subscribedInfos[key], &subInfo)
		}
		return true
	})
	return
}

func generateInsertSql(info *pubsub.SubInfo) string {
	subName, subTime := "null", "null"
	if len(info.SubName) > 0 {
		subName, subTime = fmt.Sprintf("'%s'", info.SubName), fmt.Sprintf("'%s'", info.SubTime)
	}
	return fmt.Sprintf("insert into mo_catalog.mo_subs (sub_account_id, sub_name, sub_time, pub_account_name, pub_name, pub_database, "+
		"pub_tables, pub_time, pub_comment, status) values (%d, %s, %s, '%s', '%s', '%s', '%s', now(), '%s', %d)",
		info.SubAccountId, subName, subTime, info.PubAccountName, info.PubName, info.PubDbName, info.PubTables, info.PubComment, info.Status)
}

func UpgradePubSub(txn executor.TxnExecutor) (err error) {
	accNameInfoMap, _, err := pubsub.GetAccounts(txn)
	if err != nil {
		return
	}

	// GetAllPubInfos returns map[pubAccountName#pubName] -> pubInfo
	allPubInfos, err := versions.GetAllPubInfos(txn, accNameInfoMap)
	if err != nil {
		return
	}

	// getPubSubscribedInfos returns map[pubAccountName#pubName] -> subscribedInfos
	pubSubscribedInfos, err := getPubSubscribedInfos(txn)
	if err != nil {
		return
	}

	getSubAccountIds := func(pubInfo *pubsub.PubInfo, pubAccountName string) (subAccountIds []int32) {
		if pubInfo.SubAccountsStr == pubsub.AccountAll {
			for _, accInfo := range accNameInfoMap {
				if accInfo.Name == pubAccountName {
					continue
				}

				subAccountIds = append(subAccountIds, accInfo.Id)
			}
		} else {
			for _, accName := range pubInfo.GetSubAccountNames() {
				if _, ok := accNameInfoMap[accName]; ok {
					subAccountIds = append(subAccountIds, accNameInfoMap[accName].Id)
				}
			}
		}
		return
	}

	// pub1: allPubInfos && ~pubSubscribedInfos
	// pub2: allPubInfos && pubSubscribedInfos
	// pub3: ~allPubInfos && pubSubscribedInfos
	var pub1, pub2, pub3 []string
	for key := range allPubInfos {
		if _, ok := pubSubscribedInfos[key]; ok {
			pub2 = append(pub2, key)
		} else {
			pub1 = append(pub1, key)
		}
	}
	for key := range pubSubscribedInfos {
		if _, ok := allPubInfos[key]; !ok {
			pub3 = append(pub3, key)
		}
	}

	// for pubs in:
	// 	   in pub1 -> nil, nil, StatusNormal
	//     in pub2 ->
	//         a1 := authorized sub_accounts
	//         a2 := subscribed_infos
	// 	  	   for account:
	//             1. in a1 and in a2 -> sub_name, sub_time, StatusDeleted
	//             2. in a1 and not in a2 -> nil, nil, StatusNormal
	//             3. not in a1 and in a2 -> StatusNotAuthorized
	//     int pub3 -> sub_name, sub_time, StatusDeleted
	for _, pubKey := range pub1 {
		split := strings.Split(pubKey, "#")
		pubAccountName, pubName := split[0], split[1]
		pubInfo := allPubInfos[pubKey]

		for _, subAccountId := range getSubAccountIds(pubInfo, pubAccountName) {
			subInfo := &pubsub.SubInfo{
				SubAccountId:   subAccountId,
				PubAccountName: pubAccountName,
				PubName:        pubName,
				PubDbName:      pubInfo.DbName,
				PubTables:      pubInfo.TablesStr,
				PubTime:        pubInfo.CreateTime,
				PubComment:     pubInfo.Comment,
				Status:         pubsub.SubStatusNormal,
			}
			insertSubsSql := generateInsertSql(subInfo)
			if _, err = txn.Exec(insertSubsSql, executor.StatementOption{}); err != nil {
				return
			}
		}
	}

	for _, pubKey := range pub2 {
		split := strings.Split(pubKey, "#")
		pubAccountName, pubName := split[0], split[1]
		pubInfo := allPubInfos[pubKey]
		subscribedInfos := pubSubscribedInfos[pubKey]

		subAccountIds := getSubAccountIds(pubInfo, pubAccountName)
		for _, subAccountId := range subAccountIds {
			subInfo := &pubsub.SubInfo{
				SubAccountId:   subAccountId,
				SubName:        "",
				SubTime:        "",
				PubAccountName: pubAccountName,
				PubName:        pubName,
				PubDbName:      pubInfo.DbName,
				PubTables:      pubInfo.TablesStr,
				PubTime:        pubInfo.CreateTime,
				PubComment:     pubInfo.Comment,
				Status:         pubsub.SubStatusNormal,
			}

			idx := slices.IndexFunc(subscribedInfos, func(info *pubsub.SubInfo) bool {
				return info.SubAccountId == subAccountId
			})
			if idx != -1 {
				subInfo.SubName = subscribedInfos[idx].SubName
				subInfo.SubTime = subscribedInfos[idx].SubTime
			}

			insertSubsSql := generateInsertSql(subInfo)
			if _, err = txn.Exec(insertSubsSql, executor.StatementOption{}); err != nil {
				return
			}
		}

		for _, subscribedInfo := range subscribedInfos {
			idx := slices.IndexFunc(subAccountIds, func(id int32) bool {
				return id == subscribedInfo.SubAccountId
			})
			if idx != -1 {
				continue
			}

			subInfo := &pubsub.SubInfo{
				SubAccountId:   subscribedInfo.SubAccountId,
				SubName:        subscribedInfo.SubName,
				SubTime:        subscribedInfo.SubTime,
				PubAccountName: pubAccountName,
				PubName:        pubName,
				PubDbName:      pubInfo.DbName,
				PubTables:      pubInfo.TablesStr,
				PubTime:        pubInfo.CreateTime,
				PubComment:     pubInfo.Comment,
				Status:         pubsub.SubStatusNotAuthorized,
			}

			insertSubsSql := generateInsertSql(subInfo)
			if _, err = txn.Exec(insertSubsSql, executor.StatementOption{}); err != nil {
				return
			}
		}
	}

	for _, pubKey := range pub3 {
		split := strings.Split(pubKey, "#")
		pubAccountName, pubName := split[0], split[1]
		subscribedInfos := pubSubscribedInfos[pubKey]

		for _, subscribedInfo := range subscribedInfos {
			subInfo := &pubsub.SubInfo{
				SubAccountId:   subscribedInfo.SubAccountId,
				SubName:        subscribedInfo.SubName,
				SubTime:        subscribedInfo.SubTime,
				PubAccountName: pubAccountName,
				PubName:        pubName,
				Status:         pubsub.SubStatusDeleted,
			}

			insertSubsSql := generateInsertSql(subInfo)
			if _, err = txn.Exec(insertSubsSql, executor.StatementOption{}); err != nil {
				return
			}
		}
	}

	// upgrade mo_pubs.table_list: "" ->  "*"
	for _, accountInfo := range accNameInfoMap {
		sql := "update mo_catalog.mo_pubs set table_list = '*'"
		if _, err = txn.Exec(sql, executor.StatementOption{}.WithAccountID(uint32(accountInfo.Id))); err != nil {
			return
		}
	}
	return
}
