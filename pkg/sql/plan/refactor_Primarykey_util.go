package plan

import (
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
)

func BuildComPriKeyColumnName(s []string) (string, error) {
	var name string
	newUUID, err := uuid.NewUUID()
	if err != nil {
		return name, err
	}
	name = catalog.PrefixPriColName + newUUID.String()
	return name, nil
}

// this func can't judge index table col is compound or not
func IsComPrimaryKeyColumn(s string) bool {
	if len(s) < len(catalog.PrefixPriColName) {
		return false
	}
	return s[0:len(catalog.PrefixPriColName)] == catalog.PrefixPriColName
}
