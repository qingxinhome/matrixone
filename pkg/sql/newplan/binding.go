package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func NewBinding(tag int32, nodeId int32, tableName string, cols []string, types []*plan.Type) *Binding {
	binding := &Binding{
		tag:       tag,
		nodeId:    nodeId,
		tableName: tableName,
		columns:   cols,
		colTypes:  types,
		refCnts:   make([]uint, len(cols)),
	}
	binding.colIdByName = make(map[string]int32)
	// Check whether the column names of the table are duplicate
	for i, col := range cols {
		if _, ok := binding.colIdByName[col]; ok {
			binding.colIdByName[col] = AmbiguousName
		} else {
			binding.colIdByName[col] = int32(i)
		}
	}
	return binding
}
