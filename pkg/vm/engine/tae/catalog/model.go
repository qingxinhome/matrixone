// Copyright 2021 Matrix Origin
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

package catalog

import (
	"strings"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	PhyAddrColumnName    = objectio.PhysicalAddr_Attr
	PhyAddrColumnComment = "Physical address"

	TenantSysID = uint32(0)
)

var SystemDBSchema *Schema
var SystemTableSchema *Schema
var SystemColumnSchema *Schema

const (
	ModelSchemaName   = "_ModelSchema"
	ModelAttrET       = "ET"
	ModelAttrID       = "ID"
	ModelAttrName     = "NAME"
	ModelAttrTS       = "TS"
	ModelAttrOpT      = "OPT"
	ModelAttrLogIdx   = "LOGIDX"
	ModelAttrInfo     = "INFO"
	ModelAttrParentID = "PARENTID"
)

func init() {

	var err error

	defs := pkgcatalog.NewDefines()
	SystemDBSchema, err = DefsToSchema(pkgcatalog.MO_DATABASE, defs.MoDatabaseTableDefs)
	if err != nil {
		panic(err)
	}

	SystemTableSchema, err = DefsToSchema(pkgcatalog.MO_TABLES, defs.MoTablesTableDefs)
	if err != nil {
		panic(err)
	}

	SystemColumnSchema, err = DefsToSchema(pkgcatalog.MO_COLUMNS, defs.MoColumnsTableDefs)
	if err != nil {
		panic(err)
	}
}

func DefsToSchema(name string, defs []engine.TableDef) (schema *Schema, err error) {
	schema = NewEmptySchema(name)
	schema.CatalogVersion = pkgcatalog.CatalogVersion_Curr
	var pkeyColName string
	for _, def := range defs {
		switch defVal := def.(type) {
		case *engine.ConstraintDef:
			primaryKeyDef := defVal.GetPrimaryKeyDef()
			if primaryKeyDef != nil {
				pkeyColName = primaryKeyDef.Pkey.PkeyColName
				break
			}
		}
	}
	for _, def := range defs {
		switch defVal := def.(type) {
		case *engine.AttributeDef:
			if strings.EqualFold(pkeyColName, defVal.Attr.Name) {
				if err = schema.AppendSortColWithAttribute(defVal.Attr, 0, true); err != nil {
					return
				}
			} else if defVal.Attr.ClusterBy {
				if err = schema.AppendSortColWithAttribute(defVal.Attr, 0, false); err != nil {
					return
				}
			} else {
				if err = schema.AppendColWithAttribute(defVal.Attr); err != nil {
					return
				}
			}

		case *engine.PropertiesDef:
			for _, property := range defVal.Properties {
				switch strings.ToLower(property.Key) {
				case pkgcatalog.SystemRelAttr_Comment:
					schema.Comment = property.Value
				case pkgcatalog.SystemRelAttr_Kind:
					schema.Relkind = property.Value
				case pkgcatalog.SystemRelAttr_CreateSQL:
					schema.Createsql = property.Value
				case pkgcatalog.PropSchemaExtra:
					schema.Extra = api.MustUnmarshalTblExtra([]byte(property.Value))
				default:
				}
			}

		case *engine.PartitionDef:
			schema.Partitioned = defVal.Partitioned
			schema.Partition = defVal.Partition
		case *engine.ViewDef:
			schema.View = defVal.View
		case *engine.CommentDef:
			schema.Comment = defVal.Comment
		case *engine.ConstraintDef:
			schema.Constraint, err = defVal.MarshalBinary()
			if err != nil {
				return nil, err
			}
		default:
			// We will not deal with other cases for the time being
		}
	}
	if err = schema.Finalize(false); err != nil {
		return
	}
	return
}

func SchemaToDefs(schema *Schema) (defs []engine.TableDef, err error) {
	if schema.Comment != "" {
		commentDef := new(engine.CommentDef)
		commentDef.Comment = schema.Comment
		defs = append(defs, commentDef)
	}

	if schema.Partitioned > 0 || schema.Partition != "" {
		partitionDef := new(engine.PartitionDef)
		partitionDef.Partitioned = schema.Partitioned
		partitionDef.Partition = schema.Partition
		defs = append(defs, partitionDef)
	}

	if schema.View != "" {
		viewDef := new(engine.ViewDef)
		viewDef.View = schema.View
		defs = append(defs, viewDef)
	}

	if len(schema.Constraint) > 0 {
		c := new(engine.ConstraintDef)
		if err := c.UnmarshalBinary(schema.Constraint); err != nil {
			return nil, err
		}
		defs = append(defs, c)
	}

	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		attr, err := AttrFromColDef(col)
		if err != nil {
			return nil, err
		}
		defs = append(defs, &engine.AttributeDef{Attr: *attr})
	}
	pro := new(engine.PropertiesDef)
	pro.Properties = append(pro.Properties, engine.Property{
		Key:   pkgcatalog.SystemRelAttr_Kind,
		Value: string(schema.Relkind),
	})
	if schema.Createsql != "" {
		pro.Properties = append(pro.Properties, engine.Property{
			Key:   pkgcatalog.SystemRelAttr_CreateSQL,
			Value: schema.Createsql,
		})
	}
	pro.Properties = append(pro.Properties, engine.Property{
		Key:   pkgcatalog.PropSchemaExtra,
		Value: string(api.MustMarshalTblExtra(schema.Extra)),
	})
	defs = append(defs, pro)

	return
}

func AttrFromColDef(col *ColDef) (attrs *engine.Attribute, err error) {
	var defaultVal *plan.Default
	if len(col.Default) > 0 {
		defaultVal = &plan.Default{}
		if err := types.Decode(col.Default, defaultVal); err != nil {
			return nil, err
		}
	}

	var onUpdate *plan.OnUpdate
	if len(col.OnUpdate) > 0 {
		onUpdate = new(plan.OnUpdate)
		if err := types.Decode(col.OnUpdate, onUpdate); err != nil {
			return nil, err
		}
	}

	attr := &engine.Attribute{
		Name:          col.Name,
		Type:          col.Type,
		Primary:       col.IsPrimary(),
		IsHidden:      col.IsHidden(),
		IsRowId:       col.IsPhyAddr(),
		Comment:       col.Comment,
		Default:       defaultVal,
		OnUpdate:      onUpdate,
		AutoIncrement: col.IsAutoIncrement(),
		ClusterBy:     col.IsClusterBy(),
	}
	return attr, nil
}
