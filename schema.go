package nano

import (
	"github.com/kcmvp/nano/internal"
)

type Schema struct {
	internalSchema *internal.Schema
}

func NameSpace(name string, prefix rune) *Schema {
	return &Schema{
		internalSchema: &internal.Schema{
			Name:      name,
			Namespace: string(prefix),
		},
	}
}

func (schema *Schema) DefaultProperties() *Schema {
	schema.internalSchema.PKProp = "id"
	schema.internalSchema.CreatedAtProp = "created_at"
	schema.internalSchema.UpdatedAtProp = "updated_at"
	return schema
}

func (schema *Schema) PK(prop string) *Schema {
	schema.internalSchema.PKProp = prop
	return schema
}

func (schema *Schema) CreatedAt(prop string) *Schema {
	schema.internalSchema.CreatedAtProp = prop
	return schema
}

func (schema *Schema) UpdatedAt(prop string) *Schema {
	schema.internalSchema.UpdatedAtProp = prop
	return schema
}
