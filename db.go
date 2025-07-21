package nano

import (
	"github.com/kcmvp/nano/internal"
	"github.com/samber/lo"
	"slices"
)

// Register registers a new name with a unique namespace.
// It ensures that both the name and the namespace are unique across all registered SchemaSpace.
// If the name or namespace already exists, it returns an error.
// The name and namespace are stored in the internal key-value _store.
// Upon successful registration, a new DB instance is created and stored in the registry.
// When save a key/value pair, the key will be prefixed the namespace automatically.
func Register(schemas ...*Schema) error {
	var nameSet []string
	var blueprints []*internal.Schema
	for _, schema := range schemas {
		lo.Assertf(slices.Contains(nameSet, schema.name), "duplicated schema %s", schema.name)
		nameSet = append(nameSet, schema.name)
		blueprints = append(blueprints, internal.Blueprint(schema.name, string(schema.id), string(schema.createdAt), string(schema.updatedAt),
			lo.Map(lo.Keys(schema.status), func(item JsonProperty, _ int) string {
				return string(item)
			})...))
	}
	return internal.StoreImpl().Registry(blueprints...)
}

//
//func Set(dbName
//string) func (key, value string) mo.Result[string] {
//	return func(key, value string) mo.Result[string] {
//		return SetWithTTL(dbName)(key, value, 0)
//	}
//}
//
//func SetWithTTL(dbName
//string) func (key, value string, ttl time.Duration) mo.Result[string] {
//	return func(key, value string, ttl time.Duration) mo.Result[string] {
//		panic("@todo")
//	}
//}
//
//type Raw gjson.Result
//
//func Get(dbName
//string) func (key string) mo.Result[string] {
//	return func(key string) mo.Result[string] {
//		return internal.StoreImpl().Get(dbName, key)
//	}
//}
