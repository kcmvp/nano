package nano

import (
	"github.com/kcmvp/nano/internal"
	"slices"
)

// Register registers a new name with a unique namespace.
// It ensures that both the name and the namespace are unique across all registered SchemaSpace.
// If the name or namespace already exists, it returns an error.
// The name and namespace are stored in the internal key-value _store.
// Upon successful registration, a new DB instance is created and stored in the registry.
// When save a key/value pair, the key will be prefixed the namespace automatically.
func Register(schemas ...*Schema) error {
	var rawSchemas []*internal.Schema
	var nameSet []string
	var namespaceSet []string
	for _, schema := range schemas {
		if slices.Contains(nameSet, schema.internalSchema.Name) {
			return internal.ErrDbExists
		} else if slices.Contains(namespaceSet, schema.internalSchema.Namespace) {
			return internal.ErrNamespaceExists
		}
		nameSet = append(nameSet, schema.internalSchema.Name)
		namespaceSet = append(namespaceSet, schema.internalSchema.Namespace)
		rawSchemas = append(rawSchemas, schema.internalSchema)
	}
	return internal.StoreImpl().Registry(rawSchemas...)
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
