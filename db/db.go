package db

import (
	"github.com/kcmvp/nano/internal"
	"github.com/samber/mo"
	"time"
)

type Schema struct {
	name   string
	prefix string
}

// Register registers a new name with a unique namespace.
// It ensures that both the name and the namespace are unique across all registered SchemaSpace.
// If the name or namespace already exists, it returns an error.
// The name and namespace are stored in the internal key-value _store.
// Upon successful registration, a new DB instance is created and stored in the registry.
// When save a key/value pair, the key will be prefixed the namespace automatically.
func Register(name string, schema rune) error {
	return internal.IS().Registry(name, string(schema))
}

func All() []Schema {
	panic("return all dbs")
}

func Set(dbName string) func(key, value string) mo.Result[string] {
	return func(key, value string) mo.Result[string] {
		panic("todo")
	}
}

func SetWithTTL(dbName string) func(key, value string, ttl time.Duration) mo.Result[string] {
	return func(key, value string, ttl time.Duration) mo.Result[string] {
		panic("todo")
	}
}

func Get(dbName string) func(key string) mo.Result[string] {
	return func(key string) mo.Result[string] {
		panic("")
	}
}
