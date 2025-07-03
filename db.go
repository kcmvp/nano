package nano

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	schemaSpacePrefix = "_schema"
	keySeparator      = ":"
)

var registry sync.Map
var _store *buntdb.DB
var singleCharSet = lo.Map(lo.LowerCaseLettersCharset, func(item rune, _ int) string {
	return string(item)
})
var validCharSet = append(lo.LowerCaseLettersCharset, lo.NumbersCharset...)

func init() {
	u, e := user.Current()
	if e != nil {
		log.Fatalf("failed to get current user %v", e)
	}
	dir := filepath.Join(u.HomeDir, ".nino")
	e = os.MkdirAll(dir, 0755)
	if e != nil {
		log.Fatalf("failed to create directory %v", e)
	}
	_store, e = buntdb.Open(filepath.Join(dir, "nano.db"))
	if e != nil {
		log.Fatalf("failed to open database %v", e)
	}
	// reload existing schemas from the store
	_ = _store.View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(SchemaSpace(), func(key, value string) bool {
			name := key[len(schemaSpacePrefix)+len(keySeparator):]
			var db DB
			_ = json.Unmarshal([]byte(value), &db)
			registry.Store(name, &db)
			return true
		})
	})
}

// SchemaSpace returns the schema string for system-level keys.
// These keys are used internally to manage registered databases and their prefixes.
// The format is "_schema:*", which matches all keys starting with "_schema:".
func SchemaSpace() string {
	return fmt.Sprintf("%s%s*", schemaSpacePrefix, keySeparator)
}

type DB struct {
	// name is the unique identifier for this database instance.
	name string
	// namespace is a unique prefix for all keys belonging to this DB.
	// It helps in isolating data for different logical databases within the same buntdb instance.
	// For example, if namespace is "users", all keys for this DB will be stored as "users:key".
	namespace string

	// version is the version of the database schema.
	// It can be used to manage schema migrations or versioning of the database structure.
	version string

	// schema stores the mapping between original JSON field names and their shortened representations.
	// This map is used for optimizing storage by replacing long field names with shorter ones (e.g., "firstName" -> "f1").
	// It's persisted to buntdb to ensure consistency across application restarts.
	// The key is the original field name, and the value is the shortened field name.
	schema map[string]string

	s2l map[string]string
}

// MarshalJSON implements the json.Marshaler interface for DB.
func (db *DB) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Namespace string            `json:"namespace"`
		Version   string            `json:"version"`
		Schema    map[string]string `json:"schema"`
	}{
		Namespace: db.namespace,
		Version:   db.version,
		Schema:    db.schema,
	})
}

// UnmarshalJSON implements the json.Unmarshaler interface for DB.
func (db *DB) UnmarshalJSON(data []byte) error {
	aux := &struct {
		Namespace string            `json:"namespace"`
		Version   string            `json:"version"`
		Schema    map[string]string `json:"schema"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	db.namespace = aux.Namespace
	db.version = aux.Version
	db.schema = aux.Schema
	return nil
}

// Namespace returns the namespace of the database.
// The namespace is a unique prefix for all keys belonging to this DB,
// ensuring data isolation within the same buntdb instance.
func (db *DB) Namespace() string {
	return db.namespace
}

// Name returns the name of the database.
// This name is a unique identifier used to retrieve the database instance from the registry.
// It's set during the registration process.
func (db *DB) Name() string {
	return db.name
}

// Schema returns the schema map of the database.
// This map contains the mapping between original JSON field names and their shortened representations.
// It's used for optimizing storage by replacing long field names with shorter ones.
// The key is the original field name, and the value is the shortened field name.
func (db *DB) Schema() map[string]string {
	return db.schema
}

// Register registers a new name with a unique namespace.
// It ensures that both the name and the namespace are unique across all registered SchemaSpace.
// If the name or namespace already exists, it returns an error.
// The name and namespace are stored in the internal key-value _store.
// Upon successful registration, a new DB instance is created and stored in the registry.
// When save a key/value pair, the key will be prefixed the namespace automatically.
func Register(name string, prefix rune) error {
	db := &DB{name: name, namespace: string(prefix), schema: make(map[string]string)}
	return _store.Update(func(tx *buntdb.Tx) error {
		// Step 1: Check for name existence using the most efficient method.
		if _, err := tx.Get(db.dbKey()); !errors.Is(err, buntdb.ErrNotFound) {
			// An error occurred. It's either nil (key found) or a real DB error.
			if err == nil {
				return fmt.Errorf("name '%s' already exists", name)
			}
			// A different database error occurred, so we must stop and propagate it.
			return err
		}
		// Check for existing namespace
		var exists bool
		if err := tx.AscendKeys(SchemaSpace(), func(_, value string) bool {
			if value == db.Namespace() {
				exists = true
				return false // Stop iteration if found
			}
			return true
		}); err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("namespace '%c' already exists", prefix)
		}
		// If not exists, set the new schema and store in registry
		data, _ := db.MarshalJSON()
		_, _, err := tx.Set(db.dbKey(), string(data), nil)
		if err == nil {
			// Only store in registry if the DB operation was successful
			registry.Store(name, db)
		}
		return err
	})
}

// With retrieves a registered database by its name.
// It returns a mo.Result which is an Option type that can be either Ok(*DB) if the database is found,
// or Err(error) if no database with the given name is registered.
// The function iterates through the internal registry to find the matching DB instance.
func With(dbName string) mo.Result[*DB] {
	db, ok := registry.Load(dbName)
	return lo.If(ok, mo.Ok(db.(*DB))).Else(mo.Errf[*DB]("no such database %s", dbName))
}

// Key constructs a full key for a given database key.
// It prefixes the provided key with the database's namespace and a separator.
// This ensures that keys are unique within the buntdb instance for each logical database.
func (db *DB) Key(key string) string {
	return fmt.Sprintf("%s%s%s", db.namespace, keySeparator, key)
}

// dbKey constructs a key for storing the DB's metadata in buntdb.
// This key is used internally to persist the DB's namespace and schema.
// The format is "namespace:dbName".
func (db *DB) dbKey() string {
	return fmt.Sprintf("%s%s%s", db.namespace, keySeparator, db.name)
}

// Set stores a key-value pair in the database.
// The key is automatically prefixed with the database's schema namespace.
// It returns a mo.Result[string] which is Ok(the previous value associated with the key)
// if the operation is successful, or Err(error) if an error occurs.
func (db *DB) Set(key, value string) mo.Result[string] {
	return db.SetWithTTL(key, value, -1)
}

// SetWithTTL sets a key-value pair in the database with an optional time-to-live (TTL).
// The key is automatically prefixed with the database's schema namespace.
// If ttl is greater than 0, the key-value pair will expire after the specified duration.
// It returns a mo.Result[string] which is Ok(the previous value associated with the key)
// if the operation is successful, or Err(error) if an error occurs.
// The previous value will be an empty string if the key did not exist.
func (db *DB) SetWithTTL(key, value string, ttl time.Duration) mo.Result[string] {
	var err error
	var pre string
	opt := lo.If(ttl > 0, &buntdb.SetOptions{Expires: true, TTL: ttl}).Else(nil)
	_ = _store.Update(func(tx *buntdb.Tx) error {
		// if its a valid JSON, we will shorten it
		if gjson.Valid(value) {
			// If the value is a valid JSON, we shorten it using our schema
			value = db.shorten(tx, value)
		}
		pre, _, err = tx.Set(db.Key(key), value, opt)
		return err
	})
	return lo.If(err != nil, mo.Err[string](err)).Else(mo.Ok(pre))
}

func version(props []string) string {
	slices.Sort(props)
	keys := strings.Join(props, ",")
	return fmt.Sprintf("%x", md5.Sum([]byte(keys)))
}

func (db *DB) Get(key string) mo.Result[string] {
	var v string
	var err error
	_ = _store.View(func(tx *buntdb.Tx) error {
		v, err = tx.Get(db.Key(key))
		return err
	})
	return lo.If(err != nil, mo.Err[string](err)).Else(mo.Ok(v))
}

func (db *DB) shorten(tx *buntdb.Tx, payload string) string {
	keys := lo.Map(gjson.Get(payload, "@keys").Array(), func(item gjson.Result, _ int) string {
		return item.Str
	})
	if version(keys) != db.version {
		single := lo.Without(singleCharSet, lo.Values(db.schema)...)
		lo.ForEach(keys, func(key string, _ int) {
			if _, ok := db.schema[key]; !ok {
				chosen, ok := lo.Last(single)
				if ok {
					single = lo.DropRight(single, 1)
				} else {
					_, _ = lo.AttemptWhile(-1, func(i int) (error, bool) {
						chosen = fmt.Sprintf("%s%s", lo.RandomString(1, lo.LowerCaseLettersCharset), lo.RandomString(1, validCharSet))
						return nil, !lo.Contains(lo.Values(db.schema), chosen)
					})
				}
				db.schema[key] = chosen
			}
		})
	}
	db.version = version(keys)
	data, _ := db.MarshalJSON()
	_, _, err := tx.Set(db.dbKey(), string(data), nil)
	if err != nil {
		log.Printf("failed to update schema for db %s: %v", db.name, err)
		return payload // Return original payload if schema update fails
	}
	registry.Store(db.name, db)

	// Now, apply the shortening to the payload
	var m map[string]interface{}
	_ = json.Unmarshal([]byte(payload), &m)
	shortenedMap := make(map[string]interface{})
	for k, v := range m {
		if shortKey, ok := db.schema[k]; ok {
			shortenedMap[shortKey] = v
		} else {
			// This case should ideally not happen if schema is updated correctly
			shortenedMap[k] = v
		}
	}
	shortenedPayload, _ := json.Marshal(shortenedMap)
	return string(shortenedPayload)
	// update db.version
	// persistent the schema to the store
	// refresh the registry
}
