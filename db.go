package nano

import (
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
	"strconv"
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
	// For example, if namespace is "u", all keys for this DB will be stored as "u:key".
	// namespace is typically a single character or a short string to minimize storage overhead.
	namespace string

	// schema stores the mapping between original JSON field names and their shortened representations.
	// This map is used for optimizing storage by replacing long field names with shorter ones (e.g., "firstName" -> "f1").
	// It's persisted to buntdb to ensure consistency across application restarts.
	// The key is the original field name, and the value is the shortened field name.
	schema map[string]string
	// mu is a RWMutex to protect concurrent access to the DB's fields,
	// especially during schema updates or data access.
	mu sync.RWMutex
}

// MarshalJSON implements the json.Marshaler interface for DB.
func (db *DB) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Namespace string            `json:"namespace"`
		Version   string            `json:"version"`
		Schema    map[string]string `json:"schema"`
	}{
		Namespace: db.namespace,
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

// dbKey constructs the internal key used to store the DB's schema and metadata
// within the buntdb instance. This key is prefixed with `_schema:` to
// distinguish it from user data and includes the database's name to make it unique.
// Example: "_schema:my_db_name"

func (db *DB) dbKey() string {
	return fmt.Sprintf("%s%s%s", schemaSpacePrefix, keySeparator, db.name)
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
		// if it is a valid JSON, we will shorten it
		if gjson.Valid(value) {
			// If the value is a valid JSON, we shorten it using our schema
			result := db.shorten(tx, value)
			fmt.Println(result)
		}
		pre, _, err = tx.Set(db.Key(key), value, opt)
		return err
	})
	return lo.If(err != nil, mo.Err[string](err)).Else(mo.Ok(pre))
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

// shortKey generates a unique short key for a new field in the schema.
// It prioritizes single-character lowercase letters (a-z).
// If all single characters are used, it moves to two-character keys,
// starting with a lowercase letter and followed by any character from `validCharSet` (a-z, 0-9).
// This ensures a compact and deterministic key generation.
func (db *DB) shortKey() string {
	usedShortKeys := make(map[string]struct{}, len(db.schema))
	for _, shortKey := range db.schema {
		usedShortKeys[shortKey] = struct{}{}
	}
	// Policy 1: Find the first available single-character key.
	// singleCharSet is already sorted 'a' through 'z'.
	for _, char := range lo.LowerCaseLettersCharset {
		cs := string(char)
		if _, ok := usedShortKeys[cs]; !ok {
			return cs
		}
	}
	// Policy 2: All single characters are used. Find the first available two-character key.
	// This deterministic loop is more robust and predictable than random generation.
	for _, c1 := range lo.LowerCaseLettersCharset {
		for _, c2 := range validCharSet {
			key := string(c1) + string(c2)
			if _, ok := usedShortKeys[key]; !ok {
				return key
			}
		}
	}
	// Fallback for the highly unlikely case that all 962 (26 + 26*36) keys are used.
	// This prevents an infinite loop and ensures the system can continue.
	return fmt.Sprintf("k%d", len(usedShortKeys))
}

func (db *DB) shorten(tx *buntdb.Tx, payload string) mo.Result[string] {
	db.mu.Lock()
	defer db.mu.Unlock()
	longKeys := lo.Map(gjson.Get(payload, "@keys").Array(), func(item gjson.Result, _ int) string {
		return item.Str
	})
	if len(longKeys) == 0 {
		return mo.Ok(payload)
	}
	// 1. Detect if the schema needs to evolve by checking for new keys.
	var schemaEvolved bool
	for _, key := range longKeys {
		if _, ok := db.schema[key]; !ok {
			db.schema[key] = db.shortKey()
			schemaEvolved = true
		}
	}
	if schemaEvolved {
		data, err := db.MarshalJSON()
		if err != nil {
			return mo.Err[string](fmt.Errorf("failed to marshal updated db schema: %w", err))
		}
		if _, _, err := tx.Set(db.dbKey(), string(data), nil); err != nil {
			return mo.Err[string](fmt.Errorf("failed to persist updated db schema: %w", err))
		}
		registry.Store(db.name, db)
	}

	// 2. Perform the transformation without a full unmarshal/marshal cycle.
	var builder strings.Builder
	// Pre-allocating the builder's capacity close to the original payload size
	// can prevent multiple re-allocations for large JSON objects.
	builder.Grow(len(payload))
	builder.WriteString("{")
	var transformErr error
	first := true
	// Use gjson.Parse().ForEach to iterate through the object efficiently.
	gjson.Parse(payload).ForEach(func(key, value gjson.Result) bool {
		shortKey, ok := db.schema[key.String()]
		if !ok {
			transformErr = fmt.Errorf("internal error: schema missing key '%s' after evolution", key.String())
			return false // Stop iterating on error
		}
		if !first {
			builder.WriteString(",")
		}
		first = false
		// Write the shortened key, ensuring it's a valid JSON string literal.
		builder.WriteString(strconv.Quote(shortKey))
		builder.WriteString(":")
		// Write the raw value directly from the original payload.
		builder.WriteString(value.Raw)
		return true // Continue iterating
	})

	if transformErr != nil {
		return mo.Err[string](transformErr)
	}
	builder.WriteString("}")
	return mo.Ok(builder.String())
}

// Raw represents a raw JSON value retrieved from the database.
// It's a wrapper around the underlying JSON parser's result type
// to provide a stable, implementation-agnostic API.
type Raw gjson.Result

// Exists returns true if the value exists.
func (r Raw) Exists() bool {
	return gjson.Result(r).Exists()
}

// String returns the value as a string.
func (r Raw) String() string {
	return gjson.Result(r).String()
}

// Int returns the value as an int64.
func (r Raw) Int() int64 {
	return gjson.Result(r).Int()
}

// Float returns the value as a float64.
func (r Raw) Float() float64 {
	return gjson.Result(r).Float()
}

// Bool returns the value as a boolean.
func (r Raw) Bool() bool {
	return gjson.Result(r).Bool()
}

// Value returns the value as an interface{}.
func (r Raw) Value() interface{} {
	return gjson.Result(r).Value()
}

// GetPath retrieves a specific value from a JSON object stored in the database
// using a dot-notation path. This is highly efficient for accessing single properties
// as it avoids the overhead of rehydrating the entire JSON object.
// Example: db.GetPath("1", "profile.name")
func (db *DB) GetPath(key, path string) mo.Result[Raw] {
	// 1. Translate the user-friendly path to the shortened path.
	// We need a read lock to safely access the schema.
	db.mu.RLock()
	pathSegments := strings.Split(path, ".")
	shortenedSegments := make([]string, len(pathSegments))
	for i, segment := range pathSegments {
		// If the segment is a key in our schema, translate it.
		// Otherwise, assume it's an array index or a key that isn't shortened.
		if shortKey, ok := db.schema[segment]; ok {
			shortenedSegments[i] = shortKey
		} else {
			shortenedSegments[i] = segment
		}
	}
	shortenedPath := strings.Join(shortenedSegments, ".")
	db.mu.RUnlock() // Release the lock as soon as we're done with the schema.

	// 2. Retrieve the raw, shortened payload from the database.
	var v string
	var err error
	err = _store.View(func(tx *buntdb.Tx) error {
		v, err = tx.Get(db.Key(key))
		return err
	})

	if err != nil {
		return mo.Err[Raw](err)
	}

	// 3. Use gjson on the shortened payload with the shortened path.
	result := gjson.Get(v, shortenedPath)
	return mo.Ok(Raw(result))
}

// rehydrate transforms a shortened JSON payload back into its original, readable form.
// It is the inverse of the shorten function.
func (db *DB) rehydrate(payload string) mo.Result[string] {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(db.schema) == 0 || !gjson.Valid(payload) {
		return mo.Ok(payload) // No schema or not a valid JSON object, so nothing to do.
	}

	// Create a reverse map for efficient lookups (short -> long).
	reverseSchema := make(map[string]string, len(db.schema))
	for long, short := range db.schema {
		reverseSchema[short] = long
	}

	var builder strings.Builder
	builder.Grow(len(payload))
	builder.WriteString("{")

	var transformErr error
	first := true
	gjson.Parse(payload).ForEach(func(key, value gjson.Result) bool {
		longKey, ok := reverseSchema[key.String()]
		if !ok {
			// A key exists that is not in our schema. This could be a mixed payload
			// or an error. The safest action is to return the payload as-is
			// rather than a partially rehydrated one.
			transformErr = errors.New("payload contains keys not in schema, cannot rehydrate")
			return false // Stop iterating
		}

		if !first {
			builder.WriteString(",")
		}
		first = false

		builder.WriteString(strconv.Quote(longKey))
		builder.WriteString(":")
		builder.WriteString(value.Raw)
		return true
	})

	if transformErr != nil {
		// If we stopped because of an unknown key, it's safer to return the original payload.
		return mo.Ok(payload)
	}

	builder.WriteString("}")
	return mo.Ok(builder.String())
}
