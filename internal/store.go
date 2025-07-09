package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kcmvp/nano"
	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/spf13/viper"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"log"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	schemaSpacePrefix = "_schema"
	keySeparator      = ":"
)

var validCharSet = append(lo.LowerCaseLettersCharset, lo.NumbersCharset...)

var store *Store
var once sync.Once
var cfg *viper.Viper
var storePath string

// Store is the main struct for interacting with the database.
// It encapsulates the buntdb instance and manages schemas for different data types.
// It is designed as a singleton, accessible via the IS() function.
type Store struct {
	impl    *buntdb.DB
	schemas sync.Map
}

func init() {
	cfg = viper.New()
	cfg.SetConfigName("app")
	cfg.SetConfigType("yaml")
	if executable, err := os.Executable(); err == nil {
		cfg.AddConfigPath(filepath.Dir(executable))
	} else {
		log.Fatalf("could not determine executable path, will not be searched %v", err)
	}
	cfg.AddConfigPath(".")
	cfg.AddConfigPath("../")
	cfg.AutomaticEnv()
	cfg.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // gitlab.token => GITLAB_TOKEN
	// Attempt to read the configuration file.
	if err := cfg.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if errors.As(err, &configFileNotFoundError) {
			slog.Info("config file not found, using environment variables or defaults")
		}
	}
}

// DataDir returns the path to the directory where Nano stores its data,
// including the BuntDB database file and configuration.
// This path is determined by the `nano.data` configuration setting,
// or defaults to `~/.nino` if not specified.
func DataDir() string {
	return storePath
}

func IS() *Store {
	once.Do(func() {
		storePath = cfg.GetString("nano.data")
		if len(storePath) < 0 {
			u, err := user.Current()
			if err != nil {
				log.Fatalf("failed to get current user %v", err)
			}
			storePath = filepath.Join(u.HomeDir, ".nino")
		}
		if err := os.MkdirAll(storePath, 0755); err != nil {
			log.Fatalf("failed to create directory %v", err)
		}
		data := filepath.Join(storePath, "nano.db")
		db, err := buntdb.Open(data)
		if err != nil {
			log.Fatalf("failed to open database %v", err)
		}
		store = &Store{impl: db, schemas: sync.Map{}}
		// reload existing schemas from the store
		store.loadSchemas()
	})
	return store
}

// Close closes the underlying buntdb database.
// It should be called when the application is shutting down.
func (store *Store) Close() error {
	if store.impl != nil {
		return store.impl.Close()
	}
	return nil
}

// loadSchemas loads all existing schemas from the database into the in-memory sync.Map.
// This ensures that the application's schema registry is consistent with the persisted state.
func (store *Store) loadSchemas() {
	_ = store.impl.View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(SchemaSpace(), func(key, value string) bool {
			schema := &Schema{Name: key[len(schemaSpacePrefix)+len(keySeparator):]}
			if err := json.Unmarshal([]byte(value), schema); err != nil {
				log.Printf("WARN: Corrupted schema for key '%s', skipping. Error: %v", key, err)
				return true // Continue to the next schema
			}
			store.schemas.Store(schema.Name, schema)
			return true
		})
	})
}

// Key returns the unique key for the schema in the database.
// This key is used to store and retrieve the schema definition itself.
// The format is "_schema:schemaName".
func (schema *Schema) Key() string {
	return SchemaKey(schema.Name)
}

// SchemaKey returns the unique key for a schema in the database.
func SchemaKey(name string) string {
	return fmt.Sprintf("%s%s%s", schemaSpacePrefix, keySeparator, name)
}

// Registry registers a new database schema with the store.
// It ensures that the `dbName` is unique and the `prefix` (namespace) is also unique
// across all registered schemas. If successful, the schema is persisted to the database
// and loaded into the in-memory schema registry.
func (store *Store) Registry(dbName string, prefix string) error {
	return store.impl.Update(func(tx *buntdb.Tx) error {
		// Step 1: Check for dbName existence using the most efficient method.
		schema := Schema{Name: dbName, Namespace: prefix, Mapping: make(map[string]string)}
		key := schema.Key()
		var err error
		if _, err = tx.Get(key); !errors.Is(err, buntdb.ErrNotFound) {
			// An error occurred. It's either nil (key found) or a real DB error.
			if err == nil {
				return fmt.Errorf("%w:'%s'", nano.ErrDbExists, dbName)
			}
			// A different database error occurred, so we must stop and propagate it.
			return err
		}
		// Check for existing namespace
		var exists bool
		if err = tx.AscendKeys(SchemaSpace(), func(_, value string) bool {
			exists = gjson.Get(value, "namespace").Str == prefix
			return !exists
		}); err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("%w:'%s'", nano.ErrSchemaExists, prefix)
		}
		// If not exists, set the new schema and store in registry
		data, _ := json.Marshal(&schema)
		if _, _, err = tx.Set(key, string(data), nil); err == nil {
			store.schemas.Store(schema.Name, &schema)
		}
		return err
	})
}

// Schema represents the structure and mapping rules for a specific "database"
// within the buntdb store. It defines how original field names are mapped to
// shorter, optimized keys for storage efficiency.
type Schema struct {

	// Name is the unique identifier for the schema (e.g., "users", "products").
	Name string `json:"-"`
	// Namespace is a unique prefix for all keys belonging to this DB.
	// It helps in isolating data for different logical databases within the same buntdb instance.
	// For example, if Namespace is "u", all keys for this DB will be stored as "u:key".
	// Namespace is typically a single character or a short string to minimize storage overhead.
	Namespace string `json:"namespace"`

	// Mapping stores the Mapping between original JSON field names and their shortened representations.
	// This map is used for optimizing storage by replacing long field names with shorter ones (e.g., "firstName" -> "f1").
	// It's persisted to buntdb to ensure consistency across application restarts.
	// The key is the original field Name, and the value is the shortened field Name.
	Mapping map[string]string `json:"mapping"`
	// mu is a RWMutex to protect concurrent access to the DB's fields,
	// especially during schema updates or data access.
	mu sync.RWMutex
}

// Clone creates a deep copy of the schema, allowing for safe modifications
// within a transaction without affecting the globally shared schema object.
func (schema *Schema) Clone() *Schema {
	schema.mu.RLock()
	defer schema.mu.RUnlock()
	// Create a new map and copy the key-value pairs.
	newMapping := make(map[string]string, len(schema.Mapping))
	for k, v := range schema.Mapping {
		newMapping[k] = v
	}
	return &Schema{
		Name:      schema.Name,
		Namespace: schema.Namespace,
		Mapping:   newMapping,
	}
}

// SchemaSpace returns the schema string for system-level keys.
// These keys are used internally to manage registered databases and their prefixes.
// The format is "_schema:*", which matches all keys starting with "_schema:".
func SchemaSpace() string {
	return fmt.Sprintf("%s%s*", schemaSpacePrefix, keySeparator)
}

// shortKey generates a unique short key for a new field in the schema.
// It prioritizes single-character lowercase letters (a-z).
// If all single characters are used, it moves to two-character keys,
// starting with a lowercase letter and followed by any character from `validCharSet` (a-z, 0-9).
// This ensures a compact and deterministic key generation.
func (schema *Schema) shortKey() string {
	usedShortKeys := lo.Keyify(lo.Values(schema.Mapping))
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

// shorten transforms a JSON payload by replacing original field names with their
// shorter, optimized representations based on the schema's mapping.
// If new fields are encountered, the schema's in-memory mapping is evolved.
// It returns the shortened JSON and a boolean indicating if the schema was evolved.
func (schema *Schema) shorten(payload string) (mo.Result[string], *Schema) {
	schemaEvolved := schema.Clone()
	longKeys := lo.Map(gjson.Get(payload, "@keys").Array(), func(item gjson.Result, _ int) string {
		return item.Str
	})
	if len(longKeys) == 0 {
		return mo.Ok(payload), schemaEvolved
	}
	slices.SortFunc(longKeys, func(a, b string) int {
		return len(a) - len(b)
	})
	for _, key := range longKeys {
		if len(key) == 1 {
			schemaEvolved.Mapping[key] = key
		} else if _, ok := schemaEvolved.Mapping[key]; !ok {
			schemaEvolved.Mapping[key] = schemaEvolved.shortKey()
		}
	}
	// 2. Perform the transformation without a full unmarshal/marshal cycle.
	var builder strings.Builder
	builder.Grow(len(payload))
	builder.WriteString("{")
	var transformErr error
	first := true
	gjson.Parse(payload).ForEach(func(key, value gjson.Result) bool {
		shortKey, ok := schemaEvolved.Mapping[key.String()]
		if !ok {
			transformErr = fmt.Errorf("internal error: schema missing key '%s' after evolution", key.String())
			return false // Stop iterating on error
		}
		if !first {
			builder.WriteString(",")
		}
		first = false
		builder.WriteString(strconv.Quote(shortKey))
		builder.WriteString(":")
		builder.WriteString(value.Raw)
		return true // Continue iterating
	})

	if transformErr != nil {
		return mo.Err[string](transformErr), nil
	}
	builder.WriteString("}")
	return mo.Ok(builder.String()), schemaEvolved
}

// rehydrate transforms a shortened JSON payload back into its original, readable form.
// It is the inverse of the shorten function.
func (schema *Schema) rehydrate(payload string) mo.Result[string] {
	schema.mu.RLock()
	defer schema.mu.RUnlock()

	if len(schema.Mapping) == 0 || !gjson.Valid(payload) {
		return mo.Ok(payload) // No schema or not a valid JSON object, so nothing to do.
	}

	// Create a reverse map for efficient lookups (short -> long).
	short2Long := lo.Invert(schema.Mapping)
	var builder strings.Builder
	builder.Grow(len(payload))
	builder.WriteString("{")
	var transformErr error
	first := true
	gjson.Parse(payload).ForEach(func(key, value gjson.Result) bool {
		longKey, ok := short2Long[key.String()]
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

// dataKey constructs the actual key used to store user data.
// It combines the schema's namespace with the user-provided key.
func (schema *Schema) dataKey(key string) string {
	return fmt.Sprintf("%s%s%s", schema.Namespace, keySeparator, key)
}

// Set saves a key-value pair for a given database.
// It automatically shortens the payload based on the database's schema.
// If the payload contains new keys, the schema is evolved atomically.
func (store *Store) Set(dbName, key, value string) error {
	return store.SetWithTTL(dbName, key, value, 0)
}

// SetWithTTL saves a key-value pair with a time-to-live (TTL).
// A TTL of 0 means the key will not expire.
func (store *Store) SetWithTTL(dbName, key, value string, ttl time.Duration) error {
	// 1. Load the original schema.
	val, ok := store.schemas.Load(dbName)
	if !ok {
		return fmt.Errorf("db '%s' is not registered", dbName)
	}
	schema := val.(*Schema)
	// 2. Perform the entire operation in a single, atomic transaction.
	var evolvedSchema *Schema
	var evolved bool
	err := store.impl.Update(func(tx *buntdb.Tx) error {
		// 3. Shorten the payload. This will also evolve schemaCopy's in-memory mapping.
		var shortenedPayloadResult mo.Result[string]
		shortenedPayloadResult, evolvedSchema = schema.shorten(value)
		if shortenedPayloadResult.IsError() {
			return shortenedPayloadResult.Error()
		}
		// 5. If the schema evolved, persist the updated schema copy to the database.
		evolved = len(evolvedSchema.Mapping) > len(schema.Mapping)
		if evolved {
			data, err := json.Marshal(evolvedSchema)
			if err != nil {
				return fmt.Errorf("failed to marshal updated db schema: %w", err)
			}
			if _, _, err := tx.Set(evolvedSchema.Key(), string(data), nil); err != nil {
				return fmt.Errorf("failed to persist updated db schema: %w", err)
			}
		}

		// 6. Set the user data in the transaction.
		opts := lo.IfF(ttl > 0, func() *buntdb.SetOptions {
			return &buntdb.SetOptions{Expires: true, TTL: ttl}
		}).Else(nil)
		_, _, err := tx.Set(evolvedSchema.dataKey(key), shortenedPayloadResult.MustGet(), opts)
		return err
	})

	if err == nil && evolved {
		schema.mu.Lock()
		schema.Mapping = evolvedSchema.Mapping
		schema.mu.Unlock()
	}
	return err
}

// Get retrieves a value by its key for a given database.
// The retrieved data is automatically "rehydrated" to its original, long-key format.
func (store *Store) Get(dbName, key string) mo.Result[string] {
	// 1. Load the schema.
	val, ok := store.schemas.Load(dbName)
	if !ok {
		return mo.Err[string](fmt.Errorf("db '%s' is not registered", dbName))
	}
	schema := val.(*Schema)

	// 2. Retrieve the data in a read-only transaction.
	var shortenedPayload string
	err := store.impl.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get(schema.dataKey(key))
		if err != nil {
			return err // Propagates buntdb.ErrNotFound if key doesn't exist
		}
		shortenedPayload = val
		return nil
	})

	if err != nil {
		return mo.Err[string](err)
	}

	// 3. Rehydrate the payload before returning it.
	return schema.rehydrate(shortenedPayload)
}
