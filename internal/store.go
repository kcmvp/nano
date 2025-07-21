package internal

import (
	"encoding/json"
	"errors"
	"fmt"
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
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	schemaSpacePrefix = "_schema"
	keySep            = ":"
)

// ErrDbExists is returned when attempting to register a database that already exists.
var ErrDbExists = errors.New("database already exists")

// ErrNamespaceExists is returned when attempting to register a schema that already exists.
var ErrNamespaceExists = errors.New("namespace already exists")

var validCharSet = append(lo.LowerCaseLettersCharset, lo.NumbersCharset...)
var store *Store
var once sync.Once
var cfg *viper.Viper
var storePath string

// Store is the main struct for interacting with the database.
// It encapsulates the buntdb instance and manages schemas for different data types.
// It is designed as a singleton, accessible via the StoreImpl() function.
type Store struct {
	impl    *buntdb.DB
	schemas map[string]*Schema
	mu      sync.RWMutex
}

func KeyPair(key string) (prefix, name string) {
	items := strings.Split(key, keySep)
	lo.Assert(len(items) == 2, "invalid key %s", key)
	prefix = items[0]
	name = items[1]
	return
}

func init() {
	cfg = viper.New()
	cfg.SetConfigName("app")
	cfg.SetConfigType("yaml")
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

func StoreImpl() *Store {
	once.Do(func() {
		storePath = cfg.GetString("nano.data")
		if len(storePath) == 0 {
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
		store = &Store{impl: db, schemas: map[string]*Schema{}}
		if err = store.impl.View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(SchemaSpace(), func(key, value string) bool {
				//prefix, name := KeyPair(strings.TrimPrefix(key, schemaPrefix))
				items := strings.Split(key, keySep)
				items = items[len(items)-2:]
				schema := &Schema{Name: items[0], space: items[1]}
				if err := json.Unmarshal([]byte(value), schema); err != nil {
					log.Printf("WARN: Corrupted schema for key '%s', skipping. Error: %v", key, err)
					return true // Continue to the next schema
				}
				schema.reverseMapping = lo.Invert(schema.Mapping)
				store.schemas[schema.Name] = schema
				return true
			})
		}); err != nil {
			log.Fatalf("failed to open database %v", err)
		}
	})
	return store
}

// Schemas returns all registered schemas in the store.
// This function provides a snapshot of the currently active schemas,
// allowing external components to inspect their definitions, including
// their names, namespaces, and field mappings.
func Schemas() []*Schema {
	return lo.Values(store.schemas)
}

// Close closes the underlying buntdb database.
// It should be called when the application is shutting down.
func (store *Store) Close() error {
	if store.impl != nil {
		return store.impl.Close()
	}
	return nil
}

func (store *Store) space(schema string) string {
	panic("")
}

// Registry registers a new database schema with the store.
// It performs a comprehensive, in-memory pre-check to ensure all new schemas are unique
// by both name and namespace. If validation passes, it writes all schemas
// to the database in a single, atomic transaction.
func (store *Store) Registry(schemas ...*Schema) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	spaces := lo.MapValues(store.schemas, func(schema *Schema, key string) string {
		return schema.space
	})
	// update target schemas or registry a new one
	if target, ok := lo.Find(schemas, func(item *Schema) bool {
		if !lo.HasKey(spaces, item.Name) {
			spaces[item.Name] = ""
			return false
		}
		return true
	}); ok {
		existing, _ := store.schemas[target.Name]
		lo.Assert(existing != nil, "can not find schema %s", target.Name)
		acceptable := existing.migrate(target)
		fmt.Println(acceptable)

		return fmt.Errorf("%w '%s'", ErrDbExists, target.Name)
	}
	altos(spaces)
	// --- Step 2: All checks passed. Perform the atomic database write. ---
	return store.impl.Update(func(tx *buntdb.Tx) error {
		for _, schema := range schemas {
			space, ok := spaces[schema.Name]
			lo.Assert(ok, "can not find space for schema '%s'", schema.Name)
			schema.space = space
			data, err := json.Marshal(schema)
			if err != nil {
				return fmt.Errorf("failed to marshal schema '%s': %w", schema.Name, err)
			}
			if _, _, err := tx.Set(schema.Key(), string(data), nil); err != nil {
				return fmt.Errorf("failed to persist schema '%s': %w", schema.Name, err)
			}
			// Update the in-memory cache upon successful persistence.
			store.schemas[schema.Name] = schema
		}
		return nil
	})
}

// Schema represents the structure and altos rules for a specific "database"
// within the buntdb store. It defines how original field names are mapped to
// shorter, optimized keys for storage efficiency.
type Schema struct {

	// Name is the unique identifier for the schema (e.g., "users", "products").
	Name string `json:"-"`
	// space is a unique prefix for all keys belonging to this DB.
	// It helps in isolating data for different logical databases within the same buntdb instance.
	// For example, if space is "u", all keys for this DB will be stored as "u:key".
	// space is typically a single character or a altos string to minimize storage overhead.
	space string

	// Mapping stores the Mapping between original JSON field names and their shortened representations.
	// This map is used for optimizing storage by replacing long field names with shorter ones (e.g., "firstName" -> "f1").
	// It's persisted to buntdb to ensure consistency across application restarts.
	// The key is the original field Name, and the value is the shortened field Name.
	Mapping map[string]string `json:"altos"`

	// reverseMapping is a private, in-memory cache for fast rehydration.
	// As an unexported field, it is automatically ignored by the json package.
	reverseMapping map[string]string

	// IdProp is the name of the field that uniquely identifies a record within the schema.
	IdProp string `json:"idProp"`

	// CreatedAtProp is the name of the field that stores the creation timestamp of a record.
	CreatedAtProp string `json:"createdAtProp"`

	// UpdatedAtProp is the name of the field that stores the last update timestamp of a record.
	UpdatedAtProp string `json:"updatedAtProp"`

	// StatusProps are the status fields that store the status value of a record
	StatusProps []string `json:"statusProps"`

	// mu is a RWMutex to protect concurrent access to the DB's fields,
	// especially during schema updates or data access.
	mu sync.RWMutex
}

func (schema *Schema) migrate(target *Schema) error {
	panic("")
}

func Blueprint(name, id, createdAt, updatedAt string, status ...string) *Schema {
	mapping := map[string]string{}
	if len(createdAt) > 0 {
		mapping[createdAt] = "c"
	}
	if len(updatedAt) > 0 {
		mapping[updatedAt] = "u"
	}
	return &Schema{
		Name:           name,
		IdProp:         id,
		CreatedAtProp:  createdAt,
		UpdatedAtProp:  updatedAt,
		Mapping:        mapping,
		reverseMapping: lo.Invert(mapping),
	}
}

func (schema *Schema) ID() string {
	return fmt.Sprintf("%s%s%s", schema.Name, keySep, schema.space)
}

func (schema *Schema) Key() string {
	return fmt.Sprintf("%s%s%s", schemaSpacePrefix, keySep, schema.ID())
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
		Name:    schema.Name,
		space:   schema.space,
		Mapping: newMapping,
	}
}

func (schema *Schema) NameSpace() string {
	return fmt.Sprintf("%s%s%s*", schema.space, keySep)
}

// SchemaSpace returns the schema string for system-level keys.
// These keys are used internally to manage registered databases and their prefixes.
// The format is "_schema:*", which matches all keys starting with "_schema:".
func SchemaSpace() string {
	return fmt.Sprintf("%s%s*", schemaSpacePrefix, keySep)
}

// shorten transforms a JSON payload by replacing original field names with their
// shorter, optimized representations based on the schema's altos.
// If new fields are encountered, the schema's in-memory altos is evolved.
// It returns the shortened JSON and a boolean indicating if the schema was evolved.
func (schema *Schema) shorten(payload string) (mo.Result[string], *Schema) {
	schemaEvolved := schema.Clone()
	longKeys := lo.FilterMap(gjson.Get(payload, "@keys").Array(), func(item gjson.Result, _ int) (string, bool) {
		return item.Str, true
	})
	if len(longKeys) == 0 {
		return mo.Ok(payload), schemaEvolved
	}
	lo.ForEach(longKeys, func(key string, _ int) {
		if _, ok := schemaEvolved.Mapping[key]; !ok {
			schemaEvolved.Mapping[key] = ""
		}
	})
	altos(schema.Mapping)
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

	// Create a reverse map for efficient lookups (altos -> long).
	var builder strings.Builder
	builder.Grow(len(payload))
	builder.WriteString("{")
	var transformErr error
	first := true
	gjson.Parse(payload).ForEach(func(key, value gjson.Result) bool {
		longKey, ok := schema.reverseMapping[key.String()]
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
	return fmt.Sprintf("%s%s%s", schema.space, keySep, key)
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
	store.mu.RLock()
	schema, ok := store.schemas[dbName]
	store.mu.RUnlock()
	if !ok {
		return fmt.Errorf("db '%s' is not registered", dbName)
	}
	// 2. Perform the entire operation in a single, atomic transaction.
	var evolvedSchema *Schema
	var evolved bool
	err := store.impl.Update(func(tx *buntdb.Tx) error {
		// 3. Shorten the payload. This will also evolve schemaCopy's in-memory altos.
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
		schema.reverseMapping = lo.Invert(schema.Mapping)
		schema.mu.Unlock()
	}
	return err
}

// Get retrieves a value by its key for a given database.
// The retrieved data is automatically "rehydrated" to its original, long-key format.
func (store *Store) Get(dbName, key string) mo.Result[string] {
	// 1. Load the schema.
	store.mu.RLock()
	schema, ok := store.schemas[dbName]
	store.mu.RUnlock()
	if !ok {
		return mo.Err[string](fmt.Errorf("db '%s' is not registered", dbName))
	}
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

func altos(entries map[string]string) {
	used := map[string]struct{}{}
	notMapped := lo.PickBy(entries, func(key string, value string) bool {
		if len(value) == 0 {
			used[value] = struct{}{}
		}
		return len(value) == 0
	})
	var notFilledErr = fmt.Errorf("not filled")
	for l, _ := range notMapped {
		// 36
		if _, err := lo.Attempt(len(validCharSet), func(index int) error {
			key := string(validCharSet[index])
			if !lo.HasKey(used, key) {
				entries[l] = key
				used[key] = struct{}{}
				return nil
			}
			return notFilledErr
		}); err != nil {
			// 26 * 36 = 936
			_, _ = lo.Attempt(len(lo.LowerCaseLettersCharset), func(index int) error {
				f := lo.LowerCaseLettersCharset[index]
				for _, r := range validCharSet {
					key := fmt.Sprintf("%c%c", f, r)
					if !lo.HasKey(used, key) {
						entries[l] = key
						used[key] = struct{}{}
						return nil
					}
				}
				return notFilledErr
			})
		}
	}
}
