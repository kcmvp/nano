package nano

import (
	"github.com/samber/lo"
)

type JsonProperty string

// Schema defines a lightweight metadata blueprint for JSON documents in a key-value database.
// You do not need to define every possible property in advance.
// The system will automatically discover and store all fields from any JSON document you save.
// The Schema's primary role is to tell the database which of those properties have special meaning.
//   - id: The property that uniquely identifies a document.
//   - createdAt: The property that stores the creation timestamp of a document.
//   - updatedAt: The property that stores the last update timestamp of a document.
//   - status: The property that stores the business status of a document.

type Schema struct {
	name      string
	id        JsonProperty
	createdAt JsonProperty
	updatedAt JsonProperty
	status    map[JsonProperty]struct{}
}

// NewSchema creates a new Schema instance.
//   - name: The name of the schema.
//   - id: The JsonProperty that uniquely identifies a document.
//   - traits: Optional functions to configure additional schema properties like createdAt, updatedAt, and status.

func NewSchema(name string, id JsonProperty, traits ...func(*Schema)) *Schema {
	schema := &Schema{
		name: name,
		id:   id,
		// Initialize status map to avoid nil pointer dereference if Status trait is not used
		// and schema.status is accessed later (e.g., in a getter).
		// Although currently, schema.status is only set by the Status trait.
		status: make(map[JsonProperty]struct{}),
	}
	lo.ForEach(traits, func(trait func(*Schema), _ int) {
		trait(schema)
	})
	return schema
}

// CreateTime configures the schema to recognize a specific JsonProperty as the creation timestamp.
// This property will be automatically populated with the current time when a new document is created,
// and can be used for chronological sorting or filtering.
//
// Example:
// schema.NewSchema("users", "id", schema.CreateTime("created_at"))
//
// In a JSON document: {"id": "user123", "created_at": "2023-10-27T10:00:00Z"}

func CreateTime(prop JsonProperty) func(*Schema) {
	return func(schema *Schema) {
		schema.createdAt = prop
	}
}

// UpdateTime configures the schema to recognize a specific JsonProperty as the last update timestamp.
// This property will be automatically updated with the current time whenever a document is modified,
// allowing for tracking of document freshness.
// Example:
// schema.NewSchema("products", "sku", schema.UpdateTime("last_modified"))
//
// In a JSON document: {"sku": "prod001", "last_modified": "2023-10-27T11:30:00Z"}
func UpdateTime(prop JsonProperty) func(*Schema) {
	return func(schema *Schema) {
		schema.updatedAt = prop
	}
}

// Status configures the schema to recognize one or more JsonProperties as status fields.
// Example:
// schema.NewSchema("orders", "order_id", schema.Status("order_status", "payment_status"))
//
// In a JSON document: {"order_id": "ORD456", "order_status": "pending", "payment_status": "unpaid"}
// These properties can be used to categorize or track the lifecycle of a document,
// enabling filtering or aggregation based on their values.
func Status(props ...JsonProperty) func(*Schema) {
	return func(schema *Schema) {
		schema.status = lo.Keyify(props)
	}
}
