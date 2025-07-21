
### Features
- k/v based schemaless database
- multiple store layers
  - memory: backed by `buntdb`
  - disk: sqlite
- `buntdb` just keep small blueprint of the properties defined in schema
- no need to define the schema in advance, system would evolved the schema
### Design
- `nano` is a schemaless k/v database, it means you don't need to define the schema in advance.
- `nano` would evolve the schema based on the data you put in.
- `nano` is designed to be a lightweight database, it can be embedded in your application.
- `nano` supports multiple store layers, you can choose different store layers based on your needs.
- `nano` provides a simple API to interact with the database.
- `nano` is written in Go, and it's open source.
- `nano` is inspired by `buntdb` and `sqlite`.
