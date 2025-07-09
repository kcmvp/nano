package nano

import "errors"

var ErrDbExists = errors.New("database already exists")
var ErrSchemaExists = errors.New("schema already exists")
