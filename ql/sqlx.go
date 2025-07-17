package ql

import (
	"database/sql"
)

func Query(query string) func(args ...any) ([]map[string]any, error) {
	return func(args ...any) ([]map[string]any, error) {
		panic("")
		//return secondary.SQL().Query(query, args...)
	}
}

func EachRow(query string) func(args ...any) ([]map[string]any, error) {
	return func(args ...any) ([]map[string]any, error) {
		panic("")
		//return secondary.SQL().Query(query, args...)
	}
}

func Execute(query string) func(args ...any) (sql.Result, error) {
	return func(args ...any) (sql.Result, error) {
		panic("")
		//return secondary.SQL().Exec(query, args)
	}
}
