package l2

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"sync"
)

var db *sql.DB
var once sync.Once

func SQL() *sql.DB {
	var err error
	once.Do(func() {
		db, err = sql.Open("sqlite3", "./nano.db")
		if err != nil {
			log.Fatalf("can't open db: %v", err)
		}
	})
	return db
}
