package main

import (
	"fmt"

	rashdb "github.com/thomastay/rash-db"
)

func main() {
	err := run()
	if err != nil {
		panic(err)
	}
}

func run() error {
	db, err := rashdb.Open("db.db")
	if err != nil {
		return err
	}
	fmt.Print(db)
	return nil
}
