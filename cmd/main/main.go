package main

import (
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
	db.CreateTable("Bars", Bar{})
	db.SyncAll()
	return nil
}

type Bar struct {
	Symbol    string
	Timestamp uint64
	Open      float64
	High      float64
	Low       float64
	Close     float64
}
