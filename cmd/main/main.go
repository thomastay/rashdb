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
	// TODO allow multiple keys as primary. We need symbol and timestamp
	err = db.CreateTable("Bars", Bar{}, "Symbol")
	if err != nil {
		return err
	}
	err = db.Insert("Bars", Bar{
		Symbol:    "SPY",
		Timestamp: 1695885687,
		Open:      400.0,
		High:      405.0,
		Low:       395.0,
		Close:     401.0,
	})
	if err != nil {
		return err
	}
	err = db.SyncAll()
	if err != nil {
		return err
	}
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
