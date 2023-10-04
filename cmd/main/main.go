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
	db, err := rashdb.Open("db.db", &rashdb.DBOpenOptions{
		PageSize: 2048,
	})
	if err != nil {
		return err
	}
	// TODO allow multiple keys as primary. We need symbol and timestamp
	err = db.CreateTable("Bars", Bar{}, "Symbol")
	if err != nil {
		return err
	}
	err = db.Insert("Bars", Bar{
		Symbol:    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce ullamcorper efficitur ligula sed sollicitudin. Nam laoreet varius metus et tristique. Morbi tincidunt elit scelerisque scelerisque venenatis. Vestibulum a neque eu turpis varius euismod vitae nec turpis. Aenean maximus sem ultricies porttitor tempus. Ut ornare feugiat dapibus. Etiam semper risus quam, eu placerat lorem cursus nec. Nulla sollicitudin orci quis ante laoreet dictum. Sed id felis purus. Quisque ipsum nunc, fringilla at fermentum id, scelerisque a tortor. Vestibulum a porttitor elit.  Mauris finibus semper est tempus mollis. Fusce tempus lacinia nisl, et aliquet erat tristique in. Morbi fermentum orci diam, ac facilisis nisl hendrerit et. Morbi varius enim ut nibh venenatis, tristique dapibus justo egestas. Duis interdum orci vel lorem faucibus gravida. Ut eleifend neque egestas elit tristique tempor. Curabitur condimentum a tellus ut molestie. Duis ut turpis ut neque ultrices aliquet nec vel nisi. Nunc urna eget.",
		Timestamp: 1695885687,
		Open:      400.0,
		High:      405.3,
		Low:       395.0,
		Close:     401.5,
	})
	if err != nil {
		return err
	}
	err = db.Insert("Bars", Bar{
		Symbol:    "SPY",
		Timestamp: 1695885687,
		Open:      400.0,
		High:      405.4,
		Low:       395.0,
		Close:     401.5,
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
