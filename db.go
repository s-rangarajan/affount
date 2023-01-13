package main

import embeddedpostgres "github.com/fergusstrange/embedded-postgres"

func main() {
	postgres := embeddedpostgres.NewDatabase()
	err := postgres.Start()
	if err != nil {
		panic(err)
	}
}
