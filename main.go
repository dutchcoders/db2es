package main

import "github.com/dutchcoders/db2es/cmd"

func main() {
	app := cmd.New()
	app.RunAndExitOnError()
}
