package cmd

import (
	_ "github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/op/go-logging"
	"os"
)

var Version = "0.1"

var format = logging.MustStringFormatter(
	"%{color} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}",
)

var helpTemplate = `NAME:
{{.Name}} - {{.Usage}}

DESCRIPTION:
{{.Description}}

USAGE:
{{.Name}} {{if .Flags}}[flags] {{end}}command{{if .Flags}}{{end}} [arguments...]

COMMANDS:
{{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
{{end}}{{if .Flags}}
FLAGS:
{{range .Flags}}{{.}}
{{end}}{{end}}
VERSION:
` + Version +
	`{{ "\n"}}`

var log = logging.MustGetLogger("db2es")

// db2es --database --host root:@/ http://127.0.0.1:9200/
// db2es --database --host root:@/ http://127.0.0.1:9200/

var globalFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "database",
		Usage: "database(s) to export (comma seperated)",
		Value: "",
	},
	cli.StringFlag{
		Name:  "src",
		Usage: "MySQL host url",
		Value: "",
	},
	cli.StringFlag{
		Name:  "dst",
		Usage: "destination elasticsearch url",
		Value: "",
	},
}

type Cmd struct {
	*cli.App
}

func New() *Cmd {
	app := cli.NewApp()
	app.Name = "sql2es"
	app.Author = "Dutchcoders"
	app.Usage = "sql2es"
	app.Description = ``
	app.Flags = globalFlags
	app.CustomAppHelpTemplate = helpTemplate
	app.Commands = []cli.Command{}

	app.Before = func(c *cli.Context) error {
		logBackends := []logging.Backend{}

		backend1 := logging.NewLogBackend(os.Stdout, "", 0)

		backend1Formatter := logging.NewBackendFormatter(backend1, format)

		backend1Leveled := logging.AddModuleLevel(backend1Formatter)

		level, err := logging.LogLevel("debug")
		if err != nil {
			panic(err)
		}

		backend1Leveled.SetLevel(level, "")

		logBackends = append(logBackends, backend1Leveled)

		logging.SetBackend(logBackends...)

		return nil
	}

	app.Action = exportAction

	return &Cmd{
		App: app,
	}
}
