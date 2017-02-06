package cmd

import (
	"github.com/jmoiron/sqlx"

	"fmt"
	_ "github.com/go-sql-driver/mysql"

	"context"
	"github.com/fatih/color"
	"github.com/minio/cli"
	"gopkg.in/olivere/elastic.v5"
	"net/url"
	"strings"
	"sync"
)

func exportAction(c *cli.Context) {
	var srcURL url.URL
	if s := c.GlobalString("src"); s == "" {
		fmt.Println(color.RedString(fmt.Sprintf("Source parameter not set.")))
		return
	} else if u, err := url.Parse(s); err != nil {
		fmt.Println(color.RedString(fmt.Sprintf("Source parameter invalid: %s.", err.Error())))
		return
	} else {
		srcURL = *u
	}

	var dstURL url.URL
	if s := c.GlobalString("dst"); s == "" {
		fmt.Println(color.RedString(fmt.Sprintf("Destination parameter not set.")))
		return
	} else if u, err := url.Parse(s); err != nil {
		fmt.Println(color.RedString(fmt.Sprintf("Destination parameter invalid: %s.", err.Error())))
		return
	} else {
		dstURL = *u
	}

	var databases []string
	if s := c.GlobalString("database"); s == "" {
		fmt.Println(color.RedString(fmt.Sprintf("Databases parameter not set.")))
		return
	} else {
		databases = strings.Split(s, ",")
	}

	username := ""
	password := ""
	if srcURL.User != nil {
		username = srcURL.User.Username()
		password, _ = srcURL.User.Password()
	}

	db, err := sqlx.Connect(srcURL.Scheme, fmt.Sprintf("%s:%s@tcp(%s)/", username, password, srcURL.Host))
	if err != nil {
		fmt.Println(color.RedString(fmt.Sprintf("Error connecting to source: %s", err.Error())))
		return
	}

	type X struct {
		Schema string
		Table  string
	}

	es, err := elastic.NewClient(elastic.SetURL(fmt.Sprintf("%s://%s/", dstURL.Scheme, dstURL.Host)), elastic.SetSniff(false))
	if err != nil {
		fmt.Println(color.RedString(fmt.Sprintf("Error connecting to destination (%s): %s", dstURL.String(), err.Error())))
		return
	}

	var wg sync.WaitGroup

	tables := make(chan X)
	go func() {
		wg.Add(1)
		defer wg.Done()

		bulk := es.Bulk()

		count := 0
		errorCount := 0

		for {
			x, ok := <-tables
			if !ok {
				return
			}

			fmt.Println(color.YellowString(fmt.Sprintf("Exporting table: %s %s.", x.Schema, x.Table)))

			if rows, err := db.Queryx(fmt.Sprintf("SELECT * FROM `%s`.`%s`", x.Schema, x.Table)); err != nil {
				panic(err)
			} else {
				for rows.Next() {
					t := map[string]interface{}{}
					err = rows.MapScan(t)
					if err != nil {
						panic(err)
					}

					doc := map[string]interface{}{}
					doc["schema"] = x.Schema
					doc["table"] = x.Table
					doc[x.Table] = map[string]interface{}{}

					for k, v := range t {
						if b, ok := v.([]byte); ok {
							doc[x.Table].(map[string]interface{})[k] = string(b)
						} else {
							doc[x.Table].(map[string]interface{})[k] = v
						}
					}

					bulk = bulk.Add(elastic.NewBulkIndexRequest().
						Index(dstURL.Path[1:]).
						Type(x.Table).
						Doc(doc),
					)

					if bulk.NumberOfActions() == 0 {
					} else if bulk.NumberOfActions()%100 != 0 {
					} else if response, err := bulk.Do(context.Background()); err != nil {
						fmt.Println(color.RedString(fmt.Sprintf("Error indexing: %s", err.Error())))
					} else {
						succeeded := response.Succeeded()
						failed := response.Failed()

						if response.Errors {
							for _, item := range failed {
								fmt.Println(color.RedString(fmt.Sprintf("Error indexing document: %s.", item.Error.Reason)))
							}
						}

						errorCount += len(failed)
						count += len(succeeded)

						fmt.Println(color.YellowString(fmt.Sprintf("Bulk indexing: %d total %d with %d errors.", len(succeeded), count, errorCount)))
					}
				}
			}

		}
	}()

	for _, database := range databases {
		fmt.Println(database)
		if rows, err := db.Queryx("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = ?", database); err != nil {
			panic(err)
		} else {
			for rows.Next() {

				type Table struct {
					Schema string `db:"table_schema"`
					Name   string `db:"table_name"`
				}

				var t Table
				err = rows.StructScan(&t)

				if err != nil {
					panic(err)
				}

				if t.Schema == "information_schema" {
					continue
				}

				if t.Schema == "mysql" {
					continue
				}

				tables <- X{
					Schema: t.Schema,
					Table:  t.Name,
				}
			}
		}
	}

	close(tables)

	wg.Wait()
}
