# db2es
MySQL export to Elasticsearch. This will export complete databases to Elasticsearch. Tablenames will be used types and fields will be prepended within tablename as object.

## Install

```
$ go get github.com/dutchcoders/db2es
$ db2es --database hx7dcdqpvpbky2wu --src mysql://root@127.0.0.1:3306/ --dst http://127.0.0.1:9200/test2
```
