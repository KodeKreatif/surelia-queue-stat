package main

import (
  "strconv"
  "strings"
  "bufio"
  "fmt"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
)

type Entry struct {
  msg string
  retries int 
  efrom string
  eto string
  target int
  state int
  size int64
}

func parse(fileName string, entries map[string]Entry) map[string]Entry {
  file, _ := os.Open(fileName)
  scanner := bufio.NewScanner(file)
  for scanner.Scan() {
    text := scanner.Text()
    if (text[0] != '@') {
      break
    }
    fields := strings.Split(text, " ")
    op := fields[1]
    switch op {
      case "starting":
        if fields[2] == "delivery" {
          msg := fields[5] 
          entry := entries[msg]
          if fields[7] == "remote" {
            entry.target = 1
          } else {
            entry.target = 0
          }
          entry.eto = fields[8]
          entries[msg] = entry
        }
      case "info":
        if fields[2] == "msg" {
          msg := strings.Replace(fields[3], ":", "", 1)
          entry := entries[msg]
          entry.size, _ = strconv.ParseInt(fields[5], 0, 64)
          entry.efrom = fields[7]
          entries[msg] = entry
        }
      case "new":
        if fields[2] == "msg" {
          msg := fields[3]
          entry := entries[msg]
          if entry.msg == "" {
            entry.msg = msg
          } else {
            entry.retries ++
          }
          entries[msg] = entry
        }

    }
  }

  if err := scanner.Err(); err != nil {
    log.Fatal(err)
  }

  return entries
}

func main() {
	os.Remove("./stat.db")

	db, err := sql.Open("sqlite3", "./stat.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sql := `
	create table stat (msg text not null, retries integer not null, efrom text, eto text, target integer, state integer, size integer);
	`
	_, err = db.Exec(sql)
	if err != nil {
		log.Printf("%q: %s\n", err, sql)
		return
	}

  entries := make(map[string]Entry)

  for _, fileName := range os.Args { 
    entries = parse(fileName, entries)
  }

  tx, err := db.Begin()
  if err != nil {
    log.Fatal(err)
  }

  stmt, err := tx.Prepare("insert into stat(msg, retries, efrom, eto, target, size) values (?, ?, ?, ?, ?, ?)")
  defer stmt.Close()

  for _, entry := range entries {
    _, err = stmt.Exec(entry.msg, entry.retries, entry.efrom, entry.eto, entry.target, entry.size)
    if err != nil {
      log.Fatal(err)
    }
  }

  fmt.Println()
  tx.Commit()
}
