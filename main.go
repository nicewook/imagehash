package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

func GetMD5Hash(b []byte) string {
	hash := md5.Sum(b)
	return hex.EncodeToString(hash[:])
}

func InitDB(file string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", file)
	if err != nil {
		return nil, err
	}

	createTableQuery := `
		create table IF NOT EXISTS image_table ( 
		id integer PRIMARY KEY autoincrement,
		inputdate DATETIME,
		imghash TEXT unique,
		imgpath TEXT
		)
	`
	_, e := db.Exec(createTableQuery)
	if e != nil {
		return nil, e
	}
	return db, nil
}

func AddImageRecord(db *sql.DB, imghash, imgpath string) error {
	query := `INSERT INTO image_table VALUES(NULL,?,?,?)`
	_, err := db.Exec(query, time.Now().Format(time.StampMilli), imghash, imgpath)
	return err
}

const dbfilename = "image.db"

func main() {
	// parse image path
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	imgpath := flag.String("path", ".", "directory which has images")
	flag.Parse()

	// init database
	if err := os.Remove(dbfilename); err != nil {
		log.Println(err)
	}
	db, err := InitDB(dbfilename)
	if err != nil {
		log.Fatal(err)
	}

	hashcheck := make(map[string]struct{}, 80000)

	start := time.Now()
	log.Println(start.Format(time.RFC3339))
	err = filepath.Walk(*imgpath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println(err)
			return err
		}
		if info.IsDir() { // pass directory
			return nil
		}
		// fmt.Printf("dir: %v: name: %s\n", info.IsDir(), path)
		b, _ := os.ReadFile(path)
		hash := GetMD5Hash(b)

		if _, ok := hashcheck[hash]; ok {
			log.Println("already exist:", hash)
		} else {
			hashcheck[hash] = struct{}{}
		}

		// fmt.Printf("%v, %s, %s\n", time.Now().Format(time.RFC3339Nano), hash, path)
		if err := AddImageRecord(db, hash, path); err != nil {
			log.Println("fail to insert image:", err)
		}
		return nil
	})

	end := time.Now()
	log.Println(end.Format(time.RFC3339))
	log.Println("time elapsed:", time.Since(start))

	if err != nil {
		log.Println(err)
	}
}
