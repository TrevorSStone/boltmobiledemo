package boltmobiledemo

import (
	"bytes"
	"encoding/binary"
	"github.com/boltdb/bolt"
	"log"
)

var (
	demoBucket = []byte("demoBucket")
	demoKey    = []byte("demoKey")
)

func NewBoltDB(filename string) *BoltDB {
	db, err := bolt.Open(filename+"/demo.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	return &BoltDB{db}
}

type BoltDB struct {
	db *bolt.DB
}

func (b *BoltDB) Path() string {
	return b.db.Path()
}

func (b *BoltDB) Close() {
	b.db.Close()
}

func (b *BoltDB) GetValue() int64 {
	var retVal int64
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(demoBucket)
		if b == nil {
			retVal = -1
			return nil
		}
		v := b.Get(demoKey)
		if v == nil {

			retVal = 0
		} else {
			i, err := binary.ReadVarint(bytes.NewBuffer(v))
			if err != nil {
				return err
			}
			retVal = i
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return retVal
}

func (b *BoltDB) Increment() int64 {
	buf := make([]byte, 8)
	var retVal int64
	err := b.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(demoBucket)
		if err != nil {
			log.Fatal(err)
		}
		v := b.Get(demoKey)
		if v == nil {
			binary.PutVarint(buf, 0)
			retVal = 0
		} else {
			i, err := binary.ReadVarint(bytes.NewBuffer(v))
			if err != nil {
				log.Fatal(err)
			}
			i++
			retVal = i
			binary.PutVarint(buf, i)
		}
		err = b.Put(demoKey, buf)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	return retVal
}
