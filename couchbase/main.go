package main

import (
	"fmt"
	"os"
	"time"

	"github.com/couchbase/gocb/v2"
)

type Note struct {
	DateTime string `json:"datetime"`
	Note     string `json:"note"`
	Origin   string `json:"origin"`
	BGColor  string `json:"bgcolor"`
}

type CBConnector struct {
	ConnectionString string
	BucketName       string
	Username         string // DEVTEST only; use env
	Password         string // DEVTEST only; use env
	Cluster          *gocb.Cluster
	Bucket           *gocb.Bucket
}

func (c *CBConnector) Initialize(connectionString, bucketName, username, password string) error {

	fmt.Println("Connecting to cluster and initialising bucket...")
	c.ConnectionString = connectionString
	c.BucketName = bucketName
	c.Username = username
	c.Password = password
	cluster, err := gocb.Connect("couchbase://"+connectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: c.Username,
			Password: c.Password,
		},
	})
	if err != nil {
		return fmt.Errorf("authentication failed: %v", err)
	}
	c.Cluster = cluster
	bucket := c.Cluster.Bucket(c.BucketName)
	c.Bucket = bucket
	err = c.Bucket.WaitUntilReady(3*time.Second, nil)
	if err != nil {
		return fmt.Errorf("bucket connection failed: %v", err)
	}
	fmt.Println("Cluster connected and bucket initialised")
	return nil
}

func (c *CBConnector) Disconnect() error {
	fmt.Println("Disconnecting from cluster ...")
	if err := c.Cluster.Close(nil); err != nil {
		return fmt.Errorf("error disconnecting from cluster: %v", err)
	} else {
		fmt.Println("Disconnected")
	}
	return nil
}

func (c *CBConnector) AddOrUpdateNote(key string, note Note, scope string, collection string) error {
	//upsert
	fmt.Println("adding documents")
	col := c.Bucket.Scope(scope).Collection(collection)
	_, err := col.Upsert(key, note, nil)
	if err != nil {
		return fmt.Errorf("error while upsert: %v", err)
	}
	fmt.Println("added documents")
	return nil
}

func (c *CBConnector) GetNote(key string, scope string, collection string) error {
	fmt.Println("retrieving note with ID ", key)
	col := c.Bucket.Scope(scope).Collection(collection)
	result, err := col.Get(key, nil)
	if err != nil {
		return fmt.Errorf("get operation failed : %v", err)
	}
	var note Note
	if err := result.Content(&note); err != nil {
		return fmt.Errorf("error decoding ; %v", err)
	}
	fmt.Println(note)
	return nil
}

func (c *CBConnector) GetAllNotes(scope string, collection string) error {
	fmt.Println("retrieving all notes")
	sc := c.Bucket.Scope(scope)
	results, err := sc.Query("SELECT * FROM "+string(collection), &gocb.QueryOptions{Adhoc: true})
	if err != nil {
		return fmt.Errorf("error performing query; %v", err)
	}

	for results.Next() {
		var note map[string]Note
		err := results.Row(&note)
		if err != nil {
			return fmt.Errorf("error decoding; %v", err)
		}
		fmt.Println(note["notes"])

	}
	return nil
}

func (c *CBConnector) GetNoteAndBG(scope string, collection string) error {
	fmt.Println("retrieving note and bg color")
	sc := c.Bucket.Scope(scope)
	results, err := sc.Query("SELECT note,bgcolor FROM "+string(collection), &gocb.QueryOptions{Adhoc: true})
	if err != nil {
		return fmt.Errorf("error performing query; %v", err)
	}

	for results.Next() {
		var note Note
		err := results.Row(&note)
		if err != nil {
			return fmt.Errorf("error decoding; %v", err)
		}
		fmt.Println(note)

	}
	return nil
}

func (c *CBConnector) DeleteNote(key string, scope string, collection string) error {
	fmt.Println("deleting note with ID ", key)
	col := c.Bucket.Scope(scope).Collection(collection)
	_, err := col.Remove(key, nil)
	if err != nil {
		return fmt.Errorf("remove operation failed : %v", err)
	}
	fmt.Println("removal completed")
	return nil
}

func main() {
	// gocb.SetLogger(gocb.DefaultStdioLogger())
	connection_string := "localhost"
	bucket_name := "Ororo"
	username := "cbuser"
	password := "cbuser"

	connection := CBConnector{}
	err := connection.Initialize(connection_string, bucket_name, username, password)
	if err != nil {
		fmt.Println(err)
		fmt.Println("fatal: connection failed. exiting")
		os.Exit(0)
	}

	defer func() {
		if err := connection.Disconnect(); err != nil {
			fmt.Println(err)
		}
	}()

	newnote := Note{
		DateTime: string(time.DateTime),
		Note:     "Get Good",
		Origin:   "notes",
		BGColor:  "#ff0000",
	}
	if err := connection.AddOrUpdateNote("n:note1", newnote, "_default", "notes"); err != nil {
		fmt.Println(err)
	}
	if err := connection.AddOrUpdateNote("n:note2", newnote, "_default", "notes"); err != nil {
		fmt.Println(err)
	}
	if err := connection.GetAllNotes("_default", "notes"); err != nil {
		fmt.Println(err)
	}
	if err := connection.GetNoteAndBG("_default", "notes"); err != nil {
		fmt.Println(err)
	}
	if err := connection.GetNote("n:note1", "_default", "notes"); err != nil {
		fmt.Println(err)
	}
	if err := connection.DeleteNote("n:note1", "_default", "notes"); err != nil {
		fmt.Println(err)
	}
}
