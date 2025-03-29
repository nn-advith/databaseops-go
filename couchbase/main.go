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

	err = bucket.WaitUntilReady(3*time.Second, nil)
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

// func addNote(note Note) {

// }

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

	// col := bucket.Scope("_default").Collection("users") //precreate this
	// type User struct {
	// 	Name      string   `json:"name"`
	// 	Email     string   `json:"email"`
	// 	Interests []string `json:"interests"`
	// }
	// _, err = col.Upsert("u:adam",
	// 	User{
	// 		Name:      "Adam",
	// 		Email:     "adam@test-email.com",
	// 		Interests: []string{"Swimming", "Rowing"},
	// 	}, nil)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// getResult, err := col.Get("u:jade", nil)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// var inUser User
	// err = getResult.Content(&inUser)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("User: %v\n", inUser)

	// inventoryScope := bucket.Scope("_default")
	// queryResult, err := inventoryScope.Query(
	// 	"SELECT * FROM users",
	// 	&gocb.QueryOptions{Adhoc: true},
	// )
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// for queryResult.Next() {
	// 	var result map[string]User
	// 	// unkown json so decode into interfcae
	// 	err := queryResult.Row(&result)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	fmt.Println(result["users"].Name)
	// }

	// if err := queryResult.Err(); err != nil {
	// 	log.Fatal(err)
	// }

	// queryResult, err = inventoryScope.Query(
	// 	"SELECT name, email FROM users",
	// 	&gocb.QueryOptions{Adhoc: true},
	// )
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// for queryResult.Next() {
	// 	var result User
	// 	// unkown json so decode into interfcae
	// 	err := queryResult.Row(&result)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	fmt.Println(result)
	// }

	// if err := queryResult.Err(); err != nil {
	// 	log.Fatal(err)
	// }

}
