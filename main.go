package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"google.golang.org/api/option"
	"log"
)

type MyItem struct {
	Name  string
	Value int
}

func main() {
	ctx := context.Background()

	// Set your Google Cloud Project ID and Dataset ID
	projectID := "big-query-with-go"
	datasetID := "metric"
	//tableID := "deposit"

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile("big-query-with-go-bc76bb8b0e96.json"))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ds := client.DatasetInProject(projectID, datasetID)
	fmt.Printf("Dataset Info: %v\n", ds)
	//table := client.Dataset(datasetID).Table(tableID)
	//inserter := table.Inserter()
	//
	//items := []*MyItem{
	//	{Name: "first", Value: 5},
	//}
	//
	//if err = inserter.Put(ctx, items); err != nil {
	//	log.Fatalf("Failed to insert items: %v", err)
	//}
	//
	//fmt.Println("Items inserted successfully!")
}
