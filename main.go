package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"errors"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"log"
	"math/big"
	"time"
)

type Deposit struct {
	Date          time.Time `json:"date"`
	Brand         string    `json:"brand"`
	Country       string    `json:"country"`
	PspName       string    `json:"psp_name"`
	PaymentMethod string    `json:"payment_method"`
	Amount        *big.Rat  `json:"amount"`
	Currency      string    `json:"currency"`
	Status        string    `json:"status"`
}

func main() {
	ctx := context.Background()

	// Set your Google Cloud Project ID and Dataset ID
	projectID := "big-query-with-go"
	datasetID := "metric"
	tableID := "deposit"

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile("big-query-with-go-bc76bb8b0e96.json"))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	table := client.Dataset(datasetID).Table(tableID)
	inserter := table.Inserter()

	items := []*Deposit{
		{
			Date:          time.Now(),
			Brand:         "GGPCOM",
			Country:       "KR",
			PspName:       "VISA",
			PaymentMethod: "CreditCard",
			Amount:        new(big.Rat).SetFloat64(19800.0098),
			Currency:      "USD",
			Status:        "Init",
		},
	}

	if err = inserter.Put(ctx, items); err != nil {
		log.Fatalf("Failed to insert items: %v", err)
	}

	fmt.Println("Items inserted successfully!")

	query := client.Query("SELECT pspName, amount FROM `big-query-with-go.metric.deposit`")
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("Failed to initiate read: %v", err)
	}

	for {
		var item Deposit
		err = it.Next(&item)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			log.Fatalf("Failed to read data: %v", err)
		}
		fmt.Printf("Name: %s, Value: %v\n", item.PspName, item.Amount)
	}
}
