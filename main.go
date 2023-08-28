package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"log"
	"math/big"
	"net/http"
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

	//dataset := client.Dataset(datasetID)
	//err = dataset.Table(tableID).Create(ctx, &bigquery.TableMetadata{
	//	ExpirationTime: time.Now().Add(1 * time.Hour),
	//})
	//if err != nil {
	//	fmt.Println(err)
	//}
	tableRef := client.Dataset(datasetID).Table(tableID)
	if _, err = tableRef.Metadata(ctx); err != nil {
		var e *googleapi.Error
		if errors.As(err, &e) {
			if e.Code == http.StatusNotFound {
				if err = tableRef.Create(ctx, &bigquery.TableMetadata{
					Name: "deposit",
					Schema: bigquery.Schema{
						{Name: "Date", Required: true, Type: bigquery.TimestampFieldType},
						{Name: "Brand", Required: true, Type: bigquery.StringFieldType},
						{Name: "Country", Required: true, Type: bigquery.StringFieldType},
						{Name: "PspName", Required: true, Type: bigquery.StringFieldType},
						{Name: "PaymentMethod", Required: true, Type: bigquery.StringFieldType},
						{Name: "Amount", Required: true, Type: bigquery.BigNumericFieldType},
						{Name: "Currency", Required: true, Type: bigquery.StringFieldType},
						{Name: "Status", Required: true, Type: bigquery.StringFieldType},
					},
				}); err != nil {
					log.Fatalf("create bigquery table failed")
				}
			}
		}
	}

	inserter := tableRef.Inserter()

	items := []*Deposit{
		{
			Date:          time.Now(),
			Brand:         "GGPCOM",
			Country:       "CH",
			PspName:       "VISA",
			PaymentMethod: "CreditCard",
			Amount:        new(big.Rat).SetFloat64(98980.00),
			Currency:      "USD",
			Status:        "Init",
		},
	}

	for i := 0; i < 100; i++ {
		if err = inserter.Put(ctx, items); err != nil {
			log.Fatalf("Failed to insert items: %v", err)
		}
		log.Println("insert item ", i)
	}

	//query := client.Query("SELECT pspName, amount FROM `big-query-with-go.metric.deposit`")
	//it, err := query.Read(ctx)
	//if err != nil {
	//	log.Fatalf("Failed to initiate read: %v", err)
	//}
	//
	//for {
	//	var item Deposit
	//	err = it.Next(&item)
	//	if errors.Is(err, iterator.Done) {
	//		break
	//	}
	//	if err != nil {
	//		log.Fatalf("Failed to read data: %v", err)
	//	}
	//	fmt.Printf("Name: %s, Value: %v\n", item.PspName, item.Amount)
	//}
}
