package store_test

import (
	"context"
	"testing"

	store "github.com/gradientzero/comby-store-minio"
	"github.com/gradientzero/comby/v2"
)

func TestDataStore1(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	dataStore := store.NewDataStoreMinio("127.0.0.1:9000", false, "ROOTNAME", "CHANGEME123")
	if err = dataStore.Init(ctx,
		comby.DataStoreOptionWithAttribute("key1", "value"),
	); err != nil {
		t.Fatal(err)
	}

	// check if the attribute is set
	if v, ok := dataStore.Options().Attributes.Get("key1"); ok {
		if v != "value" {
			t.Fatalf("wrong value: %q", v)
		}
	} else {
		t.Fatalf("missing key")
	}

	// reset database
	if err := dataStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// check totals
	if dataStore.Total(ctx) != 0 {
		t.Fatalf("wrong total %d", dataStore.Total(ctx))
	}

	// Set values
	if err := dataStore.Set(ctx,
		comby.DataStoreSetOptionWithBucketName("bucket1"),
		comby.DataStoreSetOptionWithObjectName("object1"),
		comby.DataStoreSetOptionWithContentType("text/plain"),
		comby.DataStoreSetOptionWithData([]byte("objectValue1")),
	); err != nil {
		t.Fatal(err)
	}
	if err := dataStore.Set(ctx,
		comby.DataStoreSetOptionWithBucketName("bucket2"),
		comby.DataStoreSetOptionWithObjectName("object2"),
		comby.DataStoreSetOptionWithContentType("text/plain"),
		comby.DataStoreSetOptionWithData([]byte("objectValue2")),
	); err != nil {
		t.Fatal(err)
	}

	// check totals
	if dataStore.Total(ctx) != 2 {
		t.Fatalf("wrong total %d", dataStore.Total(ctx))
	}

	// Get a value
	if dataModel, err := dataStore.Get(ctx,
		comby.DataStoreGetOptionWithBucketName("bucket1"),
		comby.DataStoreGetOptionWithObjectName("object1"),
	); err != nil {
		t.Fatal(err)
	} else {
		if string(dataModel.Data) != "objectValue1" {
			t.Fatalf("wrong value: %q", dataModel.Data)
		}
	}

	// List all keys
	if dataModels, err := dataStore.List(ctx); err != nil {
		if len(dataModels) != 2 {
			t.Fatalf("wrong number of keys: %d", len(dataModels))
		}
	}

	// Delete a key
	if err := dataStore.Delete(ctx,
		comby.DataStoreDeleteOptionWithBucketName("bucket2"),
		comby.DataStoreDeleteOptionWithObjectName("object2"),
	); err != nil {
		t.Fatal(err)
	}

	// check totals
	if dataStore.Total(ctx) != 1 {
		t.Fatalf("wrong total %d", dataStore.Total(ctx))
	}

	// reset database
	if err := dataStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := dataStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}
