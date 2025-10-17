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
	if v := dataStore.Options().Attributes.Get("key1"); v != nil {
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
	if dataModels, _, err := dataStore.List(ctx); err != nil {
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

func TestDataStoreWithCryptoService(t *testing.T) {
	var err error
	ctx := context.Background()

	// create crypto service with 32-byte key (AES-256)
	key := []byte("01234567890123456789012345678901")
	cryptoService, err := comby.NewCryptoService(key)
	if err != nil {
		t.Fatalf("failed to create crypto service: %v", err)
	}

	// setup and init store with crypto service
	dataStore := store.NewDataStoreMinio("127.0.0.1:9000", false, "ROOTNAME", "CHANGEME123", comby.DataStoreOptionWithCryptoService(cryptoService))
	if err = dataStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// reset database
	if err := dataStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	testData := []byte("sensitive data that should be encrypted")

	// Set encrypted value
	if err := dataStore.Set(ctx,
		comby.DataStoreSetOptionWithBucketName("encrypted-bucket"),
		comby.DataStoreSetOptionWithObjectName("encrypted-object"),
		comby.DataStoreSetOptionWithContentType("text/plain"),
		comby.DataStoreSetOptionWithData(testData),
	); err != nil {
		t.Fatal(err)
	}

	// Get and decrypt value
	if dataModel, err := dataStore.Get(ctx,
		comby.DataStoreGetOptionWithBucketName("encrypted-bucket"),
		comby.DataStoreGetOptionWithObjectName("encrypted-object"),
	); err != nil {
		t.Fatal(err)
	} else {
		if string(dataModel.Data) != string(testData) {
			t.Fatalf("decrypted data mismatch: got %q, want %q", string(dataModel.Data), string(testData))
		}
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

func TestDataStoreCopy(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	dataStore := store.NewDataStoreMinio("127.0.0.1:9000", false, "ROOTNAME", "CHANGEME123")
	if err = dataStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// reset database
	if err := dataStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// Set source value
	sourceData := []byte("data to copy")
	if err := dataStore.Set(ctx,
		comby.DataStoreSetOptionWithBucketName("source-bucket"),
		comby.DataStoreSetOptionWithObjectName("source-object"),
		comby.DataStoreSetOptionWithContentType("text/plain"),
		comby.DataStoreSetOptionWithData(sourceData),
	); err != nil {
		t.Fatal(err)
	}

	// Copy to destination
	if err := dataStore.Copy(ctx,
		comby.DataStoreCopyOptionWithSrcBucketName("source-bucket"),
		comby.DataStoreCopyOptionWithSrcObjectName("source-object"),
		comby.DataStoreCopyOptionWithDstBucketName("dest-bucket"),
		comby.DataStoreCopyOptionWithDstObjectName("dest-object"),
	); err != nil {
		t.Fatal(err)
	}

	// Verify destination has the data
	if dataModel, err := dataStore.Get(ctx,
		comby.DataStoreGetOptionWithBucketName("dest-bucket"),
		comby.DataStoreGetOptionWithObjectName("dest-object"),
	); err != nil {
		t.Fatal(err)
	} else {
		if string(dataModel.Data) != string(sourceData) {
			t.Fatalf("copied data mismatch: got %q, want %q", string(dataModel.Data), string(sourceData))
		}
	}

	// Verify source still exists
	if dataModel, err := dataStore.Get(ctx,
		comby.DataStoreGetOptionWithBucketName("source-bucket"),
		comby.DataStoreGetOptionWithObjectName("source-object"),
	); err != nil {
		t.Fatal(err)
	} else {
		if string(dataModel.Data) != string(sourceData) {
			t.Fatalf("source data mismatch: got %q, want %q", string(dataModel.Data), string(sourceData))
		}
	}

	// check totals
	if dataStore.Total(ctx) != 2 {
		t.Fatalf("wrong total after copy: %d", dataStore.Total(ctx))
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

func TestDataStoreInfo(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	dataStore := store.NewDataStoreMinio("127.0.0.1:9000", false, "ROOTNAME", "CHANGEME123")
	if err = dataStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// reset database
	if err := dataStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// Get info on empty store
	if info, err := dataStore.Info(ctx); err != nil {
		t.Fatal(err)
	} else {
		if info.StoreType != "minio" {
			t.Fatalf("wrong store type: %q", info.StoreType)
		}
		if info.NumBuckets != 0 {
			t.Fatalf("wrong num buckets (empty): %d", info.NumBuckets)
		}
		if info.NumObjects != 0 {
			t.Fatalf("wrong num objects (empty): %d", info.NumObjects)
		}
		if info.TotalSizeInBytes != 0 {
			t.Fatalf("wrong total size (empty): %d", info.TotalSizeInBytes)
		}
	}

	// Add some data
	testData1 := []byte("test data 1")
	testData2 := []byte("test data 2 - longer")
	if err := dataStore.Set(ctx,
		comby.DataStoreSetOptionWithBucketName("info-bucket1"),
		comby.DataStoreSetOptionWithObjectName("object1"),
		comby.DataStoreSetOptionWithContentType("text/plain"),
		comby.DataStoreSetOptionWithData(testData1),
	); err != nil {
		t.Fatal(err)
	}
	if err := dataStore.Set(ctx,
		comby.DataStoreSetOptionWithBucketName("info-bucket2"),
		comby.DataStoreSetOptionWithObjectName("object2"),
		comby.DataStoreSetOptionWithContentType("text/plain"),
		comby.DataStoreSetOptionWithData(testData2),
	); err != nil {
		t.Fatal(err)
	}

	// Get info on populated store
	if info, err := dataStore.Info(ctx); err != nil {
		t.Fatal(err)
	} else {
		if info.NumBuckets != 2 {
			t.Fatalf("wrong num buckets: %d", info.NumBuckets)
		}
		if info.NumObjects != 2 {
			t.Fatalf("wrong num objects: %d", info.NumObjects)
		}
		if info.TotalSizeInBytes != int64(len(testData1)+len(testData2)) {
			t.Fatalf("wrong total size: got %d, want %d", info.TotalSizeInBytes, len(testData1)+len(testData2))
		}
		if info.LastUpdateTime == 0 {
			t.Fatal("last update time should not be 0")
		}
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
