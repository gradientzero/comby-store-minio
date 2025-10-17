# comby-store-minio

Simple implementation of the `DataStore` interface defined in [comby](https://github.com/gradientzero/comby) with MinIO. **comby** is a powerful application framework designed with Event Sourcing and Command Query Responsibility Segregation (CQRS) principles, written in Go.

[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## Prerequisites

- [Golang 1.22+](https://go.dev/dl/)
- [comby](https://github.com/gradientzero/comby)
- [MinIO](https://min.io/download)

```shell
# run minio locally for testings
docker run \
   -p 9000:9000 \
   -p 9001:9001 \
   --name minio \
   -v ./data:/data \
   -e "MINIO_ROOT_USER=ROOTNAME" \
   -e "MINIO_ROOT_PASSWORD=CHANGEME123" \
   quay.io/minio/minio server /data --console-address ":9001"
```

## Installation

*comby-store-minio* supports the latest version of comby (v2), requires Go version 1.22+ and is based on MinIO client v7.0.0.

```shell
go get github.com/gradientzero/comby-store-minio
```

## Quickstart

```go
import (
	"github.com/gradientzero/comby-store-minio"
	"github.com/gradientzero/comby/v2"
)

// create redis DataStore
dataStore := store.NewDataStoreMinio("127.0.0.1:9000", false, "ROOTNAME", "CHANGEME123")
if err = dataStore.Init(ctx,
    comby.DataStoreOptionWithAttribute("anyKey", "anyValue"),
); err != nil {
    panic(err)
}

// create Facade
fc, _ := comby.NewFacade(
  comby.FacadeWithDataStore(dataStore),
)
```

## Tests

```bash
go fmt ./...
go clean -testcache
go test -v ./... -covermode=count
go test -v ./... -race
go vet ./...

# go install honnef.co/go/tools/cmd/staticcheck@latest
staticcheck ./...
```

## Contributing
Please follow the guidelines in [CONTRIBUTING.md](./CONTRIBUTING.md).

## License
This project is licensed under the [MIT License](./LICENSE.md).

## Contact
https://www.gradient0.com
