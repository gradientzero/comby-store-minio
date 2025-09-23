package store

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/gradientzero/comby/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type dataStoreMinio struct {
	options comby.DataStoreOptions

	// minio
	Endpoint     string
	minioClient  *minio.Client
	minioOptions *minio.Options

	// info
	dataStoreInfoModel *comby.DataStoreInfoModel
}

// Make sure it implements interfaces
var _ comby.DataStore = (*dataStoreMinio)(nil)

func NewDataStoreMinio(
	Endpoint string,
	Secure bool,
	AccessKeyId string,
	SecrectAccessKey string,
	opts ...comby.DataStoreOption,
) comby.DataStore {
	dsm := &dataStoreMinio{
		Endpoint: Endpoint,
		options:  comby.DataStoreOptions{},
		minioOptions: &minio.Options{
			Creds:  credentials.NewStaticV4(AccessKeyId, SecrectAccessKey, ""),
			Secure: Secure,
		},
		dataStoreInfoModel: &comby.DataStoreInfoModel{
			StoreType:      "minio",
			ConnectionInfo: fmt.Sprintf("minio://%s:***@%s - secure: %t", AccessKeyId, Endpoint, Secure),
		},
	}
	for _, opt := range opts {
		if _, err := opt(&dsm.options); err != nil {
			return nil
		}
	}
	return dsm
}

// fullfilling DataStore interface
func (dsm *dataStoreMinio) Init(ctx context.Context, opts ...comby.DataStoreOption) error {
	for _, opt := range opts {
		if _, err := opt(&dsm.options); err != nil {
			return err
		}
	}
	var err error
	dsm.minioClient, err = minio.New(dsm.Endpoint, dsm.minioOptions)
	return err
}

func (dsm *dataStoreMinio) Get(ctx context.Context, opts ...comby.DataStoreGetOption) (*comby.DataModel, error) {
	getOpts := comby.DataStoreGetOptions{}
	for _, opt := range opts {
		if _, err := opt(&getOpts); err != nil {
			return nil, err
		}
	}
	var err error
	opts2 := minio.GetObjectOptions{
		// ContentType: contentType,
	}
	minioObject, err := dsm.minioClient.GetObject(ctx, getOpts.BucketName, getOpts.ObjectName, opts2)
	if err != nil {
		return nil, err
	}
	if minioObject != nil {
		bytes, err := io.ReadAll(minioObject)
		if err != nil {
			return nil, err
		}
		return &comby.DataModel{
			BucketName: getOpts.BucketName,
			ObjectName: getOpts.ObjectName,
			Data:       bytes,
		}, nil
	}
	return nil, fmt.Errorf("object invalid")
}

func (dsm *dataStoreMinio) Set(ctx context.Context, opts ...comby.DataStoreSetOption) error {
	setOpts := comby.DataStoreSetOptions{
		Attributes: comby.NewAttributes(),
	}
	for _, opt := range opts {
		if _, err := opt(&setOpts); err != nil {
			return err
		}
	}
	var err error
	// ensure bucket exists
	bucketExists, err := dsm.minioClient.BucketExists(ctx, setOpts.BucketName)
	if err != nil {
		return err
	}
	if !bucketExists {
		isBucketPublic := false
		if _val := setOpts.Attributes.Get(comby.DATA_STORE_ATTRIBUTE_IS_PUBLIC); _val != nil {
			switch val := _val.(type) {
			case bool:
				isBucketPublic = val
			}
		}
		makeBucketOptions := minio.MakeBucketOptions{Region: "us-east-1", ObjectLocking: true}
		if err = dsm.createBucket(ctx, setOpts.BucketName, isBucketPublic, makeBucketOptions); err != nil {
			return err
		}
	}
	// convert byte slice to io.Reader
	reader := bytes.NewReader(setOpts.Data)
	objectSize := int64(len(setOpts.Data))
	opts2 := minio.PutObjectOptions{
		ContentType: setOpts.ContentType,
	}
	_, err = dsm.minioClient.PutObject(ctx, setOpts.BucketName, setOpts.ObjectName, reader, objectSize, opts2)
	if err != nil {
		return err
	}
	return nil
}

func (dsm *dataStoreMinio) Copy(ctx context.Context, opts ...comby.DataStoreCopyOption) error {
	copyOpts := comby.DataStoreCopyOptions{}
	for _, opt := range opts {
		if _, err := opt(&copyOpts); err != nil {
			return err
		}
	}
	var err error
	// ensure destination bucket exists
	bucketExists, err := dsm.minioClient.BucketExists(ctx, copyOpts.DstBucketName)
	if err != nil {
		return err
	}
	if !bucketExists {
		isBucketPublic := false
		if _val := copyOpts.Attributes.Get(comby.DATA_STORE_ATTRIBUTE_IS_PUBLIC); _val != nil {
			switch val := _val.(type) {
			case bool:
				isBucketPublic = val
			}
		}
		makeBucketOptions := minio.MakeBucketOptions{Region: "us-east-1", ObjectLocking: true}
		if err = dsm.createBucket(ctx, copyOpts.DstBucketName, isBucketPublic, makeBucketOptions); err != nil {
			return err
		}
	}
	// source options
	srcOpts := minio.CopySrcOptions{
		Bucket: copyOpts.SrcBucketName,
		Object: copyOpts.SrcObjectName,
	}
	// destination options
	dstOpts := minio.CopyDestOptions{
		Bucket: copyOpts.DstBucketName,
		Object: copyOpts.DstObjectName,
	}
	// copy server-side to new destination
	_, err = dsm.minioClient.CopyObject(ctx, dstOpts, srcOpts)
	if err != nil {
		return err
	}
	return nil
}

func (dsm *dataStoreMinio) List(ctx context.Context, opts ...comby.DataStoreListOption) ([]*comby.DataModel, int64, error) {
	listOpts := comby.DataStoreListOptions{}
	for _, opt := range opts {
		if _, err := opt(&listOpts); err != nil {
			return nil, 0, err
		}
	}
	var items []*comby.DataModel
	if dsm.minioClient != nil {
		// TODO: navie implementation, should be optimized
		buckets, err := dsm.minioClient.ListBuckets(ctx)
		if err != nil {
			return items, 0, err
		}
		for _, bucket := range buckets {

			objectCh := dsm.minioClient.ListObjects(ctx, bucket.Name, minio.ListObjectsOptions{
				Recursive: true,
			})
			for object := range objectCh {
				if object.Err != nil {
					// nothing)
				} else {
					items = append(items, &comby.DataModel{
						BucketName: bucket.Name,
						ObjectName: object.Key,
					})
				}
			}
		}
	}
	var total int64 = int64(len(items))
	return items, total, nil
}

func (dsm *dataStoreMinio) Delete(ctx context.Context, opts ...comby.DataStoreDeleteOption) error {
	deleteOpts := comby.DataStoreDeleteOptions{}
	for _, opt := range opts {
		if _, err := opt(&deleteOpts); err != nil {
			return err
		}
	}
	opts2 := minio.RemoveObjectOptions{}
	return dsm.minioClient.RemoveObject(ctx, deleteOpts.BucketName, deleteOpts.ObjectName, opts2)
}

func (dsm *dataStoreMinio) Total(ctx context.Context) int64 {
	total := int64(0)
	if dsm.minioClient != nil {
		// TODO: navie implementation, should be optimized
		buckets, err := dsm.minioClient.ListBuckets(ctx)
		if err != nil {
			return 0
		}
		for _, bucket := range buckets {

			objectCh := dsm.minioClient.ListObjects(ctx, bucket.Name, minio.ListObjectsOptions{
				Recursive: true,
			})
			for object := range objectCh {
				if object.Err != nil {
					// nothing)
				} else {
					total += 1
				}
			}
		}
	}
	return total
}

func (dsm *dataStoreMinio) Close(ctx context.Context) error {
	if dsm.minioClient != nil {
		// TODO: check if close is required at all here
	}
	return nil
}

func (dsm *dataStoreMinio) Options() comby.DataStoreOptions {
	return dsm.options
}

func (dsm *dataStoreMinio) String() string {
	return fmt.Sprintf("minio://%s", dsm.Endpoint)
}

func (dsm *dataStoreMinio) createBucket(ctx context.Context, bucketName string, public bool, makeBucketOptions minio.MakeBucketOptions) error {
	var err error
	err = dsm.minioClient.MakeBucket(ctx, bucketName, makeBucketOptions)
	if err != nil {
		return err
	}
	if public {
		policy := fmt.Sprintf(`{
				"Statement": [
				 {
				  "Action": [
				   "s3:GetObject"
				  ],
				  "Effect": "Allow",
				  "Principal": {
				   "AWS": [
					"*"
				   ]
				  },
				  "Resource": [
				   "arn:aws:s3:::%s/*"
				  ]
				 }
				],
				"Version": "2012-10-17"
			   }`, bucketName)
		err = dsm.minioClient.SetBucketPolicy(ctx, bucketName, policy)
		if err != nil {
			return err
		}
	}

	// enable versioning?
	/*
		err := dsm.minioClient.EnableVersioning(ctx, bucketName)
		if err != nil {
			return err
		}
	*/
	return nil
}

func (dsm *dataStoreMinio) Info(ctx context.Context) (*comby.DataStoreInfoModel, error) {
	// TODO: implement proper info fetching
	dsm.dataStoreInfoModel.LastUpdateTime = -1
	dsm.dataStoreInfoModel.NumBuckets = -1
	dsm.dataStoreInfoModel.NumObjects = -1
	dsm.dataStoreInfoModel.TotalSizeInBytes = -1
	return dsm.dataStoreInfoModel, nil
}

func (dsm *dataStoreMinio) Reset(ctx context.Context) error {
	if dsm.minioClient != nil {
		if buckets, err := dsm.minioClient.ListBuckets(ctx); err != nil {
			return err
		} else {
			for _, bucket := range buckets {
				objectCh := dsm.minioClient.ListObjects(ctx, bucket.Name, minio.ListObjectsOptions{
					Recursive: true,
				})
				for object := range objectCh {
					if object.Err != nil {
						continue
					} else {
						if err := dsm.minioClient.RemoveObject(ctx, bucket.Name, object.Key, minio.RemoveObjectOptions{}); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}
