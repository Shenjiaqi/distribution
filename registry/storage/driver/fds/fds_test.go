package fds

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"

	"gopkg.in/check.v1"
	"strings"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var fdsDriverConstructor func(rootDirectory string) (*Driver, error)
var skipFDS func() string

func init() {
	accessKey := os.Getenv("FDS_ACCESS_KEY")
	secretKey := os.Getenv("FDS_SECRET_KEY")
	bucket := os.Getenv("FDS_TEST_BUCKET")
	enableHttps := true
	if strings.EqualFold("false", os.Getenv("FDS_ENABLE_HTTPS")) {
		enableHttps = false
	}
	enableCDN := true
	if strings.EqualFold("false", os.Getenv("FDS_ENABLE_CDN")) {
		enableCDN = false
	}
	region := os.Getenv("FDS_REGION")
	root, err := ioutil.TempDir("", "driver-")
	if err != nil {
		panic(err)
	}

	fdsDriverConstructor = func(rootDirectory string) (*Driver, error) {
		parameters := DriverParameters{
			AccessKey:     accessKey,
			SecretKey:     secretKey,
			Bucket:        bucket,
			Region:        region,
			EnableHttps:   enableHttps,
			EnableCDN:     enableCDN,
			ChunkSize:     5 << 20,
			RootDirectory: rootDirectory,
		}

		return New(parameters)
	}

	// Skip fds storage driver tests if environment variable parameters are not provided
	skipFDS = func() string {
		if accessKey == "" || secretKey == "" ||
			bucket == "" {
			return "Must set AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_BUCKET, and S3_ENCRYPT to run S3 tests"
		}
		return ""
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return fdsDriverConstructor(root)
	}, skipFDS)
}

func TestEmptyRootList(t *testing.T) {
	if skipFDS() != "" {
		t.Skip(skipFDS())
	}

	validRoot, err := ioutil.TempDir("", "driver-")
	if err != nil {
		t.Fatalf("unexpected error creating temporary directory: %v", err)
	}
	defer os.Remove(validRoot)

	rootedDriver, err := fdsDriverConstructor(validRoot)
	if err != nil {
		t.Fatalf("unexpected error creating rooted driver: %v", err)
	}

	emptyRootDriver, err := fdsDriverConstructor("")
	if err != nil {
		t.Fatalf("unexpected error creating empty root driver: %v", err)
	}

	slashRootDriver, err := fdsDriverConstructor("/")
	if err != nil {
		t.Fatalf("unexpected error creating slash root driver: %v", err)
	}

	filename := "/test"
	contents := []byte("contents")
	ctx := context.Background()
	err = rootedDriver.PutContent(ctx, filename, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}
	defer rootedDriver.Delete(ctx, filename)

	keys, err := emptyRootDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}

	keys, err = slashRootDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}
}
