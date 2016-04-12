package fds

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Shenjiaqi/galaxy-fds-sdk-golang"

	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/client/transport"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/bitly/go-simplejson"
	"errors"
	"github.com/docker/distribution/vendor/github.com/aws/aws-sdk-go/service/s3"
	"github.com/Shenjiaqi/galaxy-fds-sdk-golang/Model"
)

const driverName = "fds"

const minChunkSize = 5 << 20

const defaultChunkSize = 2 * minChunkSize

// listMax is the largest amount of objects you can request from fds in a list call
const listMax = 1000

// validRegions maps known fds region identifiers to region descriptors
var validRegions = map[string]struct{}{}

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	AccessKey      string
	SecretKey      string
	Bucket         string
	Region         string
	EnableHttps    bool
	ChunkSize      int64
	RootDirectory  string
}

func init() {
	for _, region := range []string{
		"",
		"awsbj0",
		"awsusor0",
		"awssgp0",
		"awsde0",
	} {
		validRegions[region] = struct{}{}
	}

	factory.Register(driverName, &fdsDriverFactory{})
}

type fdsDriverFactory struct{}

func (factory *fdsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	fds              *galaxy_fds_sdk_golang.FDSClient
	driverParameters DriverParameters
}

type baseEmbed struct {
	base.Base
}

type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accesskey
// - secretkey
// - region
// - bucket
// - encrypt
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	accessKey := parameters["accesskey"]
	if accessKey == nil {
		accessKey = ""
	}

	secretKey := parameters["secretkey"]
	if secretKey == nil {
		secretKey = ""
	}

	regionName, ok := parameters["region"]
	if regionName == nil {
		return nil, fmt.Errorf("No region parameter provided")
	}
	region := fmt.Sprint(regionName)
	_, ok = validRegions[region]
	if !ok {
		return nil, fmt.Errorf("Invalid region provided: %v", region)
	}

	bucket := parameters["bucket"]
	if bucket == nil || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

 	enableHttps, ok := parameters["enableHttps"]
	if !ok || enableHttps == nil {
		enableHttps = true
	}

	chunkSize := int64(defaultChunkSize)
	chunkSizeParam := parameters["chunksize"]
	switch v := chunkSizeParam.(type) {
	case string:
		vv, err := strconv.ParseInt(v, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("chunksize parameter must be an integer, %v invalid", chunkSizeParam)
		}
		chunkSize = vv
	case int64:
		chunkSize = v
	case int, uint, int32, uint32, uint64:
		chunkSize = reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int()
	case nil:
		// do nothing
	default:
		return nil, fmt.Errorf("invalid value for chunksize: %#v", chunkSizeParam)
	}

	if chunkSize < minChunkSize {
		return nil, fmt.Errorf("The chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
	}

	rootDirectory := parameters["rootdirectory"]
	if rootDirectory == nil {
		rootDirectory = ""
	} else if (len(rootDirectory) > 0) {
		if !strings.HasSuffix(rootDirectory, "/") {
			rootDirectory += "/"
		}
	}

	params := DriverParameters{
		fmt.Sprint(accessKey),
		fmt.Sprint(secretKey),
		fmt.Sprint(bucket),
		region,
		enableHttps,
		chunkSize,
		fmt.Sprint(rootDirectory),
	}

	return New(params)
}

// New constructs a new Driver with the given AWS credentials, region, encryption flag, and
// bucketName
func New(params DriverParameters) (*Driver, error) {
	fdsobj := galaxy_fds_sdk_golang.NEWFDSClient(params.AccessKey,
		params.SecretKey)
	d := &driver{
		fds:              fdsobj,
		driverParameters: params,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	reader, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(reader)
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	_, err := d.fds.Put_Object(d.driverParameters.Bucket,
	d.driverParameters.RootDirectory + path, contents, "")
	return parseError(path, err)
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	data, err := d.fds.Get_Object(d.driverParameters.Bucket,
		d.driverParameters.RootDirectory + path, offset, -1)

	if err != nil {
		return nil, parseError(path, err)
	}
	return bytes.NewReader(data), nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	path = d.driverParameters + path
	if !append {
		resp, err := d.fds.Init_MultiPart_Upload(d.driverParameters.Bucket,
		path, "")
		if err != nil {
			return nil, err
		}
		return d.newWriter(path, resp, make([]*Model.UploadPartResult, 0))
	}

	// TODO fds donot suppor list multipart upload result yet

	return nil, errors.New("FDS do not support listing multipart uploads");
}

func parseTimeStr(timeStr string) (time.Time, error) {
	t822, err := time.Parse(time.RFC822, timeStr)
	if err == nil {
		return t822, nil
	}
	t850, err := time.Parse(time.RFC850, timeStr)
	if err == nil {
		return t850, nil
	}
	tAnsci, err := time.Parse(time.ANSIC, timeStr)
	if err != nil {
		return nil, err
	}
	return tAnsci, nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	path = d.driverParameters.RootDirectory + path
	resp, err := d.fds.List_Object(d.driverParameters.Bucket, path, "/", 1)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(resp.ObjectSummaries) == 1 {
		if ((*resp.ObjectSummaries[0]).ObjectName != path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = (*resp.ObjectSummaries[0]).Size
			meta, err := d.fds.Get_Object_Meta(d.driverParameters.Bucket, path)
			if err != nil {
				return nil, err
			}
			timeStr, err := (*meta).GetLastModified();
			if err != nil {
				return nil, err
			}
			fi.ModTime, err = parseTimeStr(timeStr)
			if err != nil {
				return nil, err
			}
		}
	} else if len(resp.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return nil, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	path := d.driverParameters.RootDirectory + opath
	if path != "/" && path[len(path)-1] != '/' {
		path = path + "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp

	resp, err := d.fds.List_Object(d.driverParameters.Bucket,
		path, "/", 1000)

	if err != nil {
		return nil, err
	}

	files := []string{}
	directories := []string{}

	for {
		for _, key := range resp.ObjectSummaries {
			files = append(files, *key.ObjectName)
		}

		for _, commonPrefix := range resp.CommonPrefixes {
			directories = append(directories, commonPrefix)
		}

		if *resp.Truncated {
			resp, err = d.fds.List_Next_Bacth_Of_Objects(resp)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			// Treat empty response as missing directory, since we don't actually
			// have directories in s3.
			return nil, storagedriver.PathNotFoundError{Path: opath}
		}
	}

	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	srcPath := d.driverParameters.RootDirectory + sourcePath
	dstPath := d.driverParameters.RootDirectory + destPath
	_, err := d.fds.Rename_Object(d.driverParameters.Bucket, srcPath, dstPath)
	return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	path = d.driverParameters.RootDirectory + path
	_, err := d.fds.Delete_Object(d.driverParameters.RootDirectory, path)
	return err
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	path = d.driverParameters.RootDirectory + path
	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresAt := time.Now().Add(20 * time.Minute)
	expires, ok := options["expiry"]
	if ok {
		e, ok := expires.(time.Time)
		if ok {
			expiresAt = e
		}
	}

	return d.fds.Generate_Presigned_URI(d.driverParameters.Bucket, path, methodString, expiresAt)
}

func (d *driver) fdsPath(path string) string {
	return nil
}

// fdsBucketKey returns the fds bucket key for the given storage driver path.
func (d *Driver) fdsBucketKey(path string) string {
	return nil
}

func parseError(path string, err error) error {
	return nil
}

func (d *driver) getEncryptionMode() *string {
	return nil
}

func (d *driver) getSSEKMSKeyID() *string {
	return nil
}

func (d *driver) getContentType() *string {
	return nil
}

func (d *driver) getACL() *string {
	return nil
}

func (d *driver) getStorageClass() *string {
	return nil
}

// writer attempts to upload parts to fds in a buffered fashion where the last
// part is at least as large as the chunksize, so the multipart upload could be
// cleanly resumed in the future. This is violated if Close is called after less
// than a full chunk is written.
type writer struct {
	driver      *driver
	initMultipartUploadResult *Model.InitMultipartUploadResult
	partId      int
	partUploadResultList *Model.UploadPartList
	size        int64
	closed      bool
	committed   bool
	cancelled   bool
	buffer      []byte
}

func (d *driver) newWriter(path, initMultipartUploadResult *Model.InitMultipartUploadResult, partUploadResult []*Model.UploadPartResult) storagedriver.FileWriter {
	var sumSize int64
	for _, p := range partUploadResult {
		sumSize += p.PartSize
	}
	return &writer {
		driver: d,
		path: path,
		initMultipartUploadResult: initMultipartUploadResult,
		partUploadResult: partUploadResult,
		size: sumSize,
	}
}

func (w *writer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	append(w.buffer, p)
	if len(w.buffer) >= minChunkSize {
		err := w.flushPart()
		if err != nil {
			return 0, nil
		}
	}

	l := len(p)
	w.size += l

	return l, nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	return w.flushPart()
}

func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.Commit()
	w.cancelled = true
	_, err := w.driver.fds.Delete_Object(w.driver.driverParameters.Bucket, w.initMultipartUploadResult.ObjectName)
	return err
}

func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	err := w.flushPart()
	if err != nil {
		return err
	}
	w.committed = true
	_, err = w.driver.fds.Complete_Multipart_Upload(w.initMultipartUploadResult, w.partUploadResultList)
	return err
}

// flushPart flushes buffers to write a part to fds.
// Only called by Write (with both buffers full) and Close/Commit (always)
func (w *writer) flushPart() error {
	if len(w.buffer) == 0 {
		w.buffer = nil
		return nil
	}

	w.partId += 1
	partResult, err := w.driver.fds.Upload_Part(w.initMultipartUploadResult.BucketName,
		w.initMultipartUploadResult.ObjectName, w.partId, w.buffer)
	if err != nil {
		return err
	}
	w.partUploadResultList.AddUploadPartResult(&partResult)

	return nil
}
