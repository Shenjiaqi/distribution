package fds

import (
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
	"time"
	"bytes"

	"github.com/XiaoMi/galaxy-fds-sdk-golang"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/XiaoMi/galaxy-fds-sdk-golang/Model"
)

const driverName = "fds"

const minChunkSize = (10 << 20)

const defaultChunkSize = 2 * minChunkSize

// listMax is the largest amount of objects you can request from fds in a list call
const listMax = 1000

// validRegions maps known fds region identifiers to region descriptors
var validRegions = map[string]struct{}{}

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	AccessKey     string
	SecretKey     string
	Bucket        string
	Region        string
	EnableHttps   bool
	EnableCDN     bool
	ChunkSize     int64
	RootDirectory string
}

func init() {
	for _, region := range []string{
		"",
		"staging",
		"awsbj0",
		"awsusor0",
		"awssgp0",
		"awsde0",
		"c3",
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
		regionName = ""
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

	enableHttps := true
	enableHttpsStr, ok := parameters["enablehttps"]
	if ok {
		if strings.EqualFold("false", fmt.Sprint(enableHttpsStr)) {
			enableHttps = false
		}
	}

	enableCDN := true
	enableCDNStr, ok := parameters["enablecdn"]
	if ok {
		if strings.EqualFold("false", fmt.Sprint(enableCDNStr)) {
			enableCDN = false
		}
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

	rootDirectory, ok := parameters["rootdirectory"].(string)
	if !ok {
		rootDirectory = ""
	}

	params := DriverParameters{
		AccessKey:     fmt.Sprint(accessKey),
		SecretKey:     fmt.Sprint(secretKey),
		Bucket:        fmt.Sprint(bucket),
		Region:        region,
		EnableHttps:   enableHttps,
		EnableCDN:     enableCDN,
		ChunkSize:     chunkSize,
		RootDirectory: fmt.Sprint(rootDirectory),
	}

	return New(params)
}

// New constructs a new Driver with the given AWS credentials, region, encryption flag, and
// bucketName
func New(params DriverParameters) (*Driver, error) {
	fdsobj := galaxy_fds_sdk_golang.NEWFDSClient(params.AccessKey,
		params.SecretKey, params.Region, params.EnableHttps, params.EnableCDN)
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
		return nil, parseError(path, err)
	}
	return ioutil.ReadAll(reader)
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	fdsPath := d.fdsPath(path)
	_, err := d.fds.Put_Object(d.driverParameters.Bucket, fdsPath, contents, "", nil)
	return parseError(path, err)
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	emptyReader := ioutil.NopCloser(bytes.NewReader(nil))
	fdsPath := d.fdsPath(path)

	reader, err := d.fds.Get_Object_Reader(d.driverParameters.Bucket,
		fdsPath, offset, -1)

	if err != nil {
		if e, ok := err.(*Model.FDSError); ok && e.Code() == 416 {
			return emptyReader, nil
		}
		return nil, parseError(path, err)
	}

	return *reader, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	fdsPath := d.fdsPath(path)
	if !append {
		resp, err := d.fds.Init_MultiPart_Upload(d.driverParameters.Bucket,
			fdsPath, "")
		if err != nil {
			return nil, parseError(path, err)
		}
		return d.newWriter(ctx, resp, &Model.UploadPartList{}), nil
	}

	list, err := d.fds.List_Multipart_Uploads(d.driverParameters.Bucket, fdsPath, "", 1)
	if err != nil {
		return nil, parseError(path, err)
	}

	for _, multi := range list.Uploads {
		if fdsPath != multi.ObjectName {
			continue
		}
		uploadPartList, err := d.fds.List_Parts(d.driverParameters.Bucket, fdsPath, multi.UploadId)
		if err != nil {
			return nil, parseError(path, err)
		}

		initUploadResult := Model.InitMultipartUploadResult{
			BucketName: d.driverParameters.Bucket,
			ObjectName: fdsPath,
			UploadId:   multi.UploadId,
		}
		return d.newWriter(ctx, &initUploadResult, uploadPartList), nil
	}

	return nil, storagedriver.PathNotFoundError{Path: path}
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
	if err == nil {
		return tAnsci, nil
	}
	rfcTime, err := time.Parse(time.RFC1123, timeStr)
	if err == nil {
		return rfcTime, nil
	}
	return time.Now(), err
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	fdsPath := d.fdsPath(path)
	resp, err := d.fds.List_Object(d.driverParameters.Bucket, fdsPath, "", 1)
	if err != nil {
		return nil, parseError(path, err)
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(resp.ObjectSummaries) == 1 {
		if (resp.ObjectSummaries[0]).ObjectName != fdsPath {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = (resp.ObjectSummaries[0]).Size
			meta, err := d.fds.Get_Object_Meta(d.driverParameters.Bucket, fdsPath)
			if err != nil {
				return nil, parseError(path, err)
			}
			timeStr, err := (*meta).GetLastModified()
			if err != nil {
				return nil, parseError(path, err)
			}
			fi.ModTime, err = parseTimeStr(timeStr)
			if err != nil {
				return nil, parseError(path, err)
			}
		}
	} else if len(resp.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	path := opath
	if path != "/" && len(path) > 0 && path[len(path)-1] != '/' {
		path = path + "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, therDS do not support listing multipart uploade is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp

	prefix := ""
	if d.fdsPath("") == "" {
		prefix = "/"
	}

	resp, err := d.fds.List_Object(d.driverParameters.Bucket,
		d.fdsPath(path), "/", galaxy_fds_sdk_golang.DEFAULT_LIST_MAX_KEYS)

	if err != nil {
		return nil, parseError(opath, err)
	}

	files := []string{}
	directories := []string{}

	for {
		for _, key := range resp.ObjectSummaries {
			files = append(files, strings.Replace(key.ObjectName, d.fdsPath(""), prefix, 1))
		}

		for _, commonPrefix := range resp.CommonPrefixes {
			directories = append(directories, strings.Replace(commonPrefix[0:len(commonPrefix)-1], d.fdsPath(""), prefix, 1))
		}

		if resp.Truncated {
			resp, err = d.fds.List_Next_Batch_Of_Objects(resp)
			if err != nil {
				return nil, parseError(opath, err)
			}
		} else {
			break
		}
	}

	if path != "/" {
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
	srcPath := d.fdsPath(sourcePath)
	dstPath := d.fdsPath(destPath)
	_, err := d.fds.Rename_Object(d.driverParameters.Bucket, srcPath, dstPath)
	return parseError(sourcePath, err)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, opath string) error {
	path := d.fdsPath(opath)
	l, err := d.fds.List_Object(d.driverParameters.Bucket, path, "", 1)
	if err != nil || (len(l.ObjectSummaries) == 0 && len(l.CommonPrefixes) == 0) {
		return storagedriver.PathNotFoundError{Path: opath}
	}

	return d.fds.Delete_Objects_With_Prefix(d.driverParameters.Bucket, path)
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	fdsPath := d.fdsPath(path)
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

	return d.fds.Generate_Presigned_URI(d.driverParameters.Bucket,
		fdsPath, methodString, expiresAt.UnixNano()/int64(time.Millisecond), map[string][]string{})
}

func (d *driver) fdsPath(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.driverParameters.RootDirectory, "/")+path, "/")
}

// fdsBucketKey returns the fds bucket key for the given storage driver path.
func (d *Driver) fdsBucketKey(path string) string {
	return ""
}

func parseError(path string, err error) error {
	if err != nil {
		if e, ok := err.(*Model.FDSError); ok && e.Code() == 404 {
			return storagedriver.PathNotFoundError{Path: path}
		}
	}
	return err
}

func (d *driver) getEncryptionMode() *string {
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
type uploadData struct {
	partId  int
	data    []byte
	err     error
	tryTime int
}

func newUploadData(partId int, data []byte) *uploadData {
	return &uploadData{
		partId:  partId,
		data:    data,
		err:     nil,
		tryTime: 0,
	}
}

type writer struct {
	driver                    *driver
	initMultipartUploadResult *Model.InitMultipartUploadResult
	partUploadResultList      *Model.UploadPartList
	size                      int64
	closed                    bool
	committed                 bool
	cancelled                 bool
	bufferList                []uploadData
	lastUploadId              int
	ctx                       context.Context
}

func (w *writer) appendByteArray(b []byte) {
	l := len(w.bufferList)
	if l == 0 || (l < maxBufferList && len(w.bufferList[l-1].data) >= minChunkSize) {
		w.lastUploadId += 1
		// use sum because i cannot find max(int, int) (int) in math package, either can i write one, it's too hard
		a := make([]byte, len(b), cap(b) + minChunkSize)
		copy(a, b)
		w.bufferList = append(w.bufferList, *newUploadData(w.lastUploadId, a))
	} else {
		w.bufferList[l-1].data = append(w.bufferList[l-1].data, b...)
	}
}

func (d *driver) newWriter(ctx context.Context, initMultipartUploadResult *Model.InitMultipartUploadResult,
	uploadPartList *Model.UploadPartList) storagedriver.FileWriter {
	var sumSize int64
	for _, p := range uploadPartList.UploadPartResultList {
		sumSize += p.PartSize
	}

	return &writer{
		driver: d,
		initMultipartUploadResult: initMultipartUploadResult,
		partUploadResultList:      uploadPartList,
		size:                      sumSize,
		bufferList:                make([]uploadData, 0),
		lastUploadId:              len(uploadPartList.UploadPartResultList),
		ctx:                       ctx,
	}
}

const maxBufferList = 10

func (w *writer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	l := len(w.bufferList)
	if l >= maxBufferList && len(w.bufferList[l-1].data) >= minChunkSize {
		err := w.flushPart()
		if err != nil {
			return 0, err
		}
	}

	w.appendByteArray(p)
	pl := len(p)
	w.size += int64(pl)

	return pl, nil
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

	w.cancelled = true
	return w.driver.fds.Abort_MultipartUpload(w.initMultipartUploadResult)
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
	return parseError(w.initMultipartUploadResult.ObjectName, err)
}

// flushPart flushes buffers to write a part to fds.
// Only called by Write (with both buffers full) and Close/Commit (always)

func (w *writer) uploadPart(p uploadData, c chan<- *uploadData) {
	_, err := w.driver.fds.Upload_Part(w.initMultipartUploadResult, p.partId, p.data)
	p.tryTime += 1
	if err != nil {
		p.err = err
		c <- &p
	} else {
		c <- nil
	}
}

func (w *writer) flushPart() error {
	l := len(w.bufferList)
	if l == 0 {
		w.bufferList = nil
		return nil
	}

	c := make(chan *uploadData, l)
	for _, d := range w.bufferList {
		go w.uploadPart(d, c)
	}

	var errs error
	w.bufferList = nil
	for i := 0; i < l; i++ {
		r := <-c
		if r != nil {
			errs = r.err
			w.bufferList = append(w.bufferList, *r)
		}
	}

	return errs
}
