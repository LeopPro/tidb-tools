package file_uploader

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testFileSlicerSuite{})

const (
	K         = 1024
	M         = K * K
	SliceSize = 100 * K // 100K
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testFileSlicerSuite struct {
}

type MockFileInfo struct {
	name string
	size int64
}

func (mf *MockFileInfo) Name() string {
	return mf.name
}

func (mf *MockFileInfo) Size() int64 {
	return mf.size
}

func (mf *MockFileInfo) Mode() os.FileMode {
	panic("won't implement")
}

func (mf *MockFileInfo) ModTime() time.Time {
	panic("won't implement")
}

func (mf *MockFileInfo) IsDir() bool {
	panic("won't implement")
}

func (mf *MockFileInfo) Sys() interface{} {
	panic("won't implement")
}

func (t *testFileSlicerSuite) TestWatcher(c *C) {
	// create dir
	dir, err := ioutil.TempDir("", "up_test_file_slicer")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	// test slice file
	fileSlicer, err := NewFileSlicer(dir, SliceSize)
	c.Assert(err, IsNil)
	testFileName := "test1"
	testFilePath := filepath.Join(dir, testFileName)
	var testFileSize int64 = 678*K + 789
	sliceInfos, err := fileSlicer.DoSlice(&MockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	expectSliceInfos := []Slice{
		{testFilePath, 0, 0, 102400},
		{testFilePath, 1, 102400, 102400},
		{testFilePath, 2, 204800, 102400},
		{testFilePath, 3, 307200, 102400},
		{testFilePath, 4, 409600, 102400},
		{testFilePath, 5, 512000, 102400},
		{testFilePath, 6, 614400, 80661},
	}
	c.Assert(sliceInfos, DeepEquals, expectSliceInfos)
	c.Assert(sliceInfos[len(sliceInfos)-1].Offset+sliceInfos[len(sliceInfos)-1].Length, Equals, testFileSize)

	testFileName = "test2"
	testFilePath = filepath.Join(dir, testFileName)
	testFileSize = 123*K + 456
	sliceInfos, err = fileSlicer.DoSlice(&MockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	expectSliceInfos = []Slice{
		{testFilePath, 0, 0, 102400},
		{testFilePath, 1, 102400, 24008},
	}
	c.Assert(sliceInfos, DeepEquals, expectSliceInfos)
	c.Assert(sliceInfos[len(sliceInfos)-1].Offset+sliceInfos[len(sliceInfos)-1].Length, Equals, testFileSize)

	// test file changed
	testFileName = "test1"
	testFilePath = filepath.Join(dir, testFileName)
	testFileSize = 234*K + 345
	sliceInfos, err = fileSlicer.DoSlice(&MockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	// when file become smaller, the checker will handle consistency problems.
	c.Assert(sliceInfos, IsNil)

	testFileName = "test2"
	testFilePath = filepath.Join(dir, testFileName)
	testFileSize = 345*K + 321
	sliceInfos, err = fileSlicer.DoSlice(&MockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	expectSliceInfos = []Slice{
		{testFilePath, 1, 102400, 102400},
		{testFilePath, 2, 204800, 102400},
		{testFilePath, 3, 307200, 46401},
	}
	c.Assert(sliceInfos, DeepEquals, expectSliceInfos)
	c.Assert(sliceInfos[len(sliceInfos)-1].Offset+sliceInfos[len(sliceInfos)-1].Length, Equals, testFileSize)

	// test checkpoint
	fileSlicer, err = NewFileSlicer(dir, SliceSize)
	c.Assert(err, IsNil)
	testFileName = "test1"
	testFilePath = filepath.Join(dir, testFileName)
	testFileSize = 456*K + 123
	sliceInfos, err = fileSlicer.DoSlice(&MockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	expectSliceInfos = []Slice{
		{testFilePath, 2, 204800, 102400},
		{testFilePath, 3, 307200, 102400},
		{testFilePath, 4, 409600, 57467},
	}
	c.Assert(sliceInfos, DeepEquals, expectSliceInfos)
	c.Assert(sliceInfos[len(sliceInfos)-1].Offset+sliceInfos[len(sliceInfos)-1].Length, Equals, testFileSize)

	testFileName = "test2"
	testFilePath = filepath.Join(dir, testFileName)
	testFileSize = 123*K + 321
	sliceInfos, err = fileSlicer.DoSlice(&MockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	c.Assert(sliceInfos, IsNil)
}
