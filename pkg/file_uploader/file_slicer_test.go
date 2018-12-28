package file_uploader

import (
	. "github.com/pingcap/check"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var _ = Suite(&testFileSlicerSuite{})

const (
	SliceStatusFile = ".fu_slice_status"
	K               = 1024
	M               = K * K
	SliceSize       = 100 * K // 100K
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testFileSlicerSuite struct {
}

type mockFileInfo struct {
	name string
	size int64
}

func (mf *mockFileInfo) Name() string {
	return mf.name
}

func (mf *mockFileInfo) Size() int64 {
	return mf.size
}

func (mf *mockFileInfo) Mode() os.FileMode {
	panic("won't implement")
}

func (mf *mockFileInfo) ModTime() time.Time {
	panic("won't implement")
}

func (mf *mockFileInfo) IsDir() bool {
	panic("won't implement")
}

func (mf *mockFileInfo) Sys() interface{} {
	panic("won't implement")
}

func (t *testFileSlicerSuite) TestWatcher(c *C) {
	// create dir
	dir, err := ioutil.TempDir("", "test_file_slicer")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	// test slice file
	sliceStatusPath := filepath.Join(dir, SliceStatusFile)
	fileSlicer, err := NewFileSlicer(sliceStatusPath, SliceSize)
	c.Assert(err, IsNil)
	testFileName := "test1"
	var testFileSize int64 = 678*K + 789
	sliceInfos, err := fileSlicer.DoSlice(&mockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	expectSliceInfos := []SliceInfo{
		{testFileName, 0, 0, 102400},
		{testFileName, 1, 102400, 102400},
		{testFileName, 2, 204800, 102400},
		{testFileName, 3, 307200, 102400},
		{testFileName, 4, 409600, 102400},
		{testFileName, 5, 512000, 102400},
		{testFileName, 6, 614400, 80661},
	}
	c.Assert(sliceInfos, DeepEquals, expectSliceInfos)
	c.Assert(sliceInfos[len(sliceInfos)-1].Offset+sliceInfos[len(sliceInfos)-1].Length, Equals, testFileSize)

	testFileName = "test2"
	testFileSize = 123*K + 456
	sliceInfos, err = fileSlicer.DoSlice(&mockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	expectSliceInfos = []SliceInfo{
		{testFileName, 0, 0, 102400},
		{testFileName, 1, 102400, 24008},
	}
	c.Assert(sliceInfos, DeepEquals, expectSliceInfos)
	c.Assert(sliceInfos[len(sliceInfos)-1].Offset+sliceInfos[len(sliceInfos)-1].Length, Equals, testFileSize)

	// test file changed
	testFileName = "test1"
	testFileSize = 234*K + 345
	sliceInfos, err = fileSlicer.DoSlice(&mockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	// when file become smaller, the checker will handle consistency problems.
	c.Assert(sliceInfos, IsNil)

	testFileName = "test2"
	testFileSize = 345*K + 321
	sliceInfos, err = fileSlicer.DoSlice(&mockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	expectSliceInfos = []SliceInfo{
		{testFileName, 1, 102400, 102400},
		{testFileName, 2, 204800, 102400},
		{testFileName, 3, 307200, 46401},
	}
	c.Assert(sliceInfos, DeepEquals, expectSliceInfos)
	c.Assert(sliceInfos[len(sliceInfos)-1].Offset+sliceInfos[len(sliceInfos)-1].Length, Equals, testFileSize)

	// test checkpoint
	fileSlicer, err = NewFileSlicer(sliceStatusPath, SliceSize)
	c.Assert(err, IsNil)
	testFileName = "test1"
	testFileSize = 456*K + 123
	sliceInfos, err = fileSlicer.DoSlice(&mockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	expectSliceInfos = []SliceInfo{
		{testFileName, 2, 204800, 102400},
		{testFileName, 3, 307200, 102400},
		{testFileName, 4, 409600, 57467},
	}
	c.Assert(sliceInfos, DeepEquals, expectSliceInfos)
	c.Assert(sliceInfos[len(sliceInfos)-1].Offset+sliceInfos[len(sliceInfos)-1].Length, Equals, testFileSize)

	testFileName = "test2"
	testFileSize = 123*K + 321
	sliceInfos, err = fileSlicer.DoSlice(&mockFileInfo{testFileName, testFileSize})
	c.Assert(err, IsNil)
	c.Assert(sliceInfos, IsNil)
}
