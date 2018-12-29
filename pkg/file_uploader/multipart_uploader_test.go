package file_uploader

import (
	. "github.com/pingcap/check"
	"io/ioutil"
	"os"
)

var _ = Suite(&testCheckPointSuite{})

type testCheckPointSuite struct {
}

func (t *testFileSlicerSuite) TestCheckPoint(c *C) {
	// create dir
	dir, err := ioutil.TempDir("", "up_test_check_point")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	checkPoint, err := loadCheckPoint(dir)
	c.Assert(err, IsNil)
	err = checkPoint.logSliceUpload(&SliceInfo{
		"test1", 0,
		1024, 1024,
	}, "hash1", true)
	c.Assert(err, IsNil)
	err = checkPoint.logSliceUpload(&SliceInfo{
		"test1", 1,
		2048, 1024,
	}, "hash2", true)
	c.Assert(err, IsNil)
	err = checkPoint.logSliceUpload(&SliceInfo{
		"test2", 0,
		0, 2048,
	}, "hash3", true)
	c.Assert(err, IsNil)
	err = checkPoint.logSliceUpload(&SliceInfo{
		"test2", 1,
		2048, 2048,
	}, "hash4", true)
	c.Assert(err, IsNil)

	c.Assert(checkPoint.isSliceUploadSuccessful(&SliceInfo{
		"test1", 0,
		1024, 1024,
	}), IsTrue)
	c.Assert(checkPoint.isSliceUploadSuccessful(&SliceInfo{
		"test3", 0,
		1024, 1024,
	}), IsFalse)
	c.Assert(checkPoint.isSliceUploadSuccessful(&SliceInfo{
		"test2", 0,
		0, 1024,
	}), IsFalse)
	c.Assert(checkPoint.checkHash(&SliceInfo{
		"test2", 0,
		0, 1024,
	}, "hash3"), IsFalse)
	c.Assert(checkPoint.checkHash(&SliceInfo{
		"test2", 0,
		0, 2048,
	}, "hash3"), IsTrue)

	checkPointRunning.Set(0)
	checkPoint, err = loadCheckPoint(dir)
	c.Assert(err, IsNil)
	c.Assert(checkPoint.isSliceUploadSuccessful(&SliceInfo{
		"test1", 1,
		2048, 1024,
	}), IsTrue)
	c.Assert(checkPoint.isSliceUploadSuccessful(&SliceInfo{
		"test2", 0,
		0, 1024,
	}), IsFalse)
	c.Assert(checkPoint.checkHash(&SliceInfo{
		"test2", 0,
		0, 1024,
	}, "hash3"), IsFalse)
	c.Assert(checkPoint.checkHash(&SliceInfo{
		"test2", 0,
		0, 2048,
	}, "hash3"), IsTrue)
}
