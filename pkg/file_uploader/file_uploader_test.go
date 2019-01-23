package file_uploader

import (
	. "github.com/pingcap/check"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

var _ = Suite(&testFileUploader{})

type testFileUploader struct{}

func (t *testFileUploader) TestFileUploaderAppend(c *C) {
	dir, err := ioutil.TempDir("", "up_test_file_uploader_append")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	filenames := []string{"testfile1", "testfile2", "testdir/testfile1"}
	for _, filename := range filenames {
		go func() {
			sourceFilePath := filepath.Join(dir, filename)
			err := os.MkdirAll(filepath.Dir(sourceFilePath), 0777)
			c.Assert(err, IsNil)
			file, err := os.OpenFile(sourceFilePath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
			c.Assert(err, IsNil)
			defer file.Close()
			_, err = io.CopyN(file, rand, 789*M)
			c.Assert(err, IsNil)
		}()
	}
	targetDir, err := ioutil.TempDir("", "up_test_file_uploader_append_target")
	c.Assert(err, IsNil)
	defer os.RemoveAll(targetDir)
	fu := NewFileUploader(dir, 8, 100*M, NewMockFileUploaderDriver(targetDir))
	fu.WaitAndClose()
	for _, filename := range filenames {
		sourceHash := NewMd5Base64FileHash()
		sourceFile, err := os.OpenFile(filepath.Join(dir, filename), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
		c.Assert(err, IsNil)
		defer sourceFile.Close()
		_, err = io.Copy(sourceHash, sourceFile)
		c.Assert(err, IsNil)
		targetHash := NewMd5Base64FileHash()
		targetFile, err := os.OpenFile(filepath.Join(targetDir, filename), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
		c.Assert(err, IsNil)
		defer targetFile.Close()
		_, err = io.Copy(targetHash, targetFile)
		c.Assert(err, IsNil)
		c.Assert(sourceHash.String(), Equals, targetHash.String(), Commentf("hash check failure, file name: %s", filename))
	}
}
